//// All timeouts are in milliseconds

import bath
import gleam/bit_array
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import gleam/set
import gleam/string
import gleam/uri
import mug

import valkyrie/internal/protocol

const protocol_version = 3

/// The configuration for a pool of connections to the database.
pub type Config {
  Config(host: String, port: Int, auth: Auth)
}

pub type Auth {
  NoAuth
  PasswordOnly(String)
  UsernameAndPassword(username: String, password: String)
}

pub opaque type Connection {
  Single(socket: mug.Socket)
  Pooled(bath.Pool(mug.Socket))
}

pub type PoolError {
  PooledResourceCreateError(String)
  NoResourcesAvailable
}

pub type Error {
  NotFound
  ProtocolError(String)
  ConnectionError
  Timeout
  TcpError(mug.Error)
  ServerError(String)
  PoolError(PoolError)
}

fn auth_to_options_list(auth: Auth) -> List(String) {
  case auth {
    NoAuth -> []
    PasswordOnly(password) -> ["AUTH", "default", password]
    UsernameAndPassword(username, password) -> ["AUTH", username, password]
  }
}

/// Create a default Redis configuration.
///
/// Returns a configuration with:
/// - host: "localhost"
/// - port: 6379
/// - auth: NoAuth
pub fn default_config() -> Config {
  Config(host: "localhost", port: 6379, auth: NoAuth)
}

/// Update the host in a Redis configuration.
pub fn host(config: Config, host: String) -> Config {
  Config(..config, host:)
}

/// Update the port in a Redis configuration.
pub fn port(config: Config, port: Int) -> Config {
  Config(..config, port:)
}

/// Update the authentication settings in a Redis configuration.
pub fn auth(config: Config, auth: Auth) -> Config {
  Config(..config, auth:)
}

pub type UrlParseError {
  InvalidUriFormat
  UnsupportedScheme
  MissingScheme
  MissingHost
}

/// Parse a Redis-compatible URI into a Config object.
///
/// Supports the following protocols:
/// - `redis://`
/// - `valkey://`
/// - `keydb://`
///
/// URI format: `protocol://[username:password@]host[:port][/database]`
///
/// The database path in the URI is ignored but won't cause an error.
/// If no port is specified, defaults to 6379.
///
/// ## Examples
///
/// ```gleam
/// // Basic usage
/// let assert Ok(config) = url_config("redis://localhost:6379")
/// // Config(host: "localhost", port: 6379, auth: NoAuth)
///
/// // With authentication
/// let assert Ok(config) = url_config("redis://user:pass@localhost:6379")
/// // Config(host: "localhost", port: 6379, auth: UsernameAndPassword("user", "pass"))
///
/// // Password-only authentication
/// let assert Ok(config) = url_config("redis://:mypassword@localhost:6379")
/// // Config(host: "localhost", port: 6379, auth: PasswordOnly("mypassword"))
///
/// // Different protocols
/// let assert Ok(config) = url_config("valkey://192.168.1.100:6380")
/// // Config(host: "192.168.1.100", port: 6380, auth: NoAuth)
///
/// // Error handling
/// let assert Error(UnsupportedScheme) = url_config("http://localhost")
/// let assert Error(MissingHost) = url_config("redis:")
/// ```
pub fn url_config(url: String) -> Result(Config, UrlParseError) {
  use parsed_uri <- result.try(
    uri.parse(url)
    |> result.replace_error(InvalidUriFormat),
  )

  // Validate scheme
  use _ <- result.try(case parsed_uri.scheme {
    option.Some("redis") | option.Some("valkey") | option.Some("keydb") ->
      Ok(parsed_uri.scheme)
    option.Some(_) -> Error(UnsupportedScheme)
    option.None -> Error(MissingScheme)
  })

  // Extract host
  use host <- result.try(case parsed_uri.host {
    option.Some("") | option.None -> Error(MissingHost)
    option.Some(h) -> Ok(h)
  })

  // Extract port (default to 6379 if not specified)
  let port = case parsed_uri.port {
    option.Some(p) -> p
    option.None -> 6379
  }

  // Extract authentication from userinfo
  let auth = case parsed_uri.userinfo {
    option.Some(userinfo) -> {
      case string.split(userinfo, ":") {
        ["", password] -> PasswordOnly(password)
        [username, password] -> UsernameAndPassword(username, password)
        [password] -> PasswordOnly(password)
        [] -> NoAuth
        _ -> NoAuth
        // fallback for malformed userinfo
      }
    }
    option.None -> NoAuth
  }

  Ok(Config(host: host, port: port, auth: auth))
}

// ------------------------------- //
// ----- Lifecycle functions ----- //
// ------------------------------- //

fn create_socket(config: Config, timeout: Int) -> Result(mug.Socket, Error) {
  use socket <- result.try(
    mug.connect(mug.ConnectionOptions(config.host, config.port, timeout))
    |> result.map_error(TcpError),
  )

  let conn = Single(socket)

  use _ <- result.try(execute(
    conn,
    [
      "HELLO",
      int.to_string(protocol_version),
      ..auth_to_options_list(config.auth)
    ],
    timeout,
  ))
  Ok(socket)
}

/// Create a single Redis connection.
///
/// This establishes a direct connection to Redis using the provided configuration.
/// For high-throughput applications, consider using `start_pool()` instead.
pub fn create_connection(
  config: Config,
  timeout: Int,
) -> Result(Connection, Error) {
  create_socket(config, timeout)
  |> result.map(Single)
}

fn create_pool_builder(
  config: Config,
  pool_size: Int,
  init_timeout: Int,
) -> bath.Builder(mug.Socket) {
  bath.new(fn() {
    create_socket(config, init_timeout)
    // TODO: proper to_string function for errors
    |> result.map_error(string.inspect)
  })
  |> bath.size(pool_size)
  |> bath.on_shutdown(fn(socket) {
    mug.shutdown(socket)
    |> result.unwrap(Nil)
  })
}

/// Start a connection pool for Redis connections.
///
/// **Note:** Consider using [`supervised_pool`](#supervised_pool) instead, which
/// provides automatic restart capabilities and better integration with OTP supervision
/// trees. This function should only be used when you need to manage the pool lifecycle
/// manually.
pub fn start_pool(
  config: Config,
  pool_size: Int,
  init_timeout: Int,
) -> Result(Connection, actor.StartError) {
  create_pool_builder(config, pool_size, init_timeout)
  |> bath.start(init_timeout)
  |> result.map(Pooled)
}

/// Create a supervised connection pool for Redis connections.
///
/// Returns a supervision specification for including the pool into your
/// supervision tree. The pool will be automatically restarted if it crashes,
/// making this the recommended approach for production applications.
///
/// The [`Connection`](#Connection) value will be sent to the provided subject
/// when the pool is started.
///
/// ## Example
///
/// ```gleam
/// TODO
/// ```
pub fn supervised_pool(
  config: Config,
  subj: process.Subject(Connection),
  pool_size: Int,
  init_timeout: Int,
) -> supervision.ChildSpecification(process.Subject(bath.Msg(mug.Socket))) {
  create_pool_builder(config, pool_size, init_timeout)
  |> bath.supervised_map(subj, Pooled, init_timeout)
}

/// Shut down a Redis connection or connection pool.
///
/// For single connections, closes the socket immediately.
/// For pooled connections, gracefully shuts down the pool with a 1 second timeout.
pub fn shutdown(conn: Connection) -> Result(Nil, Nil) {
  case conn {
    Single(socket) -> mug.shutdown(socket) |> result.replace_error(Nil)
    Pooled(pool) ->
      bath.shutdown(pool, False, 1000) |> result.replace_error(Nil)
  }
}

// ------------------------------- //
// ----- Execution functions ----- //
// ------------------------------- //

fn execute(conn: Connection, command: List(String), timeout: Int) {
  case conn {
    Single(socket) -> do_execute(socket, command, timeout)
    Pooled(pool) -> {
      bath.apply(pool, timeout, fn(socket) {
        case do_execute(socket, command, timeout) {
          Ok(value) -> bath.keep() |> bath.returning(Ok(value))
          Error(error) ->
            case error {
              ConnectionError | ProtocolError(_) | TcpError(_) -> {
                bath.discard()
                |> bath.returning(Error(error))
              }
              NotFound | ServerError(_) | Timeout -> {
                bath.keep()
                |> bath.returning(Error(error))
              }
              PoolError(_) -> panic as "unreachable - no pool here"
            }
        }
      })
      |> result.map_error(fn(bath_error) {
        case bath_error {
          bath.CheckOutResourceCreateError(error:) ->
            PooledResourceCreateError(error)
          bath.NoResourcesAvailable -> NoResourcesAvailable
        }
        |> PoolError
      })
      |> result.flatten
    }
  }
}

fn do_execute(socket: mug.Socket, command: List(String), timeout: Int) {
  use _ <- result.try(
    mug.send(socket, protocol.encode_command(command))
    |> result.map_error(TcpError),
  )

  use reply <- result.try(socket_receive(socket, <<>>, now(), timeout))
  case reply {
    [protocol.SimpleError(error)] | [protocol.BulkError(error)] ->
      Error(ServerError(error))
    value -> Ok(value)
  }
}

fn socket_receive(
  socket: mug.Socket,
  storage: BitArray,
  start_time: Int,
  timeout: Int,
) -> Result(List(protocol.Value), Error) {
  case protocol.decode_value(storage) {
    Ok(value) -> Ok(value)
    Error(_) -> {
      case now() - start_time >= timeout * 1_000_000 {
        True -> Error(Timeout)
        False ->
          case mug.receive(socket, timeout) {
            Error(tcp_error) -> Error(TcpError(tcp_error))
            Ok(packet) ->
              socket_receive(
                socket,
                bit_array.append(storage, packet),
                start_time,
                timeout,
              )
          }
      }
    }
  }
}

// TODO: make sure this is always converted to nanoseconds
@external(erlang, "erlang", "monotonic_time")
fn now() -> Int

// ---------------------------------- //
// ----- Return value functions ----- //
// ---------------------------------- //

fn expect_cursor(cursor_string: String) -> Result(Int, Error) {
  case int.parse(cursor_string) {
    Ok(value) -> Ok(value)
    Error(_) ->
      Error(ProtocolError("Expected integer cursor, got " <> cursor_string))
  }
}

fn expect_cursor_and_array(values: List(protocol.Value)) {
  case values {
    [
      protocol.Array([protocol.BulkString(new_cursor_str), protocol.Array(keys)]),
    ] -> {
      use new_cursor <- result.try(expect_cursor(new_cursor_str))
      use array <- result.try(
        list.try_map(keys, fn(item) {
          case item {
            protocol.BulkString(value) -> Ok(value)
            _ ->
              Error(
                ProtocolError(
                  protocol.error_string(expected: "string", got: [item]),
                ),
              )
          }
        }),
      )
      Ok(#(array, new_cursor))
    }
    _ ->
      Error(
        ProtocolError(protocol.error_string(
          expected: "cursor and array",
          got: values,
        )),
      )
  }
}

fn expect_integer(value: List(protocol.Value)) -> Result(Int, Error) {
  case value {
    [protocol.Integer(n)] -> Ok(n)
    _ ->
      Error(
        ProtocolError(protocol.error_string(expected: "integer", got: value)),
      )
  }
}

fn expect_integer_boolean(value) {
  expect_integer(value)
  |> result.map(fn(n) { n == 1 })
}

fn expect_float(value: List(protocol.Value)) -> Result(Float, Error) {
  case value {
    [protocol.BulkString(new)] ->
      case float.parse(new) {
        Ok(f) -> Ok(f)
        Error(_) ->
          // Try parsing as int first (Redis sometimes returns "4" instead of "4.0")
          case int.parse(new) {
            Ok(i) -> Ok(int.to_float(i))
            Error(_) -> Error(ProtocolError("Invalid float: " <> new))
          }
      }
    _ ->
      Error(ProtocolError(protocol.error_string(expected: "float", got: value)))
  }
}

fn expect_any_string(value: List(protocol.Value)) -> Result(String, Error) {
  case value {
    [protocol.SimpleString(str)] | [protocol.BulkString(str)] -> Ok(str)
    _ ->
      Error(
        ProtocolError(protocol.error_string(
          expected: "string or null",
          got: value,
        )),
      )
  }
}

fn expect_simple_string(value: List(protocol.Value)) -> Result(String, Error) {
  case value {
    [protocol.SimpleString(str)] -> Ok(str)
    _ ->
      Error(
        ProtocolError(protocol.error_string(
          expected: "simple string",
          got: value,
        )),
      )
  }
}

fn expect_nullable_bulk_string(value) {
  case value {
    [protocol.BulkString(str)] -> Ok(str)
    [protocol.Null] -> Error(NotFound)
    _ ->
      Error(
        ProtocolError(protocol.error_string(
          expected: "bulk string or null",
          got: value,
        )),
      )
  }
}

fn expect_any_nullable_string(
  value: List(protocol.Value),
) -> Result(String, Error) {
  case value {
    [protocol.SimpleString(str)] | [protocol.BulkString(str)] -> Ok(str)
    [protocol.Null] -> Error(NotFound)
    _ ->
      Error(
        ProtocolError(protocol.error_string(
          expected: "string or null",
          got: value,
        )),
      )
  }
}

fn expect_bulk_string_array(
  value: List(protocol.Value),
) -> Result(List(String), Error) {
  case value {
    [protocol.Array(array)] ->
      list.try_map(array, fn(item) {
        case item {
          protocol.BulkString(str) -> Ok(str)
          _ ->
            Error(
              ProtocolError(
                protocol.error_string(expected: "bulk string", got: [item]),
              ),
            )
        }
      })
    _ ->
      Error(ProtocolError(protocol.error_string(expected: "array", got: value)))
  }
}

fn expect_bulk_string_set(
  value: List(protocol.Value),
) -> Result(set.Set(String), Error) {
  case value {
    [protocol.Set(s)] ->
      set.fold(over: s, from: Ok(set.new()), with: fn(acc, item) {
        use values <- result.try(acc)
        case item {
          protocol.BulkString(str) -> Ok(set.insert(values, str))
          _ ->
            Error(
              ProtocolError(
                protocol.error_string(expected: "bulk string", got: [item]),
              ),
            )
        }
      })
    _ ->
      Error(ProtocolError(protocol.error_string(expected: "set", got: value)))
  }
}

fn expect_key_type(value) {
  case value {
    [protocol.SimpleString(str)] ->
      case str {
        "set" -> Ok(Set)
        "list" -> Ok(List)
        "zset" -> Ok(ZSet)
        "hash" -> Ok(Hash)
        "string" -> Ok(String)
        "stream" -> Ok(Stream)
        "none" -> Error(NotFound)
        _ -> Error(ProtocolError("Invalid key type: " <> str))
      }
    _ ->
      Error(
        ProtocolError(protocol.error_string(expected: "key type", got: value)),
      )
  }
}

// ---------------------------------- //
// ----- Escape hatch functions ----- //
// ---------------------------------- //

/// Execute a custom Redis command not already covered by `valkyrie`.
///
/// This function allows you to send any Redis command by providing
/// the command parts as a list of strings. Useful for new Redis commands
/// or extensions not yet supported by the library.
///
/// ## Example
///
/// ```gleam
/// // Execute a custom command like "CONFIG GET maxmemory"
/// let assert Ok(result) = custom(conn, ["CONFIG", "GET", "maxmemory"], 5000)
/// ```
pub fn custom(
  conn: Connection,
  parts: List(String),
  timeout: Int,
) -> Result(List(protocol.Value), Error) {
  execute(conn, parts, timeout)
}

// ---------------------------- //
// ----- Scalar functions ----- //
// ---------------------------- //

pub type KeyType {
  Set
  List
  ZSet
  Hash
  String
  Stream
}

fn key_type_to_string(key_type: KeyType) {
  case key_type {
    Set -> "set"
    List -> "list"
    ZSet -> "zset"
    Hash -> "hash"
    String -> "string"
    Stream -> "stream"
  }
}

/// Return all keys matching a pattern.
///
/// **Warning:** This command can be slow on large databases. Consider using
/// `scan()` or `scan_pattern()` for production use.
///
/// See the [Redis KEYS documentation](https://redis.io/commands/keys) for more details.
pub fn keys(
  conn: Connection,
  pattern: String,
  timeout: Int,
) -> Result(List(String), Error) {
  use value <- result.try(
    ["KEYS", pattern]
    |> execute(conn, _, timeout),
  )

  case value {
    [protocol.Array(array)] ->
      list.try_map(array, fn(item) {
        case item {
          protocol.BulkString(value) -> Ok(value)
          _ ->
            Error(
              ProtocolError(
                protocol.error_string(expected: "string", got: [item]),
              ),
            )
        }
      })
    _ ->
      Error(
        ProtocolError(protocol.error_string(
          expected: "string array",
          got: value,
        )),
      )
  }
}

/// Iterate incrementally over keys in the database.
///
/// Returns a tuple of `#(keys, next_cursor)`. Use the returned cursor
/// for subsequent calls. A cursor of 0 indicates the end of iteration.
///
/// See the [Redis SCAN documentation](https://redis.io/commands/scan) for more details.
pub fn scan(
  conn: Connection,
  cursor: Int,
  count: Int,
  timeout: Int,
) -> Result(#(List(String), Int), Error) {
  ["SCAN", int.to_string(cursor), "COUNT", int.to_string(count)]
  |> execute(conn, _, timeout)
  |> result.try(expect_cursor_and_array)
}

/// Iterate incrementally over keys matching a pattern.
///
/// Like `scan()` but only returns keys that match the given pattern.
/// Returns a tuple of `#(keys, next_cursor)`.
///
/// See the [Redis SCAN documentation](https://redis.io/commands/scan) for more details.
pub fn scan_pattern(
  conn: Connection,
  cursor: Int,
  pattern: String,
  count: Int,
  timeout: Int,
) -> Result(#(List(String), Int), Error) {
  [
    "SCAN",
    int.to_string(cursor),
    "MATCH",
    pattern,
    "COUNT",
    int.to_string(count),
  ]
  |> execute(conn, _, timeout)
  |> result.try(expect_cursor_and_array)
}

/// Iterate incrementally over keys of a specific type.
///
/// Like `scan()` but only returns keys of the specified type.
/// Returns a tuple of `#(keys, next_cursor)`.
///
/// See the [Redis SCAN documentation](https://redis.io/commands/scan) for more details.
pub fn scan_with_type(
  conn: Connection,
  cursor: Int,
  key_type: KeyType,
  count: Int,
  timeout: Int,
) -> Result(#(List(String), Int), Error) {
  [
    "SCAN",
    int.to_string(cursor),
    "COUNT",
    int.to_string(count),
    "TYPE",
    key_type_to_string(key_type),
  ]
  |> execute(conn, _, timeout)
  |> result.try(expect_cursor_and_array)
}

/// Iterate incrementally over keys matching a pattern and type.
///
/// Combines pattern matching and type filtering in a single scan operation.
/// Returns a tuple of `#(keys, next_cursor)`.
///
/// See the [Redis SCAN documentation](https://redis.io/commands/scan) for more details.
pub fn scan_pattern_with_type(
  conn: Connection,
  cursor: Int,
  key_type: KeyType,
  pattern: String,
  count: Int,
  timeout: Int,
) -> Result(#(List(String), Int), Error) {
  [
    "SCAN",
    int.to_string(cursor),
    "MATCH",
    pattern,
    "COUNT",
    int.to_string(count),
    "TYPE",
    key_type_to_string(key_type),
  ]
  |> execute(conn, _, timeout)
  |> result.try(expect_cursor_and_array)
}

/// Check if one or more keys exist.
///
/// Returns the number of keys that exist from the given list.
///
/// See the [Redis EXISTS documentation](https://redis.io/commands/exists) for more details.
pub fn exists(
  conn: Connection,
  keys: List(String),
  timeout: Int,
) -> Result(Int, Error) {
  ["EXISTS", ..keys]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Get the value of a key.
///
/// Returns `Error(NotFound)` if the key doesn't exist.
///
/// See the [Redis GET documentation](https://redis.io/commands/get) for more details.
pub fn get(conn: Connection, key: String, timeout: Int) -> Result(String, Error) {
  ["GET", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_any_nullable_string)
}

/// Get the values of multiple keys.
///
/// Returns `Error(NotFound)` if any of the keys don't exist.
/// For handling missing keys gracefully, use `custom()` with MGET.
///
/// See the [Redis MGET documentation](https://redis.io/commands/mget) for more details.
pub fn mget(
  conn: Connection,
  keys: List(String),
  timeout: Int,
) -> Result(List(String), Error) {
  use value <- result.try(
    ["MGET", ..keys]
    |> execute(conn, _, timeout),
  )

  case value {
    [protocol.Array(array)] ->
      list.try_map(array, fn(item) {
        case item {
          protocol.BulkString(str) -> Ok(str)
          protocol.Null -> Error(NotFound)
          _ ->
            Error(
              ProtocolError(
                protocol.error_string(expected: "string or null", got: [item]),
              ),
            )
        }
      })
    _ ->
      Error(ProtocolError(protocol.error_string(expected: "array", got: value)))
  }
}

/// Append a string to the value at the given key.
///
/// If the key doesn't exist, it creates a new key with the given value.
/// Returns the new length of the string after appending.
///
/// See the [Redis APPEND documentation](https://redis.io/commands/append) for more details.
pub fn append(
  conn: Connection,
  key: String,
  value: String,
  timeout: Int,
) -> Result(Int, Error) {
  ["APPEND", key, value]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

pub type SetExistenceCondition {
  // Equivalent to `NX`
  IfNotExists
  // Equivalent to `XX`
  IfExists
}

pub type SetExpiryOption {
  // Equivalent to `KEEPTTL`
  KeepTtl
  // Equivalent to `EX <value>`
  ExpirySeconds(Int)
  // Equivalent to `PX <value>`
  ExpiryMilliseconds(Int)
  // Equivalent to `EXAT <value>`
  ExpiresAtUnixSeconds(Int)
  // Equivalent to `PXAT <value>`
  ExpiresAtUnixMilliseconds(Int)
}

pub type SetOptions {
  SetOptions(
    existence_condition: option.Option(SetExistenceCondition),
    return_old: Bool,
    expiry_option: option.Option(SetExpiryOption),
  )
}

/// Default set options.
///
/// | Option | Value |
/// | ------ | ----- |
/// | `existence_condition` | `None` |
/// | `return_old` | `False` |
/// | `expiry_option` | `None` |
pub fn default_set_options() -> SetOptions {
  SetOptions(
    existence_condition: option.None,
    return_old: False,
    expiry_option: option.None,
  )
}

/// Set the value of a key with optional conditions and expiry.
///
/// ## Examples
///
/// ```gleam
/// // Basic set
/// let assert Ok(_) = valkyrie.set(conn, "key", "value", option.None, 5000)
///
/// // Set with expiry
/// let options = SetOptions(..default_set_options(), expiry_option: option.Some(ExpirySeconds(300)))
/// let assert Ok(_) = valkyrie.set(conn, "key", "value", option.Some(options), 5000)
///
/// // Set only if key doesn't exist
/// let options = SetOptions(..default_set_options(), existence_condition: option.Some(IfNotExists))
/// let assert Ok(_) = valkyrie.set(conn, "key", "value", option.Some(options), 5000)
/// ```
///
/// See the [Redis SET documentation](https://redis.io/commands/set) for more details.
pub fn set(
  conn: Connection,
  key: String,
  value: String,
  options: option.Option(SetOptions),
  timeout: Int,
) -> Result(String, Error) {
  let additions =
    options
    |> option.map(fn(options) {
      let additions = case options.expiry_option {
        option.Some(KeepTtl) -> ["KEEPTTL"]
        option.Some(ExpirySeconds(value)) -> ["EX", int.to_string(value)]
        option.Some(ExpiryMilliseconds(value)) -> ["PX", int.to_string(value)]
        option.Some(ExpiresAtUnixSeconds(value)) -> [
          "EXAT",
          int.to_string(value),
        ]
        option.Some(ExpiresAtUnixMilliseconds(value)) -> [
          "PXAT",
          int.to_string(value),
        ]
        option.None -> []
      }
      let additions = case options.return_old {
        True -> ["GET", ..additions]
        False -> additions
      }
      let additions = case options.existence_condition {
        option.Some(IfNotExists) -> ["NX", ..additions]
        option.Some(IfExists) -> ["XX", ..additions]
        option.None -> additions
      }
      additions
    })
    |> option.unwrap([])

  let command = ["SET", key, value, ..additions]

  execute(conn, command, timeout)
  |> result.try(expect_any_nullable_string)
}

/// Set multiple key-value pairs atomically.
///
/// All keys are set in a single atomic operation.
///
/// See the [Redis MSET documentation](https://redis.io/commands/mset) for more details.
pub fn mset(
  conn: Connection,
  kv_list: List(#(String, String)),
  timeout: Int,
) -> Result(String, Error) {
  let kv_list =
    kv_list
    |> list.map(fn(kv) { [kv.0, kv.1] })
    |> list.flatten

  ["MSET", ..kv_list]
  |> execute(conn, _, timeout)
  |> result.try(expect_any_string)
}

/// Delete one or more keys.
///
/// Returns the number of keys that were actually deleted.
///
/// See the [Redis DEL documentation](https://redis.io/commands/del) for more details.
pub fn del(
  conn: Connection,
  keys: List(String),
  timeout: Int,
) -> Result(Int, Error) {
  use value <- result.try(
    ["DEL", ..keys]
    |> execute(conn, _, timeout),
  )

  case value {
    [protocol.Integer(n)] -> Ok(n)
    _ ->
      Error(
        ProtocolError(protocol.error_string(expected: "integer", got: value)),
      )
  }
}

/// Increment the integer value of a key by 1.
///
/// If the key doesn't exist, it's set to 0 before incrementing.
/// Returns the new value after incrementing.
///
/// See the [Redis INCR documentation](https://redis.io/commands/incr) for more details.
pub fn incr(conn: Connection, key: String, timeout: Int) -> Result(Int, Error) {
  ["INCR", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Increment the integer value of a key by the given amount.
///
/// If the key doesn't exist, it's set to 0 before incrementing.
/// Returns the new value after incrementing.
///
/// See the [Redis INCRBY documentation](https://redis.io/commands/incrby) for more details.
pub fn incrby(
  conn: Connection,
  key: String,
  value: Int,
  timeout: Int,
) -> Result(Int, Error) {
  ["INCRBY", key, int.to_string(value)]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Increment the floating point value of a key by the given amount.
///
/// If the key doesn't exist, it's set to 0 before incrementing.
/// Returns the new value after incrementing.
///
/// See the [Redis INCRBYFLOAT documentation](https://redis.io/commands/incrbyfloat) for more details.
pub fn incrbyfloat(
  conn: Connection,
  key: String,
  value: Float,
  timeout: Int,
) -> Result(Float, Error) {
  ["INCRBYFLOAT", key, float.to_string(value)]
  |> execute(conn, _, timeout)
  |> result.try(expect_float)
}

/// Decrement the integer value of a key by 1.
///
/// If the key doesn't exist, it's set to 0 before decrementing.
/// Returns the new value after decrementing.
///
/// See the [Redis DECR documentation](https://redis.io/commands/decr) for more details.
pub fn decr(conn: Connection, key: String, timeout: Int) -> Result(Int, Error) {
  ["DECR", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Decrement the integer value of a key by the given amount.
///
/// If the key doesn't exist, it's set to 0 before decrementing.
/// Returns the new value after decrementing.
///
/// See the [Redis DECRBY documentation](https://redis.io/commands/decrby) for more details.
pub fn decr_by(
  conn: Connection,
  key: String,
  value: Int,
  timeout: Int,
) -> Result(Int, Error) {
  ["DECRBY", key, int.to_string(value)]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Return a random key from the database.
///
/// Returns `Error(NotFound)` if the database is empty.
///
/// See the [Redis RANDOMKEY documentation](https://redis.io/commands/randomkey) for more details.
pub fn random_key(conn: Connection, timeout: Int) -> Result(String, Error) {
  ["RANDOMKEY"]
  |> execute(conn, _, timeout)
  |> result.try(expect_nullable_bulk_string)
}

/// Get the type of a key.
///
/// Returns the data type stored at the key.
///
/// See the [Redis TYPE documentation](https://redis.io/commands/type) for more details.
pub fn key_type(
  conn: Connection,
  key: String,
  timeout: Int,
) -> Result(KeyType, Error) {
  ["TYPE", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_key_type)
}

/// Rename a key.
///
/// If the new key already exists, it will be overwritten.
/// Returns an error if the source key doesn't exist.
///
/// See the [Redis RENAME documentation](https://redis.io/commands/rename) for more details.
pub fn rename(
  conn: Connection,
  key: String,
  new_key: String,
  timeout: Int,
) -> Result(String, Error) {
  ["RENAME", key, new_key]
  |> execute(conn, _, timeout)
  |> result.try(expect_simple_string)
}

/// Rename a key only if the new key doesn't exist.
///
/// Returns 1 if the key was renamed, 0 if the new key already exists.
///
/// See the [Redis RENAMENX documentation](https://redis.io/commands/renamenx) for more details.
pub fn renamenx(
  conn: Connection,
  key: String,
  new_key: String,
  timeout: Int,
) -> Result(Int, Error) {
  ["RENAMENX", key, new_key]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Remove the expiration from a key.
///
/// Returns 1 if the timeout was removed, 0 if the key doesn't exist or has no timeout.
///
/// See the [Redis PERSIST documentation](https://redis.io/commands/persist) for more details.
pub fn persist(
  conn: Connection,
  key: String,
  timeout: Int,
) -> Result(Int, Error) {
  ["PERSIST", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Ping the Redis server.
///
/// Returns "PONG" if the server is responding.
/// For use with a custom message, use `ping_message()`.
///
/// See the [Redis PING documentation](https://redis.io/commands/ping) for more details.
pub fn ping(conn: Connection, timeout: Int) -> Result(String, Error) {
  ["PING"]
  |> execute(conn, _, timeout)
  |> result.try(fn(value) {
    case value {
      [protocol.SimpleString("PONG")] -> Ok("PONG")
      _ ->
        Error(
          ProtocolError(protocol.error_string(expected: "PONG", got: value)),
        )
    }
  })
}

/// Ping the Redis server with a custom message.
///
/// Returns the same message if the server is responding.
///
/// See the [Redis PING documentation](https://redis.io/commands/ping) for more details.
pub fn ping_message(
  conn: Connection,
  message: String,
  timeout: Int,
) -> Result(String, Error) {
  ["PING", message]
  |> execute(conn, _, timeout)
  |> result.try(fn(value) {
    case value {
      [protocol.BulkString(msg)] if msg == message -> Ok(msg)
      _ ->
        Error(
          ProtocolError(protocol.error_string(expected: message, got: value)),
        )
    }
  })
}

pub type ExpireCondition {
  // Equivalent to `NX`
  IfNoExpiry
  // Equivalent to `XX`
  IfHasExpiry
  // Equivalent to `GT`
  IfGreaterThan
  // Equivalent to `LT`
  IfLessThan
}

/// Set a timeout on a key.
///
/// The timeout is specified in seconds. Returns 1 if the timeout was set,
/// 0 if the key doesn't exist or the condition wasn't met.
///
/// See the [Redis EXPIRE documentation](https://redis.io/commands/expire) for more details.
pub fn expire(
  conn: Connection,
  key: String,
  ttl: Int,
  condition: option.Option(ExpireCondition),
  timeout: Int,
) -> Result(Int, Error) {
  let expiry_condition = case condition {
    option.Some(IfNoExpiry) -> ["NX"]
    option.Some(IfHasExpiry) -> ["XX"]
    option.Some(IfGreaterThan) -> ["GT"]
    option.Some(IfLessThan) -> ["LT"]
    option.None -> []
  }

  ["EXPIRE", key, int.to_string(ttl), ..expiry_condition]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

// -------------------------- //
// ----- List functions ----- //
// -------------------------- //

/// Push one or more values to the left (head) of a list.
///
/// Creates the list if it doesn't exist. Returns the new length of the list.
///
/// See the [Redis LPUSH documentation](https://redis.io/commands/lpush) for more details.
pub fn lpush(
  conn: Connection,
  key: String,
  values: List(String),
  timeout: Int,
) -> Result(Int, Error) {
  ["LPUSH", key, ..values]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Push one or more values to the right (tail) of a list.
///
/// Creates the list if it doesn't exist. Returns the new length of the list.
///
/// See the [Redis RPUSH documentation](https://redis.io/commands/rpush) for more details.
pub fn rpush(
  conn: Connection,
  key: String,
  values: List(String),
  timeout: Int,
) -> Result(Int, Error) {
  ["RPUSH", key, ..values]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Push values to the left (head) of a list, only if the list already exists.
///
/// Returns 0 if the key doesn't exist, otherwise returns the new length of the list.
///
/// See the [Redis LPUSHX documentation](https://redis.io/commands/lpushx) for more details.
pub fn lpushx(
  conn: Connection,
  key: String,
  values: List(String),
  timeout: Int,
) -> Result(Int, Error) {
  ["LPUSHX", key, ..values]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Push values to the right (tail) of a list, only if the list already exists.
///
/// Returns 0 if the key doesn't exist, otherwise returns the new length of the list.
///
/// See the [Redis RPUSHX documentation](https://redis.io/commands/rpushx) for more details.
pub fn rpushx(
  conn: Connection,
  key: String,
  values: List(String),
  timeout: Int,
) -> Result(Int, Error) {
  ["RPUSHX", key, ..values]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Get the length of a list.
///
/// Returns 0 if the key doesn't exist or is not a list.
///
/// See the [Redis LLEN documentation](https://redis.io/commands/llen) for more details.
pub fn llen(conn: Connection, key: String, timeout: Int) -> Result(Int, Error) {
  ["LLEN", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Get a range of elements from a list.
///
/// Both start and end are zero-based indexes. Negative numbers can be used
/// to designate elements starting from the tail of the list.
///
/// See the [Redis LRANGE documentation](https://redis.io/commands/lrange) for more details.
pub fn lrange(
  conn: Connection,
  key: String,
  start: Int,
  end: Int,
  timeout: Int,
) -> Result(List(String), Error) {
  ["LRANGE", key, int.to_string(start), int.to_string(end)]
  |> execute(conn, _, timeout)
  |> result.try(expect_bulk_string_array)
}

/// Remove and return an element from the left (head) of a list.
///
/// Returns `Error(NotFound)` if the key doesn't exist or the list is empty.
/// This version removes exactly one element.
///
/// See the [Redis LPOP documentation](https://redis.io/commands/lpop) for more details.
pub fn lpop(
  conn: Connection,
  key: String,
  count: Int,
  timeout: Int,
) -> Result(String, Error) {
  ["LPOP", key, int.to_string(count)]
  |> execute(conn, _, timeout)
  |> result.try(fn(value) {
    case value {
      // When count is provided, Redis returns an array
      [protocol.Array([protocol.BulkString(str)])] -> Ok(str)
      [protocol.Array([])] -> Error(NotFound)
      [protocol.Null] -> Error(NotFound)
      _ ->
        Error(
          ProtocolError(protocol.error_string(
            expected: "string or array",
            got: value,
          )),
        )
    }
  })
}

/// Remove and return an element from the right (tail) of a list.
///
/// Returns `Error(NotFound)` if the key doesn't exist or the list is empty.
/// This version removes exactly one element.
///
/// See the [Redis RPOP documentation](https://redis.io/commands/rpop) for more details.
pub fn rpop(
  conn: Connection,
  key: String,
  count: Int,
  timeout: Int,
) -> Result(String, Error) {
  ["RPOP", key, int.to_string(count)]
  |> execute(conn, _, timeout)
  |> result.try(fn(value) {
    case value {
      // When count is provided, Redis returns an array
      [protocol.Array([protocol.BulkString(str)])] -> Ok(str)
      [protocol.Array([])] -> Error(NotFound)
      [protocol.Null] -> Error(NotFound)
      _ ->
        Error(
          ProtocolError(protocol.error_string(
            expected: "string or array",
            got: value,
          )),
        )
    }
  })
}

/// Get an element from a list by its index.
///
/// Index is zero-based. Negative indices count from the end (-1 is the last element).
/// Returns `Error(NotFound)` if the index is out of range.
///
/// See the [Redis LINDEX documentation](https://redis.io/commands/lindex) for more details.
pub fn lindex(
  conn: Connection,
  key: String,
  index: Int,
  timeout: Int,
) -> Result(String, Error) {
  ["LINDEX", key, int.to_string(index)]
  |> execute(conn, _, timeout)
  |> result.try(expect_nullable_bulk_string)
}

/// Remove elements equal to value from a list.
///
/// - count > 0: Remove elements equal to value moving from head to tail
/// - count < 0: Remove elements equal to value moving from tail to head
/// - count = 0: Remove all elements equal to value
///
/// Returns the number of removed elements.
///
/// See the [Redis LREM documentation](https://redis.io/commands/lrem) for more details.
pub fn lrem(
  conn: Connection,
  key: String,
  count: Int,
  value: String,
  timeout: Int,
) -> Result(Int, Error) {
  ["LREM", key, int.to_string(count), value]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Set the value of an element in a list by its index.
///
/// Index is zero-based. Returns an error if the index is out of range.
///
/// See the [Redis LSET documentation](https://redis.io/commands/lset) for more details.
pub fn lset(
  conn: Connection,
  key: String,
  index: Int,
  value: String,
  timeout: Int,
) -> Result(String, Error) {
  ["LSET", key, int.to_string(index), value]
  |> execute(conn, _, timeout)
  |> result.try(expect_simple_string)
}

pub type InsertPosition {
  Before
  After
}

/// Insert an element before or after another element in a list.
///
/// Returns the new length of the list, or -1 if the pivot element wasn't found.
/// Returns 0 if the key doesn't exist.
///
/// See the [Redis LINSERT documentation](https://redis.io/commands/linsert) for more details.
pub fn linsert(
  conn: Connection,
  key: String,
  position: InsertPosition,
  pivot: String,
  value: String,
  timeout: Int,
) -> Result(Int, Error) {
  let position = case position {
    Before -> "BEFORE"
    After -> "AFTER"
  }

  ["LINSERT", key, position, pivot, value]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

// ------------------------- //
// ----- Set functions ----- //
// ------------------------- //

pub fn sadd(
  conn: Connection,
  key: String,
  values: List(String),
  timeout: Int,
) -> Result(Int, Error) {
  ["SADD", key, ..values]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

pub fn scard(conn: Connection, key: String, timeout: Int) -> Result(Int, Error) {
  ["SCARD", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

pub fn sismember(
  conn: Connection,
  key: String,
  value: String,
  timeout: Int,
) -> Result(Bool, Error) {
  ["SISMEMBER", key, value]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer_boolean)
}

pub fn smembers(
  conn: Connection,
  key: String,
  timeout: Int,
) -> Result(set.Set(String), Error) {
  ["SMEMBERS", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_bulk_string_set)
}

pub fn sscan(
  conn: Connection,
  key: String,
  cursor: Int,
  count: Int,
  timeout: Int,
) -> Result(#(List(String), Int), Error) {
  ["SSCAN", key, int.to_string(cursor), "COUNT", int.to_string(count)]
  |> execute(conn, _, timeout)
  |> result.try(expect_cursor_and_array)
}

pub fn sscan_pattern(
  conn: Connection,
  key: String,
  cursor: Int,
  pattern: String,
  count: Int,
  timeout: Int,
) -> Result(#(List(String), Int), Error) {
  [
    "SSCAN",
    key,
    int.to_string(cursor),
    "MATCH",
    pattern,
    "COUNT",
    int.to_string(count),
  ]
  |> execute(conn, _, timeout)
  |> result.try(expect_cursor_and_array)
}

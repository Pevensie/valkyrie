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

pub fn default_config() -> Config {
  Config(host: "localhost", port: 6379, auth: NoAuth)
}

pub fn host(config: Config, host: String) -> Config {
  Config(..config, host:)
}

pub fn port(config: Config, port: Int) -> Config {
  Config(..config, port:)
}

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

pub fn create_connection(
  config: Config,
  timeout: Int,
) -> Result(Connection, Error) {
  create_socket(config, timeout)
  |> result.map(Single)
}

pub fn create_pool_builder(
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

pub fn start_pool(
  config: Config,
  pool_size: Int,
  init_timeout: Int,
) -> Result(Connection, actor.StartError) {
  create_pool_builder(config, pool_size, init_timeout)
  |> bath.start(init_timeout)
  |> result.map(Pooled)
}

pub fn supervised_pool(
  config: Config,
  pool_size: Int,
  init_timeout: Int,
) -> #(
  supervision.ChildSpecification(process.Subject(bath.Msg(mug.Socket))),
  process.Selector(Connection),
) {
  let subj = process.new_subject()
  let spec =
    create_pool_builder(config, pool_size, init_timeout)
    |> bath.supervised(subj, init_timeout)

  let selector =
    process.new_selector()
    |> process.select_map(subj, Pooled)

  #(spec, selector)
}

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

/// Execute a command not already covered by `valkyrie`.
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

/// see [here](https://redis.io/commands/keys)!
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

/// see [here](https://redis.io/commands/scan)!
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

/// see [here](https://redis.io/commands/scan)!
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

/// see [here](https://redis.io/commands/scan)!
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

/// see [here](https://redis.io/commands/scan)!
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

/// see [here](https://redis.io/commands/exists)!
pub fn exists(
  conn: Connection,
  keys: List(String),
  timeout: Int,
) -> Result(Int, Error) {
  ["EXISTS", ..keys]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// see [here](https://redis.io/commands/get)!
pub fn get(conn: Connection, key: String, timeout: Int) -> Result(String, Error) {
  ["GET", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_any_nullable_string)
}

/// see [here](https://redis.io/commands/mget)!
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

/// see [here](https://redis.io/commands/append)!
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

/// see [here](https://redis.io/commands/set)!
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

/// see [here](https://redis.io/commands/mset)!
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

/// see [here](https://redis.io/commands/del)!
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

/// see [here](https://redis.io/commands/incr)!
pub fn incr(conn: Connection, key: String, timeout: Int) -> Result(Int, Error) {
  ["INCR", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// see [here](https://redis.io/commands/incrby)!
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

/// see [here](https://redis.io/commands/incrbyfloat)!
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

/// see [here](https://redis.io/commands/decr)!
pub fn decr(conn: Connection, key: String, timeout: Int) -> Result(Int, Error) {
  ["DECR", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// see [here](https://redis.io/commands/decrby)!
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

/// see [here](https://redis.io/commands/randomkey)!
pub fn random_key(conn: Connection, timeout: Int) -> Result(String, Error) {
  ["RANDOMKEY"]
  |> execute(conn, _, timeout)
  |> result.try(expect_nullable_bulk_string)
}

/// see [here](https://redis.io/commands/type)!
pub fn key_type(
  conn: Connection,
  key: String,
  timeout: Int,
) -> Result(KeyType, Error) {
  ["TYPE", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_key_type)
}

/// see [here](https://redis.io/commands/rename)!
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

/// see [here](https://redis.io/commands/renamenx)!
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

/// see [here](https://redis.io/commands/persist)!
pub fn persist(
  conn: Connection,
  key: String,
  timeout: Int,
) -> Result(Int, Error) {
  ["PERSIST", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// see [here](https://redis.io/commands/ping)!
/// for use with a custom message, use `ping_message/3`.
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

/// see [here](https://redis.io/commands/ping)!
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

/// see [here](https://redis.io/commands/expire)!
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

/// see [here](https://redis.io/commands/lpush)!
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

/// see [here](https://redis.io/commands/rpush)!
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

/// see [here](https://redis.io/commands/lpushx)!
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

/// see [here](https://redis.io/commands/rpushx)!
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

pub fn llen(conn: Connection, key: String, timeout: Int) -> Result(Int, Error) {
  ["LLEN", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// see [here](https://redis.io/commands/lrange)!
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

/// see [here](https://redis.io/commands/lpop)!
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

/// see [here](https://redis.io/commands/rpop)!
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

/// see [here](https://redis.io/commands/lindex)!
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

/// see [here](https://redis.io/commands/lrem)!
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

/// see [here](https://redis.io/commands/lset)!
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

/// see [here](https://redis.io/commands/linsert)!
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

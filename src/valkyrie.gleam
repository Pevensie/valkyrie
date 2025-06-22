import bath
import gleam/bit_array
import gleam/dict
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
import gleam/time/timestamp
import gleam/uri
import mug

import valkyrie/resp

const protocol_version = 3

/// The configuration for connecting to a Redis-compatible database.
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
  Conflict
  RespError(String)
  ConnectionError
  Timeout
  TcpError(mug.Error)
  ServerError(String)
  PoolError(PoolError)
}

fn error_to_string(error) {
  case error {
    NotFound -> "Not found"
    Conflict -> "Conflict"
    RespError(message) -> "RESP protocol error: " <> message
    ConnectionError -> "Connection error"
    Timeout -> "Timeout"
    TcpError(error) -> "TCP error: " <> string.inspect(error)
    ServerError(message) -> message
    PoolError(error) ->
      "Pool error: "
      <> case error {
        NoResourcesAvailable -> "No resources available"
        PooledResourceCreateError(message) ->
          "Failed to create resource: " <> message
      }
  }
}

pub type StartError {
  ActorStartError(actor.StartError)
  PingError(Error)
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
/// This will attempt to authenticate with Redis using the provided configuration via
/// the `HELLO 3` command. See the documentation on [`supervised_pool`](#supervised_pool)
/// for more information on how to use connections in Valkyrie. The API is the same for
/// using single connections and supervised pools.
///
/// This establishes a direct connection to Redis using the provided configuration.
/// For high-throughput applications, consider using [`supervised_pool`](#supervised_pool).
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
    |> result.map_error(error_to_string)
  })
  |> bath.size(pool_size)
  |> bath.on_shutdown(fn(socket) {
    mug.shutdown(socket)
    |> result.unwrap(Nil)
  })
}

/// Start a connection pool for Redis connections.
///
/// This function will `PING` your Redis instance once to ensure it is reachable.
/// Further information about how to use the connection pool can be found in the
/// documentation for [`supervised_pool`](#supervised_pool).
///
/// Consider using [`supervised_pool`](#supervised_pool) instead, which
/// provides automatic restart capabilities and better integration with OTP supervision
/// trees. This function should only be used when you need to manage the pool lifecycle
/// manually.
pub fn start_pool(
  config config: Config,
  size pool_size: Int,
  timeout init_timeout: Int,
) -> Result(Connection, StartError) {
  use pool <- result.try(
    create_pool_builder(config, pool_size, init_timeout)
    |> bath.start(init_timeout)
    |> result.map(Pooled)
    |> result.map_error(ActorStartError),
  )

  ping(pool, option.None, 1000)
  |> result.replace(pool)
  |> result.map_error(PingError)
}

/// Create a supervised connection pool for Redis connections.
///
/// Returns a supervision specification for including the pool into your
/// supervision tree. The pool will be automatically restarted if it crashes,
/// making this the recommended approach for production applications.
///
/// Connections are created lazily when requested from the pool. On creation,
/// connections will call `HELLO 3` with any authentication information to authenticate
/// with the Redis-compatible server. No additional commands will be sent.
///
/// The [`Connection`](#Connection) value will be sent to the provided subject
/// when the pool is started.
///
/// ## Example
///
/// ```gleam
/// import gleam/erlang/process
/// import gleam/option
/// import gleam/otp/static_supervisor as supervisor
/// import valkyrie
///
/// pub fn main() {
///   // Create a subject to receive the connection once the supervision tree has been
///   // started. Use a named subject to make sure we can always receive the connection,
///   // even if our original process crashes.
///   let conn_receiver_name = process.new_name("valkyrie_conn_receiver")
///   let assert Ok(_) = process.register(process.self(), conn_receiver_name)
///
///   let conn_receiver = process.named_subject(conn_receiver_name)
///
///   // Define a pool of 10 connections to the default Redis instance on localhost.
///   let valkyrie_child_spec =
///     valkyrie.default_config()
///     |> valkyrie.supervised_pool(
///       receiver: conn_receiver,
///       size: 10,
///       timeout: 1000
///     )
///
///   // Start the pool under a supervisor
///   let assert Ok(_started) =
///     supervisor.new(supervisor.OneForOne)
///     |> supervisor.add(valkyrie_child_spec)
///     |> supervisor.start
///
///   // Receive the connection now that the pool is started
///   let assert Ok(conn) = process.receive(conn_receiver, 1000)
///
///   // Use the connection.
///   let assert Ok(_) = valkyrie.set(conn, "key", "value", option.None, 1000)
///   let assert Ok(_) = valkyrie.get(conn, "key", 1000)
///
///   // Do more stuff...
/// }
/// ```
pub fn supervised_pool(
  config config: Config,
  receiver subj: process.Subject(Connection),
  size pool_size: Int,
  timeout init_timeout: Int,
) -> supervision.ChildSpecification(process.Subject(bath.Msg(mug.Socket))) {
  create_pool_builder(config, pool_size, init_timeout)
  |> bath.supervised_map(subj, Pooled, init_timeout)
}

/// Shut down a Redis connection or connection pool.
///
/// For single connections, closes the socket immediately.
/// For pooled connections, gracefully shuts down the pool with the provided timeout.
///
/// You likely only need to use this function when you're using a single connection or
/// an unsupervised pool. You should let your supervision tree handle the shutdown of
/// supervised connection pools.
pub fn shutdown(conn: Connection, timeout: Int) -> Result(Nil, Nil) {
  case conn {
    Single(socket) -> mug.shutdown(socket) |> result.replace_error(Nil)
    Pooled(pool) ->
      bath.shutdown(pool, False, timeout) |> result.replace_error(Nil)
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
              ConnectionError | RespError(_) | TcpError(_) -> {
                bath.discard()
                |> bath.returning(Error(error))
              }
              NotFound | Conflict | ServerError(_) | Timeout -> {
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
    mug.send(socket, resp.encode_command(command))
    |> result.map_error(TcpError),
  )

  use reply <- result.try(socket_receive(socket, <<>>, monotonic_now(), timeout))
  case reply {
    [resp.SimpleError(error)] | [resp.BulkError(error)] ->
      Error(ServerError(error))
    value -> Ok(value)
  }
}

fn socket_receive(
  socket: mug.Socket,
  storage: BitArray,
  start_time: Int,
  timeout: Int,
) -> Result(List(resp.Value), Error) {
  case resp.decode_value(storage) {
    Ok(value) -> Ok(value)
    Error(_) -> {
      case monotonic_now() - start_time >= timeout * 1_000_000 {
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

@external(erlang, "valkyrie_ffi", "monotonic_now")
fn monotonic_now() -> Int

// ---------------------------------- //
// ----- Return value functions ----- //
// ---------------------------------- //

fn expect_cursor(cursor_string: String) -> Result(Int, Error) {
  case int.parse(cursor_string) {
    Ok(value) -> Ok(value)
    Error(_) ->
      Error(RespError("Expected integer cursor, got " <> cursor_string))
  }
}

fn expect_cursor_and_array(
  values: List(resp.Value),
) -> Result(#(List(String), Int), Error) {
  case values {
    [resp.Array([resp.BulkString(new_cursor_str), resp.Array(keys)])] -> {
      use new_cursor <- result.try(expect_cursor(new_cursor_str))
      use array <- result.try(
        list.try_map(keys, fn(item) {
          case item {
            resp.BulkString(value) -> Ok(value)
            _ ->
              Error(
                RespError(resp.error_string(expected: "string", got: [item])),
              )
          }
        }),
      )
      Ok(#(array, new_cursor))
    }
    _ ->
      Error(
        RespError(resp.error_string(expected: "cursor and array", got: values)),
      )
  }
}

fn expect_cursor_and_sorted_set_member_array(
  values: List(resp.Value),
) -> Result(#(List(#(String, Score)), Int), Error) {
  case values {
    [resp.Array([resp.BulkString(new_cursor_str), resp.Array(members)])] -> {
      use new_cursor <- result.try(expect_cursor(new_cursor_str))
      use array <- result.try(
        members
        |> list.sized_chunk(2)
        |> list.try_map(fn(item) {
          case item {
            [resp.BulkString(member), resp.BulkString(score)] ->
              case score_from_string(score) {
                Ok(score) -> Ok(#(member, score))
                _ -> Error(RespError("Invalid score: " <> score))
              }
            _ ->
              Error(
                RespError(resp.error_string(
                  expected: "member and score",
                  got: item,
                )),
              )
          }
        }),
      )
      Ok(#(array, new_cursor))
    }
    _ ->
      Error(
        RespError(resp.error_string(expected: "cursor and array", got: values)),
      )
  }
}

fn expect_cursor_and_hash_field_array(
  values: List(resp.Value),
) -> Result(#(List(#(String, String)), Int), Error) {
  case values {
    [resp.Array([resp.BulkString(new_cursor_str), resp.Array(members)])] -> {
      use new_cursor <- result.try(expect_cursor(new_cursor_str))
      use array <- result.try(
        members
        |> list.sized_chunk(2)
        |> list.try_map(fn(item) {
          case item {
            [resp.BulkString(field), resp.BulkString(value)] ->
              Ok(#(field, value))
            _ ->
              Error(
                RespError(resp.error_string(
                  expected: "member and score",
                  got: item,
                )),
              )
          }
        }),
      )
      Ok(#(array, new_cursor))
    }
    _ ->
      Error(
        RespError(resp.error_string(expected: "cursor and array", got: values)),
      )
  }
}

fn expect_integer(value: List(resp.Value)) -> Result(Int, Error) {
  case value {
    [resp.Integer(n)] -> Ok(n)
    _ -> Error(RespError(resp.error_string(expected: "integer", got: value)))
  }
}

fn expect_nullable_integer(value: List(resp.Value)) -> Result(Int, Error) {
  case value {
    [resp.Integer(n)] -> Ok(n)
    [resp.Null] -> Error(NotFound)
    _ -> Error(RespError(resp.error_string(expected: "integer", got: value)))
  }
}

fn expect_integer_boolean(value) {
  expect_integer(value)
  |> result.map(fn(n) { n == 1 })
}

fn expect_float(value: List(resp.Value)) -> Result(Float, Error) {
  case value {
    [resp.BulkString(new)] ->
      case float.parse(new) {
        Ok(f) -> Ok(f)
        Error(_) ->
          // Try parsing as int first (Redis sometimes returns "4" instead of "4.0")
          case int.parse(new) {
            Ok(i) -> Ok(int.to_float(i))
            Error(_) -> Error(RespError("Invalid float: " <> new))
          }
      }
    [resp.Double(value)] -> Ok(value)
    _ ->
      Error(
        RespError(resp.error_string(
          expected: "bulk string representation of a float, or double",
          got: value,
        )),
      )
  }
}

fn expect_nullable_float(value: List(resp.Value)) -> Result(Float, Error) {
  case value {
    [resp.BulkString(new)] ->
      case float.parse(new) {
        Ok(f) -> Ok(f)
        Error(_) ->
          // Try parsing as int first (Redis sometimes returns "4" instead of "4.0")
          case int.parse(new) {
            Ok(i) -> Ok(int.to_float(i))
            Error(_) -> Error(RespError("Invalid float: " <> new))
          }
      }
    [resp.Double(double)] -> Ok(double)
    [resp.Null] -> Error(NotFound)
    _ ->
      Error(
        RespError(resp.error_string(
          expected: "bulk string representation of a float, double, or null",
          got: value,
        )),
      )
  }
}

fn expect_any_string(value: List(resp.Value)) -> Result(String, Error) {
  case value {
    [resp.SimpleString(str)] | [resp.BulkString(str)] -> Ok(str)
    _ ->
      Error(
        RespError(resp.error_string(expected: "string or null", got: value)),
      )
  }
}

fn expect_simple_string(value: List(resp.Value)) -> Result(String, Error) {
  case value {
    [resp.SimpleString(str)] -> Ok(str)
    _ ->
      Error(RespError(resp.error_string(expected: "simple string", got: value)))
  }
}

fn expect_nullable_bulk_string(value: List(resp.Value)) -> Result(String, Error) {
  case value {
    [resp.BulkString(str)] -> Ok(str)
    [resp.Null] -> Error(NotFound)
    _ ->
      Error(
        RespError(resp.error_string(expected: "bulk string or null", got: value)),
      )
  }
}

fn expect_any_nullable_string(value: List(resp.Value)) -> Result(String, Error) {
  case value {
    [resp.SimpleString(str)] | [resp.BulkString(str)] -> Ok(str)
    [resp.Null] -> Error(NotFound)
    _ ->
      Error(
        RespError(resp.error_string(expected: "string or null", got: value)),
      )
  }
}

fn expect_bulk_string_array(
  value: List(resp.Value),
) -> Result(List(String), Error) {
  case value {
    [resp.Array(array)] ->
      list.try_map(array, fn(item) {
        case item {
          resp.BulkString(str) -> Ok(str)
          _ ->
            Error(
              RespError(resp.error_string(expected: "bulk string", got: [item])),
            )
        }
      })
    _ -> Error(RespError(resp.error_string(expected: "array", got: value)))
  }
}

fn expect_nullable_bulk_string_array(
  value: List(resp.Value),
) -> Result(List(Result(String, Error)), Error) {
  case value {
    [resp.Array(array)] ->
      list.map(array, fn(item) {
        case item {
          resp.BulkString(str) -> Ok(str)
          resp.Null -> Error(NotFound)
          _ ->
            Error(
              RespError(
                resp.error_string(expected: "string or null", got: [item]),
              ),
            )
        }
      })
      |> Ok
    _ -> Error(RespError(resp.error_string(expected: "array", got: value)))
  }
}

fn expect_bulk_string_set(
  value: List(resp.Value),
) -> Result(set.Set(String), Error) {
  case value {
    [resp.Set(s)] ->
      set.fold(over: s, from: Ok(set.new()), with: fn(acc, item) {
        use values <- result.try(acc)
        case item {
          resp.BulkString(str) -> Ok(set.insert(values, str))
          _ ->
            Error(
              RespError(resp.error_string(expected: "bulk string", got: [item])),
            )
        }
      })
    _ -> Error(RespError(resp.error_string(expected: "set", got: value)))
  }
}

fn expect_score(value) {
  case value {
    resp.Double(value) -> Ok(Double(value))
    resp.Infinity -> Ok(Infinity)
    resp.NegativeInfinity -> Ok(NegativeInfinity)
    _ -> Error(RespError(resp.error_string(expected: "score", got: [value])))
  }
}

fn expect_sorted_set_member_array(
  value: List(resp.Value),
) -> Result(List(#(String, Score)), Error) {
  case value {
    [resp.Array(members)] -> {
      use array <- result.then(
        members
        |> list.try_map(fn(item) {
          case item {
            resp.Array([resp.BulkString(member), score]) ->
              expect_score(score)
              |> result.map(fn(score) { #(member, score) })
            _ ->
              Error(
                RespError(
                  resp.error_string(expected: "member and score", got: [item]),
                ),
              )
          }
        }),
      )
      Ok(array)
    }
    _ -> Error(RespError(resp.error_string(expected: "array", got: value)))
  }
}

fn expect_nullable_rank_and_score(value) {
  case value {
    [resp.Array([resp.Integer(rank), score])] ->
      score
      |> expect_score
      |> result.map(fn(score) { #(rank, score) })
    [resp.Null] -> Error(NotFound)
    _ -> Error(RespError(resp.error_string(expected: "array", got: value)))
  }
}

fn expect_key_type(value) {
  case value {
    [resp.SimpleString(str)] ->
      case str {
        "set" -> Ok(Set)
        "list" -> Ok(List)
        "zset" -> Ok(ZSet)
        "hash" -> Ok(Hash)
        "string" -> Ok(String)
        "stream" -> Ok(Stream)
        "none" -> Error(NotFound)
        _ -> Error(RespError("Invalid key type: " <> str))
      }
    _ -> Error(RespError(resp.error_string(expected: "key type", got: value)))
  }
}

fn expect_map(value) {
  case value {
    [resp.Map(map)] -> Ok(map)
    [resp.Array([])] -> Error(NotFound)
    _ ->
      Error(
        RespError(resp.error_string(expected: "map or empty array", got: value)),
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
) -> Result(List(resp.Value), Error) {
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
    ZSet -> "zset"
    List -> "list"
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
    [resp.Array(array)] ->
      list.try_map(array, fn(item) {
        case item {
          resp.BulkString(value) -> Ok(value)
          _ ->
            Error(RespError(resp.error_string(expected: "string", got: [item])))
        }
      })
    _ ->
      Error(RespError(resp.error_string(expected: "string array", got: value)))
  }
}

/// Iterate incrementally over keys in the database.
///
/// Returns a tuple of `#(keys, next_cursor)`. Use the returned cursor
/// for subsequent calls. A cursor of 0 indicates the end of iteration.
///
/// You can provide optional pattern or key type filters to limit the keys returned.
///
/// See the [Redis SCAN documentation](https://redis.io/commands/scan) for more details.
pub fn scan(
  conn: Connection,
  cursor: Int,
  pattern_filter: option.Option(String),
  count: Int,
  key_type_filter: option.Option(KeyType),
  timeout: Int,
) -> Result(#(List(String), Int), Error) {
  let modifiers = case key_type_filter {
    option.Some(key_type) -> ["TYPE", key_type_to_string(key_type)]
    option.None -> []
  }
  let modifiers = ["COUNT", int.to_string(count), ..modifiers]
  let modifiers = case pattern_filter {
    option.Some(pattern) -> ["MATCH", pattern, ..modifiers]
    option.None -> modifiers
  }
  ["SCAN", int.to_string(cursor), ..modifiers]
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
/// Returns a list of `Result(String, Error)` values. The value will be
/// `Error(NotFound)` if the key doesn't exist.
///
/// See the [Redis MGET documentation](https://redis.io/commands/mget) for more details.
pub fn mget(
  conn: Connection,
  keys: List(String),
  timeout: Int,
) -> Result(List(Result(String, Error)), Error) {
  ["MGET", ..keys]
  |> execute(conn, _, timeout)
  |> result.try(expect_nullable_bulk_string_array)
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
  let modifiers =
    options
    |> option.map(fn(options) {
      let modifiers = case options.expiry_option {
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
      let modifiers = case options.return_old {
        True -> ["GET", ..modifiers]
        False -> modifiers
      }
      let modifiers = case options.existence_condition {
        option.Some(IfNotExists) -> ["NX", ..modifiers]
        option.Some(IfExists) -> ["XX", ..modifiers]
        option.None -> modifiers
      }
      modifiers
    })
    |> option.unwrap([])

  let command = ["SET", key, value, ..modifiers]

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
    [resp.Integer(n)] -> Ok(n)
    _ -> Error(RespError(resp.error_string(expected: "integer", got: value)))
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
pub fn decrby(
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
pub fn randomkey(conn: Connection, timeout: Int) -> Result(String, Error) {
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
) -> Result(Bool, Error) {
  ["PERSIST", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer_boolean)
}

/// Ping the Redis server.
///
/// If no message is provided, returns "PONG" if the server is responding.
/// Otherwise, returns the provided message if the server is responding.
///
/// See the [Redis PING documentation](https://redis.io/commands/ping) for more details.
pub fn ping(
  conn: Connection,
  message: option.Option(String),
  timeout: Int,
) -> Result(String, Error) {
  let #(message, expected) = case message {
    option.None -> #(["PING"], "PONG")
    option.Some(msg) -> #(["PING", msg], msg)
  }

  message
  |> execute(conn, _, timeout)
  |> result.try(fn(value) {
    case value {
      [resp.SimpleString(got)] | [resp.BulkString(got)] if got == expected ->
        Ok(got)
      _ -> Error(RespError(resp.error_string(expected: expected, got: value)))
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

/// Set a TTL in seconds on a key, relative to the current time.
///
/// Returns `True` if the timeout was set, `False` if the key doesn't exist or the
/// condition wasn't met.
///
/// **Note:** KeyDB does not support the `EXPIRE` command's conditional behaviour.
/// Make sure to pass `option.None` if to the `condition` argument if you're using
/// KeyDB.
///
/// See the [Redis EXPIRE documentation](https://redis.io/commands/expire) for more details.
pub fn expire(
  conn: Connection,
  key: String,
  ttl: Int,
  condition: option.Option(ExpireCondition),
  timeout: Int,
) -> Result(Bool, Error) {
  let expiry_condition = case condition {
    option.Some(IfNoExpiry) -> ["NX"]
    option.Some(IfHasExpiry) -> ["XX"]
    option.Some(IfGreaterThan) -> ["GT"]
    option.Some(IfLessThan) -> ["LT"]
    option.None -> []
  }

  ["EXPIRE", key, int.to_string(ttl), ..expiry_condition]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer_boolean)
}

/// Set a TTL in milliseconds on a key, relative to the current time.
///
/// Returns `True` if the timeout was set, `False` if the key doesn't exist or the
/// condition wasn't met.
///
/// **Note:** KeyDB does not support the `PEXPIRE` command's conditional behaviour.
/// Make sure to pass `option.None` if to the `condition` argument if you're using
/// KeyDB.
///
/// See the [Redis PEXPIRE documentation](https://redis.io/commands/pexpire) for more details.
pub fn pexpire(
  conn: Connection,
  key: String,
  ttl: Int,
  condition: option.Option(ExpireCondition),
  timeout: Int,
) -> Result(Bool, Error) {
  let expiry_condition = case condition {
    option.Some(IfNoExpiry) -> ["NX"]
    option.Some(IfHasExpiry) -> ["XX"]
    option.Some(IfGreaterThan) -> ["GT"]
    option.Some(IfLessThan) -> ["LT"]
    option.None -> []
  }

  ["PEXPIRE", key, int.to_string(ttl), ..expiry_condition]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer_boolean)
}

/// Set an absolute expiry on a key.
///
/// The expiry is specified as a Unix timestamp. If the timeout is in the past, the
/// key will be deleted immediately. Returns `True` if the timeout was set, `False` if
/// the key doesn't exist or the condition wasn't met.
///
/// **Note:** KeyDB does not support the `EXPIREAT` command's conditional behaviour.
/// Make sure to pass `option.None` if to the `condition` argument if you're using
/// KeyDB.
///
/// See the [Redis EXPIREAT documentation](https://redis.io/commands/expireat) for more details.
pub fn expireat(
  conn: Connection,
  key: String,
  timestamp: timestamp.Timestamp,
  condition: option.Option(ExpireCondition),
  timeout: Int,
) -> Result(Bool, Error) {
  let expiry_condition = case condition {
    option.Some(IfNoExpiry) -> ["NX"]
    option.Some(IfHasExpiry) -> ["XX"]
    option.Some(IfGreaterThan) -> ["GT"]
    option.Some(IfLessThan) -> ["LT"]
    option.None -> []
  }

  [
    "EXPIREAT",
    key,
    timestamp
      |> timestamp.to_unix_seconds
      |> float.round
      |> int.to_string,
    ..expiry_condition
  ]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer_boolean)
}

pub type Expiration {
  ExpiresAfter(Int)
  NoExpiration
}

/// Returns the absolute Unix timestamp (since January 1, 1970) in seconds at which the
/// given key will expire.
///
/// Will return `Ok(NoExpiration)` if the key has no associated expiration, or
/// `Error(NotFound)` if the key does not exist.
///
/// **Note:** KeyDB does not support the `EXPIRETIME` command.
///
/// See the [Redis EXPIRETIME documentation](https://redis.io/commands/expiretime) for more details.
pub fn expiretime(
  conn: Connection,
  key: String,
  timeout: Int,
) -> Result(Expiration, Error) {
  ["EXPIRETIME", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
  |> result.try(fn(value) {
    case value {
      -2 -> Error(NotFound)
      -1 -> Ok(NoExpiration)
      _ -> Ok(ExpiresAfter(value))
    }
  })
}

/// Returns the absolute Unix timestamp (since January 1, 1970) in milliseconds at
/// which the given key will expire.
///
/// Will return `Ok(NoExpiration)` if the key has no associated expiration, or
/// `Error(NotFound)` if the key does not exist.
///
/// **Note:** KeyDB does not support the `PEXPIRETIME` command.
///
/// See the [Redis PEXPIRETIME documentation](https://redis.io/commands/pexpiretime) for more details.
pub fn pexpiretime(
  conn: Connection,
  key: String,
  timeout: Int,
) -> Result(Expiration, Error) {
  ["PEXPIRETIME", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
  |> result.try(fn(value) {
    case value {
      -2 -> Error(NotFound)
      -1 -> Ok(NoExpiration)
      _ -> Ok(ExpiresAfter(value))
    }
  })
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

/// Remove and return elements from the left (head) of a list.
///
/// Returns `Error(NotFound)` if the key doesn't exist or the list is empty.
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
      [resp.Array([resp.BulkString(str)])] -> Ok(str)
      [resp.Array([])] -> Error(NotFound)
      [resp.Null] -> Error(NotFound)
      _ ->
        Error(
          RespError(resp.error_string(expected: "string or array", got: value)),
        )
    }
  })
}

/// Remove and return elements from the right (tail) of a list.
///
/// Returns `Error(NotFound)` if the key doesn't exist or the list is empty.
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
      [resp.Array([resp.BulkString(str)])] -> Ok(str)
      [resp.Array([])] -> Error(NotFound)
      [resp.Null] -> Error(NotFound)
      _ ->
        Error(
          RespError(resp.error_string(expected: "string or array", got: value)),
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

/// Add one or more members to a set.
///
/// Creates the set if it doesn't exist. Returns the number of members that were
/// actually added to the set (not including members that were already present).
///
/// See the [Redis SADD documentation](https://redis.io/commands/sadd) for more details.
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

/// Get the number of members in a set.
///
/// Returns 0 if the key doesn't exist.
///
/// See the [Redis SCARD documentation](https://redis.io/commands/scard) for more details.
pub fn scard(conn: Connection, key: String, timeout: Int) -> Result(Int, Error) {
  ["SCARD", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Check if a value is a member of a set.
///
/// Returns `True` if the value is a member of the set, `False` otherwise.
/// Returns `False` if the key doesn't exist.
///
/// See the [Redis SISMEMBER documentation](https://redis.io/commands/sismember) for more details.
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

/// Get all members of a set.
///
/// Returns an empty set if the key doesn't exist.
///
/// **Warning:** This command can be slow on large sets. Consider using
/// `sscan()` for production use with large sets.
///
/// See the [Redis SMEMBERS documentation](https://redis.io/commands/smembers) for more details.
pub fn smembers(
  conn: Connection,
  key: String,
  timeout: Int,
) -> Result(set.Set(String), Error) {
  ["SMEMBERS", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_bulk_string_set)
}

/// Iterate incrementally over members of a set.
///
/// Returns a tuple of `#(members, next_cursor)`. Use the returned cursor
/// for subsequent calls. A cursor of 0 indicates the end of iteration.
///
/// This is the recommended way to iterate over large sets in production
/// environments as it doesn't block the server like `smembers()`.
///
/// See the [Redis SSCAN documentation](https://redis.io/commands/sscan) for more details.
pub fn sscan(
  conn: Connection,
  key: String,
  cursor: Int,
  pattern_filter: option.Option(String),
  count: Int,
  timeout: Int,
) -> Result(#(List(String), Int), Error) {
  let modifiers = case pattern_filter {
    option.Some(pattern) -> ["MATCH", pattern, "COUNT", int.to_string(count)]
    option.None -> ["COUNT", int.to_string(count)]
  }

  ["SSCAN", key, int.to_string(cursor), ..modifiers]
  |> execute(conn, _, timeout)
  |> result.try(expect_cursor_and_array)
}

// -------------------------------- //
// ----- Sorted set functions ----- //
// -------------------------------- //

pub type Score {
  Infinity
  Double(Float)
  NegativeInfinity
}

fn score_to_string(score) {
  case score {
    Infinity -> "+inf"
    NegativeInfinity -> "-inf"
    Double(value) -> float.to_string(value)
  }
}

fn score_from_string(score: String) -> Result(Score, Nil) {
  case score {
    "+inf" | "inf" -> Ok(Infinity)
    "-inf" -> Ok(NegativeInfinity)
    _ ->
      case int.parse(score) {
        Ok(score) ->
          score
          |> int.to_float
          |> Double
          |> Ok

        Error(Nil) ->
          score
          |> float.parse
          |> result.map(Double)
      }
  }
}

pub type ZAddCondition {
  /// Equivalent to `NX`
  IfNotExistsInSet
  /// Equivalent to `XX`
  IfExistsInSet
  /// Equivalent to `XX LT`
  IfScoreLessThanExisting
  /// Equivalent to `XX GT`
  IfScoreGreaterThanExisting
}

/// Add one or more members with scores to a sorted set.
///
/// Creates the sorted set if it doesn't exist. The return value depends on the
/// `return_changed` parameter:
/// - If `False`: returns the number of new members added
/// - If `True`: returns the number of members added or updated
///
/// The `condition` parameter controls when the operation should proceed:
/// - `IfNotExistsInSet`: Only add new members (like NX option)
/// - `IfExistsInSet`: Only update existing members (like XX option)
/// - `IfScoreLessThanExisting`: Only update if new score is less than existing
/// - `IfScoreGreaterThanExisting`: Only update if new score is greater than existing
///
/// Returns `Error(Conflict)` if the operation was aborted due to a conflict with one
/// of the options.
///
/// See the [Redis ZADD documentation](https://redis.io/commands/zadd) for more details.
pub fn zadd(
  conn: Connection,
  key: String,
  members: List(#(String, Score)),
  condition: ZAddCondition,
  return_changed: Bool,
  timeout: Int,
) -> Result(Int, Error) {
  let changed_modifier = case return_changed {
    True -> ["CH"]
    False -> []
  }
  let modifiers = case condition {
    IfNotExistsInSet -> ["NX", ..changed_modifier]
    IfExistsInSet -> ["XX", ..changed_modifier]
    IfScoreLessThanExisting -> ["XX", "LT", ..changed_modifier]
    IfScoreGreaterThanExisting -> ["XX", "GT", ..changed_modifier]
  }
  let command =
    list.append(
      modifiers,
      list.flat_map(members, fn(member) {
        [score_to_string(member.1), member.0]
      }),
    )

  ["ZADD", key, ..command]
  |> execute(conn, _, timeout)
  |> result.try(fn(value) {
    case expect_nullable_integer(value) {
      Ok(integer) -> Ok(integer)

      // This API only returns null if there's a conflict
      Error(NotFound) -> Error(Conflict)
      Error(error) -> Error(error)
    }
  })
}

/// Increment the score of a member in a sorted set.
///
/// If the member doesn't exist, it's added with the given score as its initial value.
/// Returns the new score of the member after incrementing.
///
/// See the [Redis ZINCRBY documentation](https://redis.io/commands/zincrby) for more details.
pub fn zincrby(
  conn: Connection,
  key: String,
  member: String,
  delta: Score,
  timeout: Int,
) -> Result(Float, Error) {
  ["ZINCRBY", key, score_to_string(delta), member]
  |> execute(conn, _, timeout)
  |> result.try(expect_float)
}

/// Get the number of members in a sorted set.
///
/// Returns 0 if the key doesn't exist.
///
/// See the [Redis ZCARD documentation](https://redis.io/commands/zcard) for more details.
pub fn zcard(conn: Connection, key: String, timeout: Int) -> Result(Int, Error) {
  ["ZCARD", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Count the members in a sorted set within a score range.
///
/// Both `min` and `max` scores are inclusive by default. Use `Score` variants
/// to specify infinity bounds for open-ended ranges.
///
/// See the [Redis ZCOUNT documentation](https://redis.io/commands/zcount) for more details.
pub fn zcount(
  conn: Connection,
  key: String,
  min: Score,
  max: Score,
  timeout: Int,
) -> Result(Int, Error) {
  ["ZCOUNT", key, score_to_string(min), score_to_string(max)]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Get the score of a member in a sorted set.
///
/// Returns `Error(NotFound)` if the key doesn't exist or the member is not in the set.
///
/// See the [Redis ZSCORE documentation](https://redis.io/commands/zscore) for more details.
pub fn zscore(
  conn: Connection,
  key: String,
  member: String,
  timeout: Int,
) -> Result(Float, Error) {
  ["ZSCORE", key, member]
  |> execute(conn, _, timeout)
  |> result.try(expect_nullable_float)
}

/// Iterate incrementally over members and scores of a sorted set.
///
/// Returns a tuple of `#(members_with_scores, next_cursor)`. Use the returned cursor
/// for subsequent calls. A cursor of 0 indicates the end of iteration.
///
/// This is the recommended way to iterate over large sorted sets in production
/// environments.
///
/// See the [Redis ZSCAN documentation](https://redis.io/commands/zscan) for more details.
pub fn zscan(
  conn: Connection,
  key: String,
  cursor: Int,
  pattern_filter: option.Option(String),
  count: Int,
  timeout: Int,
) -> Result(#(List(#(String, Score)), Int), Error) {
  let modifiers = case pattern_filter {
    option.Some(pattern) -> ["MATCH", pattern, "COUNT", int.to_string(count)]
    option.None -> ["COUNT", int.to_string(count)]
  }

  ["ZSCAN", key, int.to_string(cursor), ..modifiers]
  |> execute(conn, _, timeout)
  |> result.try(expect_cursor_and_sorted_set_member_array)
}

/// Remove one or more members from a sorted set.
///
/// Returns the number of members that were actually removed from the set
/// (not including members that were not present).
///
/// See the [Redis ZREM documentation](https://redis.io/commands/zrem) for more details.
pub fn zrem(
  conn: Connection,
  key: String,
  members: List(String),
  timeout: Int,
) -> Result(Int, Error) {
  ["ZREM", key, ..members]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Return random members from a sorted set with their scores.
///
/// Returns up to `count` random members. The members are returned with their scores.
/// If the sorted set is smaller than `count`, all members are returned.
///
/// See the [Redis ZRANDMEMBER documentation](https://redis.io/commands/zrandmember) for more details.
pub fn zrandmember(
  conn: Connection,
  key: String,
  count: Int,
  timeout: Int,
) -> Result(List(#(String, Score)), Error) {
  ["ZRANDMEMBER", key, int.to_string(count), "WITHSCORES"]
  |> execute(conn, _, timeout)
  |> result.try(expect_sorted_set_member_array)
}

/// Remove and return members with the lowest scores from a sorted set.
///
/// Returns up to `count` members with the lowest scores. The members are removed
/// from the sorted set and returned with their scores.
///
/// See the [Redis ZPOPMIN documentation](https://redis.io/commands/zpopmin) for more details.
pub fn zpopmin(
  conn: Connection,
  key: String,
  count: Int,
  timeout: Int,
) -> Result(List(#(String, Score)), Error) {
  ["ZPOPMIN", key, int.to_string(count)]
  |> execute(conn, _, timeout)
  |> result.try(expect_sorted_set_member_array)
}

/// Remove and return members with the highest scores from a sorted set.
///
/// Returns up to `count` members with the highest scores. The members are removed
/// from the sorted set and returned with their scores.
///
/// See the [Redis ZPOPMAX documentation](https://redis.io/commands/zpopmax) for more details.
pub fn zpopmax(
  conn: Connection,
  key: String,
  count: Int,
  timeout: Int,
) -> Result(List(#(String, Score)), Error) {
  ["ZPOPMAX", key, int.to_string(count)]
  |> execute(conn, _, timeout)
  |> result.try(expect_sorted_set_member_array)
}

pub type NumericBound(a) {
  NumericInclusive(a)
  NumericExclusive(a)
}

fn numeric_bound_to_string(
  bound: NumericBound(a),
  to_string_func: fn(a) -> String,
) -> String {
  case bound {
    NumericInclusive(value) -> to_string_func(value)
    NumericExclusive(value) -> "(" <> to_string_func(value)
  }
}

/// Get a range of members from a sorted set by rank (index).
///
/// Returns members with their scores in the specified rank range.
/// Ranks are 0-based, with 0 being the member with the lowest score.
/// Use `NumericBound` to specify inclusive or exclusive bounds.
///
/// If `reverse` is `True`, returns members in descending score order.
///
/// See the [Redis ZRANGE documentation](https://redis.io/commands/zrange) for more details.
pub fn zrange(
  conn: Connection,
  key: String,
  start: NumericBound(Int),
  stop: NumericBound(Int),
  reverse: Bool,
  timeout: Int,
) -> Result(List(#(String, Score)), Error) {
  let modifiers = case reverse {
    True -> ["REV", "WITHSCORES"]
    False -> ["WITHSCORES"]
  }
  [
    "ZRANGE",
    key,
    numeric_bound_to_string(start, int.to_string),
    numeric_bound_to_string(stop, int.to_string),
    ..modifiers
  ]
  |> execute(conn, _, timeout)
  |> result.try(expect_sorted_set_member_array)
}

/// Get a range of members from a sorted set by score.
///
/// Returns all members with scores within the specified score range.
/// Use `NumericBound` with `Score` values to specify inclusive or exclusive bounds,
/// including infinity bounds for open-ended ranges.
///
/// If `reverse` is `True`, returns members in descending score order.
///
/// See the [Redis ZRANGE documentation](https://redis.io/commands/zrange) for more details.
pub fn zrange_byscore(
  conn: Connection,
  key: String,
  start: NumericBound(Score),
  stop: NumericBound(Score),
  reverse: Bool,
  timeout: Int,
) -> Result(List(#(String, Score)), Error) {
  let modifiers = case reverse {
    True -> ["REV", "WITHSCORES"]
    False -> ["WITHSCORES"]
  }

  [
    "ZRANGE",
    key,
    numeric_bound_to_string(start, score_to_string),
    numeric_bound_to_string(stop, score_to_string),
    "BYSCORE",
    ..modifiers
  ]
  |> execute(conn, _, timeout)
  |> result.try(expect_sorted_set_member_array)
}

pub type LexBound {
  LexInclusive(String)
  LexExclusive(String)
  LexPositiveInfinity
  LexNegativeInfinity
}

fn lex_bound_to_string(bound: LexBound) -> String {
  case bound {
    LexInclusive(value) -> "[" <> value
    LexExclusive(value) -> "(" <> value
    LexPositiveInfinity -> "+"
    LexNegativeInfinity -> "-"
  }
}

/// Get a range of members from a sorted set by lexicographic order.
///
/// When all members have the same score, this command returns members
/// within the specified lexicographic range. Use `LexBound` to specify
/// inclusive/exclusive bounds or infinity bounds.
///
/// If `reverse` is `True`, returns members in reverse lexicographic order.
/// Note: This only works correctly when all members have the same score.
///
/// See the [Redis ZRANGE documentation](https://redis.io/commands/zrange) for more details.
pub fn zrange_bylex(
  conn: Connection,
  key: String,
  start: LexBound,
  stop: LexBound,
  reverse: Bool,
  timeout: Int,
) -> Result(List(String), Error) {
  let modifiers = case reverse {
    True -> ["REV"]
    False -> []
  }

  [
    "ZRANGE",
    key,
    lex_bound_to_string(start),
    lex_bound_to_string(stop),
    "BYLEX",
    ..modifiers
  ]
  |> execute(conn, _, timeout)
  |> result.try(expect_bulk_string_array)
}

/// Get the rank (index) of a member in a sorted set.
///
/// Returns the 0-based rank of the member, where 0 is the member with the lowest score.
/// Returns `Error(NotFound)` if the key doesn't exist or the member is not in the set.
///
/// See the [Redis ZRANK documentation](https://redis.io/commands/zrank) for more details.
pub fn zrank(
  conn: Connection,
  key: String,
  member: String,
  timeout: Int,
) -> Result(Int, Error) {
  ["ZRANK", key, member]
  |> execute(conn, _, timeout)
  |> result.try(expect_nullable_integer)
}

/// Get the rank (index) and score of a member in a sorted set.
///
/// Returns a tuple of `#(rank, score)` where rank is 0-based (lowest score = 0).
/// Returns `Error(NotFound)` if the key doesn't exist or the member is not in the set.
///
/// **Note:** This command is not supported by KeyDB.
///
/// See the [Redis ZRANK documentation](https://redis.io/commands/zrank) for more details.
pub fn zrank_withscore(
  conn: Connection,
  key: String,
  member: String,
  timeout: Int,
) -> Result(#(Int, Score), Error) {
  ["ZRANK", key, member, "WITHSCORE"]
  |> execute(conn, _, timeout)
  |> result.try(expect_nullable_rank_and_score)
}

/// Get the reverse rank (index) of a member in a sorted set.
///
/// Returns the 0-based rank of the member in descending order, where 0 is the member
/// with the highest score. Returns `Error(NotFound)` if the key doesn't exist or
/// the member is not in the set.
///
/// See the [Redis ZREVRANK documentation](https://redis.io/commands/zrevrank) for more details.
pub fn zrevrank(
  conn: Connection,
  key: String,
  member: String,
  timeout: Int,
) -> Result(Int, Error) {
  ["ZREVRANK", key, member]
  |> execute(conn, _, timeout)
  |> result.try(expect_nullable_integer)
}

/// Get the reverse rank (index) and score of a member in a sorted set.
///
/// Returns a tuple of `#(reverse_rank, score)` where reverse rank is 0-based in
/// descending order (highest score = 0). Returns `Error(NotFound)` if the key
/// doesn't exist or the member is not in the set.
///
/// **Note:** This command is not supported by KeyDB.
///
/// See the [Redis ZREVRANK documentation](https://redis.io/commands/zrevrank) for more details.
pub fn zrevrank_withscore(
  conn: Connection,
  key: String,
  member: String,
  timeout: Int,
) -> Result(#(Int, Score), Error) {
  ["ZREVRANK", key, member, "WITHSCORE"]
  |> execute(conn, _, timeout)
  |> result.try(expect_nullable_rank_and_score)
}

// -------------------------- //
// ----- Hash functions ----- //
// -------------------------- //

/// Set field-value pairs in a hash.
///
/// Creates the hash if it doesn't exist. Returns the number of fields that were
/// added (not including fields that were updated with new values).
///
/// See the [Redis HSET documentation](https://redis.io/commands/hset) for more details.
pub fn hset(
  conn: Connection,
  key: String,
  values: dict.Dict(String, String),
  timeout: Int,
) -> Result(Int, Error) {
  let values =
    values
    |> dict.to_list
    |> list.flat_map(fn(item) { [item.0, item.1] })

  ["HSET", key, ..values]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Set a field in a hash, only if the field doesn't already exist.
///
/// Returns `True` if the field was set, `False` if the field already exists.
/// Creates the hash if it doesn't exist.
///
/// See the [Redis HSETNX documentation](https://redis.io/commands/hsetnx) for more details.
pub fn hsetnx(
  conn: Connection,
  key: String,
  field: String,
  value: String,
  timeout: Int,
) -> Result(Bool, Error) {
  ["HSETNX", key, field, value]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer_boolean)
}

/// Get the number of fields in a hash.
///
/// Returns 0 if the key doesn't exist.
///
/// See the [Redis HLEN documentation](https://redis.io/commands/hlen) for more details.
pub fn hlen(conn: Connection, key: String, timeout: Int) -> Result(Int, Error) {
  ["HLEN", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Get all field names in a hash.
///
/// Returns an empty list if the key doesn't exist.
///
/// See the [Redis HKEYS documentation](https://redis.io/commands/hkeys) for more details.
pub fn hkeys(
  conn: Connection,
  key: String,
  timeout: Int,
) -> Result(List(String), Error) {
  ["HKEYS", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_bulk_string_array)
}

/// Get the value of a field in a hash.
///
/// Returns `Error(NotFound)` if the key doesn't exist or the field doesn't exist.
///
/// See the [Redis HGET documentation](https://redis.io/commands/hget) for more details.
pub fn hget(
  conn: Connection,
  key: String,
  field: String,
  timeout: Int,
) -> Result(String, Error) {
  ["HGET", key, field]
  |> execute(conn, _, timeout)
  |> result.try(expect_nullable_bulk_string)
}

/// Get all field-value pairs in a hash.
///
/// Returns an empty dictionary if the key doesn't exist.
///
/// **Note:** The return type uses raw `resp.Value` types. This may change in future versions.
///
/// See the [Redis HGETALL documentation](https://redis.io/commands/hgetall) for more details.
pub fn hgetall(
  conn: Connection,
  key: String,
  timeout: Int,
) -> Result(dict.Dict(resp.Value, resp.Value), Error) {
  ["HGETALL", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_map)
}

/// Get the values of multiple fields in a hash.
///
/// Returns a list of `Result(String, Error)` values. The value will be
/// `Error(NotFound)` if the field doesn't exist.
///
/// See the [Redis HMGET documentation](https://redis.io/commands/hmget) for more details.
pub fn hmget(
  conn: Connection,
  key: String,
  fields: List(String),
  timeout: Int,
) -> Result(List(Result(String, Error)), Error) {
  ["HMGET", key, ..fields]
  |> execute(conn, _, timeout)
  |> result.try(expect_nullable_bulk_string_array)
}

/// Get the string length of a field's value in a hash.
///
/// Returns 0 if the key doesn't exist or the field doesn't exist.
///
/// See the [Redis HSTRLEN documentation](https://redis.io/commands/hstrlen) for more details.
pub fn hstrlen(
  conn: Connection,
  key: String,
  field: String,
  timeout: Int,
) -> Result(Int, Error) {
  ["HSTRLEN", key, field]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Get all values in a hash.
///
/// Returns an empty list if the key doesn't exist.
///
/// See the [Redis HVALS documentation](https://redis.io/commands/hvals) for more details.
pub fn hvals(
  conn: Connection,
  key: String,
  timeout: Int,
) -> Result(List(String), Error) {
  ["HVALS", key]
  |> execute(conn, _, timeout)
  |> result.try(expect_bulk_string_array)
}

/// Delete one or more fields from a hash.
///
/// Returns the number of fields that were actually removed from the hash
/// (not including fields that didn't exist).
///
/// See the [Redis HDEL documentation](https://redis.io/commands/hdel) for more details.
pub fn hdel(
  conn: Connection,
  key: String,
  fields: List(String),
  timeout: Int,
) -> Result(Int, Error) {
  ["HDEL", key, ..fields]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Check if a field exists in a hash.
///
/// Returns `True` if the field exists, `False` otherwise.
/// Returns `False` if the key doesn't exist.
///
/// See the [Redis HEXISTS documentation](https://redis.io/commands/hexists) for more details.
pub fn hexists(
  conn: Connection,
  key: String,
  field: String,
  timeout: Int,
) -> Result(Bool, Error) {
  ["HEXISTS", key, field]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer_boolean)
}

/// Increment the integer value of a field in a hash by the given amount.
///
/// If the field doesn't exist, it's set to 0 before incrementing.
/// Creates the hash if it doesn't exist. Returns the new value after incrementing.
///
/// See the [Redis HINCRBY documentation](https://redis.io/commands/hincrby) for more details.
pub fn hincrby(
  conn: Connection,
  key: String,
  field: String,
  value: Int,
  timeout: Int,
) -> Result(Int, Error) {
  ["HINCRBY", key, field, int.to_string(value)]
  |> execute(conn, _, timeout)
  |> result.try(expect_integer)
}

/// Increment the floating point value of a field in a hash by the given amount.
///
/// If the field doesn't exist, it's set to 0 before incrementing.
/// Creates the hash if it doesn't exist. Returns the new value after incrementing.
///
/// See the [Redis HINCRBYFLOAT documentation](https://redis.io/commands/hincrbyfloat) for more details.
pub fn hincrbyfloat(
  conn: Connection,
  key: String,
  field: String,
  value: Float,
  timeout: Int,
) -> Result(Float, Error) {
  ["HINCRBYFLOAT", key, field, float.to_string(value)]
  |> execute(conn, _, timeout)
  |> result.try(expect_float)
}

/// Iterate incrementally over field-value pairs in a hash.
///
/// Returns a tuple of `#(field_value_pairs, next_cursor)`. Use the returned cursor
/// for subsequent calls. A cursor of 0 indicates the end of iteration.
///
/// This is the recommended way to iterate over large hashes in production
/// environments.
///
/// See the [Redis HSCAN documentation](https://redis.io/commands/hscan) for more details.
pub fn hscan(
  conn: Connection,
  key: String,
  cursor: Int,
  pattern_filter: option.Option(String),
  count: Int,
  timeout: Int,
) -> Result(#(List(#(String, String)), Int), Error) {
  let modifiers = case pattern_filter {
    option.Some(pattern) -> ["MATCH", pattern, "COUNT", int.to_string(count)]
    option.None -> ["COUNT", int.to_string(count)]
  }
  ["HSCAN", key, int.to_string(cursor), ..modifiers]
  |> execute(conn, _, timeout)
  |> result.try(expect_cursor_and_hash_field_array)
}

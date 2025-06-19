//// All timeouts are in milliseconds

import gleam/float
import gleam/int
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/result

import valkyrie/error
import valkyrie/internal/client
import valkyrie/internal/command
import valkyrie/internal/protocol

pub type Message =
  client.Message

/// The configuration for a pool of connections to the database.
pub type Config {
  Config(host: String, port: Int, timeout: Int, pool_size: Int, auth: Auth)
}

pub type Auth {
  NoAuth
  PasswordOnly(String)
  UsernameAndPassword(username: String, password: String)
}

fn auth_to_options_list(auth: Auth) -> List(String) {
  case auth {
    NoAuth -> []
    PasswordOnly(password) -> ["AUTH", "default", password]
    UsernameAndPassword(username, password) -> ["AUTH", username, password]
  }
}

pub fn default_config() -> Config {
  Config(
    host: "localhost",
    port: 6379,
    timeout: 1024,
    pool_size: 3,
    auth: NoAuth,
  )
}

pub fn host(config: Config, host: String) -> Config {
  Config(..config, host:)
}

pub fn port(config: Config, port: Int) -> Config {
  Config(..config, port:)
}

pub fn timeout(config: Config, timeout: Int) -> Config {
  Config(..config, timeout:)
}

pub fn size(config: Config, size: Int) -> Config {
  Config(..config, pool_size: size)
}

pub fn auth(config: Config, auth: Auth) -> Config {
  Config(..config, auth:)
}

pub type KeyType {
  Set
  List
  ZSet
  Hash
  String
  Stream
}

pub type ExpireCondition {
  NX
  XX
  GT
  LT
}

pub fn start(config: Config) -> Result(client.Client, actor.StartError) {
  client.start(
    config.host,
    config.port,
    config.timeout,
    config.pool_size,
    auth_to_options_list(config.auth),
  )
}

pub fn shutdown(client: client.Client) -> Result(Nil, Nil) {
  client.shutdown(client)
}

/// Use this if you need to construct a command not already covered by `valkyrie`
pub fn execute(
  client: client.Client,
  parts: List(String),
  timeout: Int,
) -> Result(List(protocol.Value), error.Error) {
  parts
  |> command.custom
  |> client.execute(client, _, timeout)
}

/// use this if you need to construct a blocking command not already covered by `valkyrie`
pub fn execute_blocking(client, parts: List(String), timeout: Int) {
  parts
  |> command.custom
  |> client.execute_blocking(client, _, timeout)
}

/// see [here](https://redis.io/commands/keys)!
pub fn keys(client, pattern: String, timeout: Int) {
  command.keys(pattern)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.Array(array)] ->
        list.try_map(array, fn(item) {
          case item {
            protocol.BulkString(value) -> Ok(value)
            _ -> Error(error.RESPError)
          }
        })
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/scan)!
pub fn scan(client, cursor: Int, count: Int, timeout: Int) {
  command.scan(cursor, count)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [
        protocol.Array([
          protocol.BulkString(new_cursor_str),
          protocol.Array(keys),
        ]),
      ] ->
        case int.parse(new_cursor_str) {
          Ok(new_cursor) -> {
            use array <- result.then(
              list.try_map(keys, fn(item) {
                case item {
                  protocol.BulkString(value) -> Ok(value)
                  _ -> Error(error.RESPError)
                }
              }),
            )
            Ok(#(array, new_cursor))
          }
          Error(Nil) -> Error(error.RESPError)
        }
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/scan)!
pub fn scan_pattern(
  client,
  cursor: Int,
  pattern: String,
  count: Int,
  timeout: Int,
) {
  command.scan_pattern(cursor, pattern, count)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [
        protocol.Array([
          protocol.BulkString(new_cursor_str),
          protocol.Array(keys),
        ]),
      ] ->
        case int.parse(new_cursor_str) {
          Ok(new_cursor) -> {
            use array <- result.then(
              list.try_map(keys, fn(item) {
                case item {
                  protocol.BulkString(value) -> Ok(value)
                  _ -> Error(error.RESPError)
                }
              }),
            )
            Ok(#(array, new_cursor))
          }
          Error(Nil) -> Error(error.RESPError)
        }
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/scan)!
pub fn scan_with_type(
  client,
  cursor: Int,
  key_type: KeyType,
  count: Int,
  timeout: Int,
) {
  case key_type {
    Set -> command.scan_with_type(cursor, "set", count)
    List -> command.scan_with_type(cursor, "list", count)
    ZSet -> command.scan_with_type(cursor, "zset", count)
    Hash -> command.scan_with_type(cursor, "hash", count)
    String -> command.scan_with_type(cursor, "string", count)
    Stream -> command.scan_with_type(cursor, "stream", count)
  }
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [
        protocol.Array([
          protocol.BulkString(new_cursor_str),
          protocol.Array(keys),
        ]),
      ] ->
        case int.parse(new_cursor_str) {
          Ok(new_cursor) -> {
            use array <- result.then(
              list.try_map(keys, fn(item) {
                case item {
                  protocol.BulkString(value) -> Ok(value)
                  _ -> Error(error.RESPError)
                }
              }),
            )
            Ok(#(array, new_cursor))
          }
          Error(Nil) -> Error(error.RESPError)
        }
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/scan)!
pub fn scan_pattern_with_type(
  client,
  cursor: Int,
  key_type: KeyType,
  pattern: String,
  count: Int,
  timeout: Int,
) {
  case key_type {
    Set -> command.scan_pattern_with_type(cursor, "set", pattern, count)
    List -> command.scan_pattern_with_type(cursor, "list", pattern, count)
    ZSet -> command.scan_pattern_with_type(cursor, "zset", pattern, count)
    Hash -> command.scan_pattern_with_type(cursor, "hash", pattern, count)
    String -> command.scan_pattern_with_type(cursor, "string", pattern, count)
    Stream -> command.scan_pattern_with_type(cursor, "stream", pattern, count)
  }
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [
        protocol.Array([
          protocol.BulkString(new_cursor_str),
          protocol.Array(keys),
        ]),
      ] ->
        case int.parse(new_cursor_str) {
          Ok(new_cursor) -> {
            use array <- result.then(
              list.try_map(keys, fn(item) {
                case item {
                  protocol.BulkString(value) -> Ok(value)
                  _ -> Error(error.RESPError)
                }
              }),
            )
            Ok(#(array, new_cursor))
          }
          Error(Nil) -> Error(error.RESPError)
        }
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/exists)!
pub fn exists(client, keys: List(String), timeout: Int) {
  command.exists(keys)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.Integer(n)] -> Ok(n)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/get)!
pub fn get(client, key: String, timeout: Int) {
  command.get(key)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.SimpleString(str)] | [protocol.BulkString(str)] -> Ok(str)
      [protocol.Null] -> Error(error.NotFound)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/mget)!
pub fn mget(client, keys: List(String), timeout: Int) {
  command.mget(keys)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.Array(array)] ->
        list.try_map(array, fn(item) {
          case item {
            protocol.BulkString(str) -> Ok(str)
            protocol.Null -> Error(error.NotFound)
            _ -> Error(error.RESPError)
          }
        })
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/append)!
pub fn append(client, key: String, value: String, timeout: Int) {
  command.append(key, value)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.Integer(n)] -> Ok(n)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/set)!
pub fn set(client, key: String, value: String, timeout: Int) {
  command.set(key, value, [])
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.SimpleString(str)] | [protocol.BulkString(str)] -> Ok(str)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/set)!
pub fn set_new(client, key: String, value: String, timeout: Int) {
  command.set(key, value, [command.NX])
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.SimpleString(str)] | [protocol.BulkString(str)] -> Ok(str)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/set)!
pub fn set_existing(client, key: String, value: String, timeout: Int) {
  command.set(key, value, [command.XX, command.GET])
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.SimpleString(str)] | [protocol.BulkString(str)] -> Ok(str)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/mset)!
pub fn mset(client, kv_list: List(#(String, String)), timeout: Int) {
  command.mset(kv_list)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.SimpleString(str)] | [protocol.BulkString(str)] -> Ok(str)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/del)!
pub fn del(client, keys: List(String), timeout: Int) {
  command.del(keys)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.Integer(n)] -> Ok(n)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/incr)!
pub fn incr(client, key: String, timeout: Int) {
  command.incr(key)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.Integer(new)] -> Ok(new)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/incrby)!
pub fn incr_by(client, key: String, value: Int, timeout: Int) {
  command.incr_by(key, value)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.Integer(new)] -> Ok(new)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/incrbyfloat)!
pub fn incr_by_float(client, key: String, value: Float, timeout: Int) {
  command.incr_by_float(key, value)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.BulkString(new)] ->
        float.parse(new)
        |> result.replace_error(error.RESPError)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/decr)!
pub fn decr(client, key: String, timeout: Int) {
  command.decr(key)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.Integer(new)] -> Ok(new)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/decrby)!
pub fn decr_by(client, key: String, value: Int, timeout: Int) {
  command.decr_by(key, value)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.Integer(new)] -> Ok(new)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/randomkey)!
pub fn random_key(client, timeout: Int) {
  command.random_key()
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.BulkString(str)] -> Ok(str)
      [protocol.Null] -> Error(error.NotFound)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/type)!
pub fn key_type(client, key: String, timeout: Int) {
  command.key_type(key)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.SimpleString(str)] ->
        case str {
          "set" -> Ok(Set)
          "list" -> Ok(List)
          "zset" -> Ok(ZSet)
          "hash" -> Ok(Hash)
          "string" -> Ok(String)
          "stream" -> Ok(Stream)
          _ -> Error(error.RESPError)
        }

      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/rename)!
pub fn rename(client, key: String, new_key: String, timeout: Int) {
  command.rename(key, new_key)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.SimpleString(str)] -> Ok(str)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/renamenx)!
pub fn renamenx(client, key: String, new_key: String, timeout: Int) {
  command.renamenx(key, new_key)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.Integer(n)] -> Ok(n)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/persist)!
pub fn persist(client, key: String, timeout: Int) {
  command.persist(key)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.Integer(n)] -> Ok(n)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/ping)!
/// for use with a custom message, use `ping_message/3`.
pub fn ping(client, timeout: Int) {
  command.ping()
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.SimpleString("PONG")] -> Ok("PONG")
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/ping)!
pub fn ping_message(client, message: String, timeout: Int) {
  command.ping_message(message)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.BulkString(message)] -> Ok(message)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/expire)!
pub fn expire(client, key: String, ttl: Int, timeout: Int) {
  command.expire(key, ttl, option.None)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.Integer(n)] -> Ok(n)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/expire)!
pub fn expire_if(
  client,
  key: String,
  ttl: Int,
  condition: ExpireCondition,
  timeout: Int,
) {
  case condition {
    NX -> command.expire(key, ttl, option.Some("NX"))
    XX -> command.expire(key, ttl, option.Some("XX"))
    GT -> command.expire(key, ttl, option.Some("GT"))
    LT -> command.expire(key, ttl, option.Some("LT"))
  }
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.Integer(n)] -> Ok(n)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

pub type Next {
  Continue
  UnsubscribeFromAll
  UnsubscribeFrom(List(String))
}

/// see [here](https://redis.io/commands/publish)!
pub fn publish(client, channel: String, message: String, timeout: Int) {
  command.publish(channel, message)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    case value {
      [protocol.Integer(n)] -> Ok(n)
      _ -> Error(error.RESPError)
    }
  })
  |> result.flatten
}

/// see [here](https://redis.io/commands/subscribe)!
/// Also see [here](https://redis.io/docs/manual/keyspace-notifications)!
pub fn subscribe(
  client: client.Client,
  channels: List(String),
  init_handler: fn(String, Int) -> Nil,
  message_handler: fn(String, String) -> Next,
  timeout: Int,
) {
  let _ =
    command.subscribe(channels)
    |> client.execute_blocking(client, _, timeout)
    |> result.map(fn(value) {
      list.each(value, fn(item) {
        case item {
          protocol.Push([
            protocol.BulkString("subscribe"),
            protocol.BulkString(channel),
            protocol.Integer(n),
          ]) -> Ok(init_handler(channel, n))
          _ -> Error(error.RESPError)
        }
      })
    })

  use value <- client.receive_forever(client, timeout)
  case value {
    Ok([
      protocol.Push([
        protocol.BulkString("message"),
        protocol.BulkString(channel),
        protocol.BulkString(message),
      ]),
    ]) ->
      case message_handler(channel, message) {
        Continue -> True
        UnsubscribeFromAll -> {
          let _ = unsubscribe_from_all(client, timeout)
          False
        }
        UnsubscribeFrom(channels) ->
          case unsubscribe(client, channels, timeout) {
            Ok(result) -> result
            Error(_) -> False
          }
      }

    _ -> False
  }
}

/// see [here](https://redis.io/commands/psubscribe)!
/// Also see [here](https://redis.io/docs/manual/keyspace-notifications)!
pub fn subscribe_to_patterns(
  client,
  patterns: List(String),
  init_handler: fn(String, Int) -> Nil,
  message_handler: fn(String, String, String) -> Next,
  timeout: Int,
) {
  let _ =
    command.subscribe_to_patterns(patterns)
    |> client.execute_blocking(client, _, timeout)
    |> result.map(fn(value) {
      list.each(value, fn(item) {
        case item {
          protocol.Push([
            protocol.BulkString("psubscribe"),
            protocol.BulkString(channel),
            protocol.Integer(n),
          ]) -> init_handler(channel, n)
          _ -> Nil
        }
      })
    })

  use value <- client.receive_forever(client, timeout)
  case value {
    Ok([
      protocol.Push([
        protocol.BulkString("pmessage"),
        protocol.BulkString(pattern),
        protocol.BulkString(channel),
        protocol.BulkString(message),
      ]),
    ]) ->
      case message_handler(pattern, channel, message) {
        Continue -> True
        UnsubscribeFromAll -> {
          let _ = unsubscribe_from_all_patterns(client, timeout)
          False
        }
        UnsubscribeFrom(patterns) -> {
          case unsubscribe_from_patterns(client, patterns, timeout) {
            Ok(result) -> result
            Error(_) -> False
          }
        }
      }

    _ -> False
  }
}

fn unsubscribe(client: client.Client, channels: List(String), timeout: Int) {
  command.unsubscribe(channels)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    list.all(value, fn(item) {
      let assert protocol.Push([
        protocol.BulkString("unsubscribe"),
        protocol.BulkString(_),
        protocol.Integer(n),
      ]) = item
      n > 0
    })
  })
}

fn unsubscribe_from_all(client: client.Client, timeout: Int) {
  command.unsubscribe_from_all()
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    list.all(value, fn(item) {
      let assert protocol.Push([
        protocol.BulkString("unsubscribe"),
        protocol.BulkString(_),
        protocol.Integer(n),
      ]) = item
      n > 0
    })
  })
}

fn unsubscribe_from_patterns(client, patterns: List(String), timeout: Int) {
  command.unsubscribe_from_patterns(patterns)
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    list.all(value, fn(item) {
      let assert protocol.Push([
        protocol.BulkString("punsubscribe"),
        protocol.BulkString(_),
        protocol.Integer(n),
      ]) = item
      n > 0
    })
  })
}

fn unsubscribe_from_all_patterns(client, timeout: Int) {
  command.unsubscribe_from_all_patterns()
  |> client.execute(client, _, timeout)
  |> result.map(fn(value) {
    list.all(value, fn(item) {
      let assert protocol.Push([
        protocol.BulkString("punsubscribe"),
        protocol.BulkString(_),
        protocol.Integer(n),
      ]) = item
      n > 0
    })
  })
}

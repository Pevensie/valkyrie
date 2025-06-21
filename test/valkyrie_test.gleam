import envoy
import gleam/bool
import gleam/dict
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option
import gleam/otp/static_supervisor
import gleam/set
import gleam/string
import gleeunit
import gleeunit/should
import valkyrie
import valkyrie/internal/protocol

pub fn main() {
  gleeunit.main()
}

fn get_test_conn(next: fn(valkyrie.Connection) -> a) -> a {
  let assert Ok(conn) =
    valkyrie.default_config()
    |> valkyrie.start_pool(3, 1000)

  let res = next(conn)
  let assert Ok(_) = valkyrie.custom(conn, ["FLUSHDB"], 1000)
  let assert Ok(_) = valkyrie.shutdown(conn)
  res
}

fn get_supervised_conn(next: fn(valkyrie.Connection) -> a) -> a {
  let connection_subject = process.new_subject()
  let child_spec =
    valkyrie.default_config()
    |> valkyrie.supervised_pool(connection_subject, 3, 1000)

  let assert Ok(_started) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(child_spec)
    |> static_supervisor.start

  let assert Ok(conn) = process.receive(connection_subject, 1000)

  let res = next(conn)
  let assert Ok(_) = valkyrie.custom(conn, ["FLUSHDB"], 1000)
  // Shutdown is handled by the supervision tree shutdown
  res
}

fn is_keydb() -> Bool {
  envoy.get("DATABASE") == Ok("keydb")
}

// ------------------------------- //
// ----- Lifecycle functions ----- //
// ------------------------------- //

pub fn create_connection_test() {
  let assert Ok(conn) =
    valkyrie.default_config()
    |> valkyrie.create_connection(1000)

  let assert Ok("PONG") = valkyrie.ping(conn, 1000)
  let assert Ok(_) = valkyrie.shutdown(conn)
}

pub fn pool_connection_test() {
  use client <- get_test_conn()
  let assert Ok("PONG") = valkyrie.ping(client, 1000)
}

pub fn supervised_pool_connection_test() {
  use client <- get_supervised_conn()
  let assert Ok("PONG") = valkyrie.ping(client, 1000)
}

// ----------------------------- //
// ----- URL configuration ----- //
// ----------------------------- //

pub fn url_config_basic_test() {
  let assert Ok(config) = valkyrie.url_config("redis://localhost:6379")
  config.host |> should.equal("localhost")
  config.port |> should.equal(6379)
  config.auth |> should.equal(valkyrie.NoAuth)
}

pub fn url_config_supported_schemes_test() {
  let assert Ok(config1) = valkyrie.url_config("redis://localhost:6379")
  config1.host |> should.equal("localhost")

  let assert Ok(config2) = valkyrie.url_config("valkey://localhost:6379")
  config2.host |> should.equal("localhost")

  let assert Ok(config3) = valkyrie.url_config("keydb://localhost:6379")
  config3.host |> should.equal("localhost")
}

pub fn url_config_with_ports_test() {
  // Default port
  let assert Ok(config1) = valkyrie.url_config("redis://localhost")
  config1.port |> should.equal(6379)

  // Custom port
  let assert Ok(config2) = valkyrie.url_config("redis://localhost:6380")
  config2.port |> should.equal(6380)
}

pub fn url_config_with_auth_test() {
  // Password only
  let assert Ok(config1) =
    valkyrie.url_config("redis://:mypassword@localhost:6379")
  config1.auth |> should.equal(valkyrie.PasswordOnly("mypassword"))

  // Username and password
  let assert Ok(config2) =
    valkyrie.url_config("redis://user:pass@localhost:6379")
  config2.auth |> should.equal(valkyrie.UsernameAndPassword("user", "pass"))
}

pub fn url_config_error_cases_test() {
  let assert Error(valkyrie.UnsupportedScheme) =
    valkyrie.url_config("http://localhost:6379")

  let assert Error(valkyrie.MissingHost) = valkyrie.url_config("redis://:6379")

  let assert Error(valkyrie.InvalidUriFormat) =
    valkyrie.url_config("not a valid uri")

  let assert Error(valkyrie.MissingScheme) =
    valkyrie.url_config("//localhost:6379")
}

pub fn url_config_integration_test() {
  let assert Ok(config) = valkyrie.url_config("redis://localhost:6379")
  let assert Ok(conn) = config |> valkyrie.create_connection(1000)
  let assert Ok("PONG") = valkyrie.ping(conn, 1000)
  let assert Ok(_) = valkyrie.shutdown(conn)
}

// ---------------------------------- //
// ----- Escape hatch functions ----- //
// ---------------------------------- //

pub fn custom_command_test() {
  use conn <- get_test_conn()

  let assert Ok([protocol.BulkString("Hello World")]) =
    conn |> valkyrie.custom(["ECHO", "Hello World"], 1000)

  let assert Ok([protocol.BulkString("Custom ping")]) =
    conn |> valkyrie.custom(["PING", "Custom ping"], 1000)
}

// ---------------------------- //
// ----- Scalar functions ----- //
// ---------------------------- //

pub fn set_get_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    conn |> valkyrie.set("test:key", "test value", option.None, 1000)

  let assert Ok("test value") = conn |> valkyrie.get("test:key", 1000)
}

pub fn get_nonexistent_key_test() {
  use conn <- get_test_conn()

  let assert Error(valkyrie.NotFound) =
    conn |> valkyrie.get("nonexistent:key", 1000)
}

pub fn set_with_options_test() {
  use conn <- get_test_conn()

  let options =
    valkyrie.SetOptions(
      existence_condition: option.Some(valkyrie.IfNotExists),
      return_old: False,
      expiry_option: option.None,
    )

  let assert Ok("OK") =
    conn |> valkyrie.set("test:nx:key", "value1", option.Some(options), 1000)

  let assert Error(valkyrie.NotFound) =
    conn |> valkyrie.set("test:nx:key", "value2", option.Some(options), 1000)
}

pub fn mset_mget_test() {
  use conn <- get_test_conn()

  let pairs = [
    #("test:mset:1", "value1"),
    #("test:mset:2", "value2"),
    #("test:mset:3", "value3"),
  ]

  let assert Ok("OK") = conn |> valkyrie.mset(pairs, 1000)

  let keys = ["test:mset:1", "test:mset:2", "test:mset:3", "test:mset:4"]
  let assert Ok(values) = conn |> valkyrie.mget(keys, 1000)

  values
  |> should.equal([
    Ok("value1"),
    Ok("value2"),
    Ok("value3"),
    Error(valkyrie.NotFound),
  ])
}

pub fn append_test() {
  use conn <- get_test_conn()

  let assert Ok(5) = conn |> valkyrie.append("test:append", "Hello", 1000)
  let assert Ok(11) = conn |> valkyrie.append("test:append", " World", 1000)
  let assert Ok("Hello World") = conn |> valkyrie.get("test:append", 1000)
}

pub fn exists_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    conn |> valkyrie.set("test:exists:1", "value", option.None, 1000)
  let assert Ok("OK") =
    conn |> valkyrie.set("test:exists:2", "value", option.None, 1000)

  let assert Ok(2) =
    conn |> valkyrie.exists(["test:exists:1", "test:exists:2"], 1000)
  let assert Ok(1) =
    conn |> valkyrie.exists(["test:exists:1", "nonexistent"], 1000)
  let assert Ok(0) =
    conn |> valkyrie.exists(["nonexistent1", "nonexistent2"], 1000)
}

pub fn del_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    conn |> valkyrie.set("test:del:1", "value", option.None, 1000)
  let assert Ok("OK") =
    conn |> valkyrie.set("test:del:2", "value", option.None, 1000)

  let assert Ok(2) = conn |> valkyrie.del(["test:del:1", "test:del:2"], 1000)
  let assert Ok(0) = conn |> valkyrie.del(["nonexistent"], 1000)
}

pub fn keys_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    conn |> valkyrie.set("test:keys:foo", "1", option.None, 1000)
  let assert Ok("OK") =
    conn |> valkyrie.set("test:keys:bar", "2", option.None, 1000)
  let assert Ok("OK") =
    conn |> valkyrie.set("test:keys:baz", "3", option.None, 1000)

  let assert Ok(keys) = conn |> valkyrie.keys("test:keys:*", 1000)
  keys |> list.length |> should.equal(3)
  keys |> list.contains("test:keys:foo") |> should.be_true
  keys |> list.contains("test:keys:bar") |> should.be_true
  keys |> list.contains("test:keys:baz") |> should.be_true
}

pub fn scan_test() {
  use conn <- get_test_conn()

  let test_keys =
    list.range(1, 20)
    |> list.map(fn(i) { "test:scan:" <> int.to_string(i) })

  list.each(test_keys, fn(key) {
    let assert Ok("OK") = conn |> valkyrie.set(key, "value", option.None, 1000)
  })

  let assert Ok(#(keys, cursor)) = conn |> valkyrie.scan(0, 10, 1000)
  keys |> list.length |> should.not_equal(0)

  case cursor {
    0 -> Nil
    _ -> {
      let assert Ok(_) = conn |> valkyrie.scan(cursor, 10, 1000)
      Nil
    }
  }
}

pub fn scan_pattern_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    conn |> valkyrie.set("test:scanpat:1", "1", option.None, 1000)
  let assert Ok("OK") =
    conn |> valkyrie.set("test:scanpat:2", "2", option.None, 1000)
  let assert Ok("OK") =
    conn |> valkyrie.set("other:key", "3", option.None, 1000)

  let assert Ok(#(keys, _)) =
    conn |> valkyrie.scan_pattern(0, "test:scanpat:*", 10, 1000)

  keys
  |> list.all(fn(key) { string.starts_with(key, "test:scanpat:") })
  |> should.be_true
}

pub fn key_type_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    conn |> valkyrie.set("test:type:string", "value", option.None, 1000)
  let assert Ok(valkyrie.String) =
    conn |> valkyrie.key_type("test:type:string", 1000)

  let assert Ok(_) = conn |> valkyrie.lpush("test:type:list", ["item"], 1000)
  let assert Ok(valkyrie.List) =
    conn |> valkyrie.key_type("test:type:list", 1000)

  let assert Error(valkyrie.NotFound) =
    conn |> valkyrie.key_type("test:type:nonexistent", 1000)
}

pub fn rename_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    conn |> valkyrie.set("test:rename:old", "value", option.None, 1000)
  let assert Ok("OK") =
    conn |> valkyrie.rename("test:rename:old", "test:rename:new", 1000)

  let assert Error(valkyrie.NotFound) =
    conn |> valkyrie.get("test:rename:old", 1000)
  let assert Ok("value") = conn |> valkyrie.get("test:rename:new", 1000)
}

pub fn renamenx_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    conn |> valkyrie.set("test:renamenx:old", "value1", option.None, 1000)
  let assert Ok("OK") =
    conn |> valkyrie.set("test:renamenx:existing", "value2", option.None, 1000)

  // Should succeed - target doesn't exist
  let assert Ok(1) =
    conn |> valkyrie.renamenx("test:renamenx:old", "test:renamenx:new", 1000)

  let assert Ok("OK") =
    conn |> valkyrie.set("test:renamenx:another", "value3", option.None, 1000)

  // Should fail - target exists
  let assert Ok(0) =
    conn
    |> valkyrie.renamenx(
      "test:renamenx:another",
      "test:renamenx:existing",
      1000,
    )
}

pub fn random_key_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    conn |> valkyrie.set("test:random:1", "1", option.None, 1000)
  let assert Ok("OK") =
    conn |> valkyrie.set("test:random:2", "2", option.None, 1000)

  let assert Ok(key) = conn |> valkyrie.random_key(1000)
  key |> string.length |> should.not_equal(0)
}

// Numeric Operations
pub fn incr_test() {
  use conn <- get_test_conn()

  let assert Ok(1) = conn |> valkyrie.incr("test:incr", 1000)
  let assert Ok(2) = conn |> valkyrie.incr("test:incr", 1000)
  let assert Ok(3) = conn |> valkyrie.incr("test:incr", 1000)
}

pub fn incrby_test() {
  use conn <- get_test_conn()

  let assert Ok(5) = conn |> valkyrie.incrby("test:incrby", 5, 1000)
  let assert Ok(15) = conn |> valkyrie.incrby("test:incrby", 10, 1000)
  let assert Ok(12) = conn |> valkyrie.incrby("test:incrby", -3, 1000)
}

pub fn incrbyfloat_test() {
  use conn <- get_test_conn()

  let assert Ok(result1) =
    conn |> valkyrie.incrbyfloat("test:incrbyfloat", 2.5, 1000)
  result1 |> should.equal(2.5)

  let assert Ok(result2) =
    conn |> valkyrie.incrbyfloat("test:incrbyfloat", 1.5, 1000)
  result2 |> should.equal(4.0)
}

pub fn decr_test() {
  use conn <- get_test_conn()

  let assert Ok(-1) = conn |> valkyrie.decr("test:decr", 1000)
  let assert Ok(-2) = conn |> valkyrie.decr("test:decr", 1000)
}

pub fn decrby_test() {
  use conn <- get_test_conn()

  let assert Ok(-5) = conn |> valkyrie.decrby("test:decrby", 5, 1000)
  let assert Ok(-15) = conn |> valkyrie.decrby("test:decrby", 10, 1000)
  let assert Ok(-12) = conn |> valkyrie.decrby("test:decrby", -3, 1000)
}

pub fn ping_test() {
  use conn <- get_test_conn()

  let assert Ok("PONG") = conn |> valkyrie.ping(1000)
}

pub fn ping_message_test() {
  use conn <- get_test_conn()

  let assert Ok("Hello Redis") =
    conn |> valkyrie.ping_message("Hello Redis", 1000)
}

pub fn expire_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    conn |> valkyrie.set("test:expire", "value", option.None, 1000)
  let assert Ok(1) =
    conn |> valkyrie.expire("test:expire", 10, option.None, 1000)
  let assert Ok("value") = conn |> valkyrie.get("test:expire", 1000)

  let assert Ok(0) =
    conn |> valkyrie.expire("test:expire:nonexistent", 10, option.None, 1000)
}

pub fn persist_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    conn |> valkyrie.set("test:persist", "value", option.None, 1000)
  let assert Ok(1) =
    conn |> valkyrie.expire("test:persist", 10, option.None, 1000)
  let assert Ok(1) = conn |> valkyrie.persist("test:persist", 1000)

  let assert Ok("OK") =
    conn |> valkyrie.set("test:persist:no_ttl", "value", option.None, 1000)
  let assert Ok(0) = conn |> valkyrie.persist("test:persist:no_ttl", 1000)
}

// -------------------------- //
// ----- List functions ----- //
// -------------------------- //

pub fn lpush_rpush_test() {
  use conn <- get_test_conn()

  let assert Ok(2) =
    conn |> valkyrie.lpush("test:list", ["first", "second"], 1000)
  let assert Ok(4) =
    conn |> valkyrie.rpush("test:list", ["third", "fourth"], 1000)

  let assert Ok(items) = conn |> valkyrie.lrange("test:list", 0, -1, 1000)
  items |> should.equal(["second", "first", "third", "fourth"])
}

pub fn lpushx_rpushx_test() {
  use conn <- get_test_conn()

  // Should fail on non-existent list
  let assert Ok(0) = conn |> valkyrie.lpushx("test:listx", ["value"], 1000)
  let assert Ok(0) = conn |> valkyrie.rpushx("test:listx", ["value"], 1000)

  // Create list first
  let assert Ok(_) = conn |> valkyrie.lpush("test:listx", ["initial"], 1000)

  // Now should work
  let assert Ok(2) = conn |> valkyrie.lpushx("test:listx", ["left"], 1000)
  let assert Ok(3) = conn |> valkyrie.rpushx("test:listx", ["right"], 1000)

  let assert Ok(items) = conn |> valkyrie.lrange("test:listx", 0, -1, 1000)
  items |> should.equal(["left", "initial", "right"])
}

pub fn llen_test() {
  use conn <- get_test_conn()

  let assert Ok(0) = conn |> valkyrie.llen("test:llen", 1000)

  let assert Ok(_) = conn |> valkyrie.rpush("test:llen", ["a", "b", "c"], 1000)
  let assert Ok(3) = conn |> valkyrie.llen("test:llen", 1000)
}

pub fn lrange_test() {
  use conn <- get_test_conn()

  let assert Ok(_) =
    conn |> valkyrie.rpush("test:lrange", ["a", "b", "c", "d", "e"], 1000)

  let assert Ok(all) = conn |> valkyrie.lrange("test:lrange", 0, -1, 1000)
  all |> should.equal(["a", "b", "c", "d", "e"])

  let assert Ok(subset) = conn |> valkyrie.lrange("test:lrange", 1, 3, 1000)
  subset |> should.equal(["b", "c", "d"])
}

pub fn lpop_rpop_test() {
  use conn <- get_test_conn()

  let assert Ok(_) = conn |> valkyrie.rpush("test:pop", ["a", "b", "c"], 1000)

  let assert Ok("a") = conn |> valkyrie.lpop("test:pop", 1, 1000)
  let assert Ok("c") = conn |> valkyrie.rpop("test:pop", 1, 1000)
  let assert Ok(1) = conn |> valkyrie.llen("test:pop", 1000)
}

pub fn lindex_test() {
  use conn <- get_test_conn()

  let assert Ok(_) =
    conn |> valkyrie.rpush("test:lindex", ["a", "b", "c"], 1000)

  let assert Ok("a") = conn |> valkyrie.lindex("test:lindex", 0, 1000)
  let assert Ok("b") = conn |> valkyrie.lindex("test:lindex", 1, 1000)
  let assert Ok("c") = conn |> valkyrie.lindex("test:lindex", -1, 1000)
  let assert Error(valkyrie.NotFound) =
    conn |> valkyrie.lindex("test:lindex", 10, 1000)
}

pub fn lrem_test() {
  use conn <- get_test_conn()

  let assert Ok(_) =
    conn |> valkyrie.rpush("test:lrem", ["a", "b", "a", "c", "a"], 1000)

  let assert Ok(2) = conn |> valkyrie.lrem("test:lrem", 2, "a", 1000)

  let assert Ok(items) = conn |> valkyrie.lrange("test:lrem", 0, -1, 1000)
  items |> should.equal(["b", "c", "a"])
}

pub fn lset_test() {
  use conn <- get_test_conn()

  let assert Ok(_) = conn |> valkyrie.rpush("test:lset", ["a", "b", "c"], 1000)
  let assert Ok("OK") = conn |> valkyrie.lset("test:lset", 1, "B", 1000)

  let assert Ok(items) = conn |> valkyrie.lrange("test:lset", 0, -1, 1000)
  items |> should.equal(["a", "B", "c"])
}

pub fn linsert_test() {
  use conn <- get_test_conn()

  let assert Ok(_) = conn |> valkyrie.rpush("test:linsert", ["a", "c"], 1000)
  let assert Ok(3) =
    conn |> valkyrie.linsert("test:linsert", valkyrie.Before, "c", "b", 1000)

  let assert Ok(items) = conn |> valkyrie.lrange("test:linsert", 0, -1, 1000)
  items |> should.equal(["a", "b", "c"])
}

// ------------------------- //
// ----- Set functions ----- //
// ------------------------- //

pub fn sadd_scard_test() {
  use conn <- get_test_conn()

  let assert Ok(3) =
    conn |> valkyrie.sadd("test:set", ["apple", "banana", "cherry"], 1000)
  let assert Ok(3) = conn |> valkyrie.scard("test:set", 1000)

  let assert Ok(1) = conn |> valkyrie.sadd("test:set", ["apple", "date"], 1000)
  let assert Ok(4) = conn |> valkyrie.scard("test:set", 1000)
}

pub fn sismember_test() {
  use conn <- get_test_conn()

  let assert Ok(_) =
    conn |> valkyrie.sadd("test:set:members", ["red", "green", "blue"], 1000)

  let assert Ok(True) =
    conn |> valkyrie.sismember("test:set:members", "red", 1000)
  let assert Ok(True) =
    conn |> valkyrie.sismember("test:set:members", "green", 1000)
  let assert Ok(False) =
    conn |> valkyrie.sismember("test:set:members", "yellow", 1000)
}

pub fn smembers_test() {
  use conn <- get_test_conn()

  let assert Ok(_) =
    conn |> valkyrie.sadd("test:set:all", ["x", "y", "z"], 1000)

  let assert Ok(members) = conn |> valkyrie.smembers("test:set:all", 1000)
  members |> set.contains("x") |> should.be_true
  members |> set.contains("y") |> should.be_true
  members |> set.contains("z") |> should.be_true
  members |> set.size |> should.equal(3)
}

pub fn sscan_test() {
  use conn <- get_test_conn()

  let members = list.range(1, 20) |> list.map(int.to_string)
  let assert Ok(_) = conn |> valkyrie.sadd("test:set:scan", members, 1000)

  let assert Ok(#(scanned_members, cursor)) =
    conn |> valkyrie.sscan("test:set:scan", 0, 10, 1000)
  scanned_members |> list.length |> should.not_equal(0)

  case cursor {
    0 -> Nil
    _ -> {
      let assert Ok(_) =
        conn |> valkyrie.sscan("test:set:scan", cursor, 10, 1000)
      Nil
    }
  }
}

pub fn sscan_pattern_test() {
  use conn <- get_test_conn()

  let assert Ok(_) =
    conn
    |> valkyrie.sadd(
      "test:set:pattern",
      ["user:1", "user:2", "admin:1", "guest:1"],
      1000,
    )

  let assert Ok(#(user_members, _)) =
    conn |> valkyrie.sscan_pattern("test:set:pattern", 0, "user:*", 10, 1000)

  user_members
  |> list.all(fn(member) { string.starts_with(member, "user:") })
  |> should.be_true
}

// -------------------------------- //
// ----- Sorted set functions ----- //
// -------------------------------- //

pub fn zadd_zcard_test() {
  use conn <- get_test_conn()

  let members = [
    #("member1", valkyrie.Double(1.0)),
    #("member2", valkyrie.Double(2.5)),
    #("member3", valkyrie.Double(3.0)),
  ]

  let assert Ok(3) =
    valkyrie.zadd(
      conn,
      "test:zset:basic",
      members,
      valkyrie.IfNotExistsInSet,
      False,
      1000,
    )
  let assert Ok(3) = valkyrie.zcard(conn, "test:zset:basic", 1000)
}

pub fn zscore_zincrby_test() {
  use conn <- get_test_conn()

  let assert Ok(1) =
    valkyrie.zadd(
      conn,
      "test:zset:score",
      [#("member1", valkyrie.Double(10.0))],
      valkyrie.IfNotExistsInSet,
      False,
      1000,
    )

  let assert Ok(10.0) =
    valkyrie.zscore(conn, "test:zset:score", "member1", 1000)
  let assert Error(valkyrie.NotFound) =
    valkyrie.zscore(conn, "test:zset:score", "nonexistent", 1000)

  let assert Ok(15.5) =
    valkyrie.zincrby(
      conn,
      "test:zset:score",
      "member1",
      valkyrie.Double(5.5),
      1000,
    )
  let assert Ok(12.5) =
    valkyrie.zincrby(
      conn,
      "test:zset:score",
      "member1",
      valkyrie.Double(-3.0),
      1000,
    )
}

pub fn zcount_test() {
  use conn <- get_test_conn()

  let members = [
    #("member1", valkyrie.Double(1.0)),
    #("member2", valkyrie.Double(2.0)),
    #("member3", valkyrie.Double(3.0)),
    #("member4", valkyrie.Double(4.0)),
    #("member5", valkyrie.Double(5.0)),
  ]

  let assert Ok(_) =
    valkyrie.zadd(
      conn,
      "test:zset:count",
      members,
      valkyrie.IfNotExistsInSet,
      False,
      1000,
    )

  let assert Ok(3) =
    valkyrie.zcount(
      conn,
      "test:zset:count",
      valkyrie.Double(2.0),
      valkyrie.Double(4.0),
      1000,
    )
  let assert Ok(5) =
    valkyrie.zcount(
      conn,
      "test:zset:count",
      valkyrie.NegativeInfinity,
      valkyrie.Infinity,
      1000,
    )
}

pub fn zrem_test() {
  use conn <- get_test_conn()

  let members = [
    #("member1", valkyrie.Double(1.0)),
    #("member2", valkyrie.Double(2.0)),
    #("member3", valkyrie.Double(3.0)),
  ]

  let assert Ok(_) =
    valkyrie.zadd(
      conn,
      "test:zset:rem",
      members,
      valkyrie.IfNotExistsInSet,
      False,
      1000,
    )
  let assert Ok(2) =
    valkyrie.zrem(conn, "test:zset:rem", ["member1", "member2"], 1000)
  let assert Ok(1) = valkyrie.zcard(conn, "test:zset:rem", 1000)
  let assert Ok(0) = valkyrie.zrem(conn, "test:zset:rem", ["nonexistent"], 1000)
}

pub fn zrank_test() {
  use conn <- get_test_conn()

  let members = [
    #("first", valkyrie.Double(1.0)),
    #("second", valkyrie.Double(2.0)),
    #("third", valkyrie.Double(3.0)),
  ]

  let assert Ok(_) =
    valkyrie.zadd(
      conn,
      "test:zset:rank",
      members,
      valkyrie.IfNotExistsInSet,
      False,
      1000,
    )

  let assert Ok(0) = valkyrie.zrank(conn, "test:zset:rank", "first", 1000)
  let assert Ok(1) = valkyrie.zrank(conn, "test:zset:rank", "second", 1000)
  let assert Ok(2) = valkyrie.zrank(conn, "test:zset:rank", "third", 1000)

  let assert Ok(2) = valkyrie.zrevrank(conn, "test:zset:rank", "first", 1000)
  let assert Ok(1) = valkyrie.zrevrank(conn, "test:zset:rank", "second", 1000)
  let assert Ok(0) = valkyrie.zrevrank(conn, "test:zset:rank", "third", 1000)

  let assert Error(valkyrie.NotFound) =
    valkyrie.zrank(conn, "test:zset:rank", "nonexistent", 1000)
}

pub fn zrank_withscore_test() {
  // KeyDB doesn't support WITHSCORE in zrank
  use <- bool.guard(when: is_keydb(), return: Ok(10))
  use conn <- get_test_conn()

  let members = [
    #("first", valkyrie.Double(1.0)),
    #("second", valkyrie.Double(2.0)),
    #("third", valkyrie.Double(3.0)),
  ]

  let assert Ok(_) =
    valkyrie.zadd(
      conn,
      "test:zset:rank",
      members,
      valkyrie.IfNotExistsInSet,
      False,
      1000,
    )

  let assert Ok(#(0, valkyrie.Double(1.0))) =
    valkyrie.zrank_withscore(conn, "test:zset:rank", "first", 1000)
  let assert Ok(#(1, valkyrie.Double(2.0))) =
    valkyrie.zrank_withscore(conn, "test:zset:rank", "second", 1000)
  let assert Ok(#(2, valkyrie.Double(3.0))) =
    valkyrie.zrank_withscore(conn, "test:zset:rank", "third", 1000)

  let assert Ok(#(2, valkyrie.Double(1.0))) =
    valkyrie.zrevrank_withscore(conn, "test:zset:rank", "first", 1000)
  let assert Ok(#(1, valkyrie.Double(2.0))) =
    valkyrie.zrevrank_withscore(conn, "test:zset:rank", "second", 1000)
  let assert Ok(#(0, valkyrie.Double(3.0))) =
    valkyrie.zrevrank_withscore(conn, "test:zset:rank", "third", 1000)

  let assert Error(valkyrie.NotFound) =
    valkyrie.zrank(conn, "test:zset:rank", "nonexistent", 1000)
}

pub fn zscan_test() {
  use conn <- get_test_conn()

  let members = [
    #("member1", valkyrie.Double(1.0)),
    #("member2", valkyrie.Double(2.0)),
    #("member3", valkyrie.Double(3.0)),
    #("member4", valkyrie.Double(4.0)),
  ]

  let assert Ok(_) =
    valkyrie.zadd(
      conn,
      "test:zset:scan",
      members,
      valkyrie.IfNotExistsInSet,
      False,
      1000,
    )
  let assert Ok(#(scan_result, _cursor)) =
    valkyrie.zscan(conn, "test:zset:scan", 0, 10, 1000)
  scan_result |> list.length |> should.equal(4)
}

pub fn zscan_pattern_test() {
  use conn <- get_test_conn()

  let members = [
    #("prefix_member1", valkyrie.Double(1.0)),
    #("prefix_member2", valkyrie.Double(2.0)),
    #("other_member", valkyrie.Double(3.0)),
    #("prefix_member3", valkyrie.Double(4.0)),
  ]

  let assert Ok(_) =
    valkyrie.zadd(
      conn,
      "test:zset:scanpattern",
      members,
      valkyrie.IfNotExistsInSet,
      False,
      1000,
    )
  let assert Ok(#(scan_result, _cursor)) =
    valkyrie.zscan_pattern(
      conn,
      "test:zset:scanpattern",
      0,
      "prefix_*",
      10,
      1000,
    )
  scan_result |> list.length |> should.equal(3)
}

// -------------------------- //
// ----- Hash functions ----- //
// -------------------------- //

pub fn hset_hget_test() {
  use conn <- get_test_conn()

  let hash_values =
    dict.new()
    |> dict.insert("field1", "value1")
    |> dict.insert("field2", "value2")
    |> dict.insert("field3", "value3")

  let assert Ok(_) = valkyrie.hset(conn, "test:hash:basic", hash_values, 1000)
  let assert Ok(3) = valkyrie.hlen(conn, "test:hash:basic", 1000)

  let assert Ok("value1") =
    valkyrie.hget(conn, "test:hash:basic", "field1", 1000)
  let assert Ok("value2") =
    valkyrie.hget(conn, "test:hash:basic", "field2", 1000)
  let assert Ok("value3") =
    valkyrie.hget(conn, "test:hash:basic", "field3", 1000)
  let assert Error(valkyrie.NotFound) =
    valkyrie.hget(conn, "test:hash:basic", "nonexistent", 1000)
}

pub fn hsetnx_test() {
  use conn <- get_test_conn()

  let assert Ok(True) =
    valkyrie.hsetnx(conn, "test:hash:nx", "field1", "value1", 1000)
  let assert Ok(False) =
    valkyrie.hsetnx(conn, "test:hash:nx", "field1", "newvalue", 1000)
  let assert Ok("value1") = valkyrie.hget(conn, "test:hash:nx", "field1", 1000)
}

pub fn hlen_hkeys_hvals_test() {
  use conn <- get_test_conn()

  let hash_values =
    dict.new()
    |> dict.insert("field1", "value1")
    |> dict.insert("field2", "value2")
    |> dict.insert("field3", "value3")

  let assert Ok(_) = valkyrie.hset(conn, "test:hash:info", hash_values, 1000)

  let assert Ok(3) = valkyrie.hlen(conn, "test:hash:info", 1000)

  let assert Ok(keys) = valkyrie.hkeys(conn, "test:hash:info", 1000)
  keys
  |> list.sort(string.compare)
  |> should.equal(["field1", "field2", "field3"])

  let assert Ok(values) = valkyrie.hvals(conn, "test:hash:info", 1000)
  values
  |> list.sort(string.compare)
  |> should.equal(["value1", "value2", "value3"])
}

pub fn hgetall_test() {
  use conn <- get_test_conn()

  let hash_values =
    dict.new()
    |> dict.insert("field1", "value1")
    |> dict.insert("field2", "value2")

  let assert Ok(_) = valkyrie.hset(conn, "test:hash:getall", hash_values, 1000)
  let assert Ok(result) = valkyrie.hgetall(conn, "test:hash:getall", 1000)
  result |> dict.size |> should.equal(2)
}

pub fn hmget_test() {
  use conn <- get_test_conn()

  let hash_values =
    dict.new()
    |> dict.insert("field1", "value1")
    |> dict.insert("field2", "value2")
    |> dict.insert("field3", "value3")

  let assert Ok(_) = valkyrie.hset(conn, "test:hash:mget", hash_values, 1000)

  let assert Ok(results) =
    valkyrie.hmget(
      conn,
      "test:hash:mget",
      ["field1", "nonexistent", "field3"],
      1000,
    )
  results |> list.length |> should.equal(3)
  let assert [Ok("value1"), Error(_), Ok("value3")] = results
}

pub fn hstrlen_test() {
  use conn <- get_test_conn()

  let hash_values =
    dict.new()
    |> dict.insert("short", "hi")
    |> dict.insert("long", "this is a longer string")

  let assert Ok(_) = valkyrie.hset(conn, "test:hash:strlen", hash_values, 1000)

  let assert Ok(2) = valkyrie.hstrlen(conn, "test:hash:strlen", "short", 1000)
  let assert Ok(23) = valkyrie.hstrlen(conn, "test:hash:strlen", "long", 1000)
  let assert Ok(0) =
    valkyrie.hstrlen(conn, "test:hash:strlen", "nonexistent", 1000)
}

pub fn hdel_hexists_test() {
  use conn <- get_test_conn()

  let hash_values =
    dict.new()
    |> dict.insert("field1", "value1")
    |> dict.insert("field2", "value2")
    |> dict.insert("field3", "value3")

  let assert Ok(_) = valkyrie.hset(conn, "test:hash:del", hash_values, 1000)

  let assert Ok(True) = valkyrie.hexists(conn, "test:hash:del", "field1", 1000)
  let assert Ok(True) = valkyrie.hexists(conn, "test:hash:del", "field2", 1000)
  let assert Ok(False) =
    valkyrie.hexists(conn, "test:hash:del", "nonexistent", 1000)

  let assert Ok(2) =
    valkyrie.hdel(conn, "test:hash:del", ["field1", "field2"], 1000)

  let assert Ok(False) = valkyrie.hexists(conn, "test:hash:del", "field1", 1000)
  let assert Ok(False) = valkyrie.hexists(conn, "test:hash:del", "field2", 1000)
  let assert Ok(True) = valkyrie.hexists(conn, "test:hash:del", "field3", 1000)
  let assert Ok(1) = valkyrie.hlen(conn, "test:hash:del", 1000)
}

pub fn hincrby_test() {
  use conn <- get_test_conn()

  let assert Ok(5) =
    valkyrie.hincrby(conn, "test:hash:incr", "counter", 5, 1000)
  let assert Ok(15) =
    valkyrie.hincrby(conn, "test:hash:incr", "counter", 10, 1000)
  let assert Ok(10) =
    valkyrie.hincrby(conn, "test:hash:incr", "counter", -5, 1000)
  let assert Ok("10") = valkyrie.hget(conn, "test:hash:incr", "counter", 1000)
}

pub fn hincrbyfloat_test() {
  use conn <- get_test_conn()

  let assert Ok(result1) =
    valkyrie.hincrbyfloat(conn, "test:hash:incrfloat", "counter", 3.14, 1000)
  result1 |> should.equal(3.14)

  let assert Ok(result2) =
    valkyrie.hincrbyfloat(conn, "test:hash:incrfloat", "counter", 2.86, 1000)
  result2 |> should.equal(6.0)

  let assert Ok(result3) =
    valkyrie.hincrbyfloat(conn, "test:hash:incrfloat", "counter", -1.5, 1000)
  result3 |> should.equal(4.5)
}

pub fn hscan_test() {
  use conn <- get_test_conn()

  let hash_values =
    dict.new()
    |> dict.insert("field1", "value1")
    |> dict.insert("field2", "value2")
    |> dict.insert("field3", "value3")
    |> dict.insert("field4", "value4")

  let assert Ok(_) = valkyrie.hset(conn, "test:hash:scan", hash_values, 1000)
  let assert Ok(4) = valkyrie.hlen(conn, "test:hash:scan", 1000)

  let assert Ok(#(items, _cursor)) =
    valkyrie.hscan(conn, "test:hash:scan", 0, 10, 1000)
  items |> list.length |> should.equal(8)
}

pub fn hscan_pattern_test() {
  use conn <- get_test_conn()

  let hash_values =
    dict.new()
    |> dict.insert("prefix_field1", "value1")
    |> dict.insert("prefix_field2", "value2")
    |> dict.insert("other_field", "value3")
    |> dict.insert("prefix_field3", "value4")

  let assert Ok(_) =
    valkyrie.hset(conn, "test:hash:scanpattern", hash_values, 1000)
  let assert Ok(4) = valkyrie.hlen(conn, "test:hash:scanpattern", 1000)

  let assert Ok(#(items, _cursor)) =
    valkyrie.hscan_pattern(
      conn,
      "test:hash:scanpattern",
      0,
      "prefix_*",
      10,
      1000,
    )
  items |> list.length |> should.equal(6)
}

// -------------------------------- //
// ----- Edge cases and tests ----- //
// -------------------------------- //

pub fn unicode_test() {
  use conn <- get_test_conn()

  let unicode_value = "Hello 世界 🌍"
  let assert Ok("OK") =
    conn |> valkyrie.set("test:unicode", unicode_value, option.None, 1000)
  let assert Ok(retrieved) = conn |> valkyrie.get("test:unicode", 1000)
  retrieved |> should.equal(unicode_value)
}

pub fn binary_data_test() {
  use conn <- get_test_conn()

  let binary_value = "hello\u{0000}world\u{0001}\u{0002}"
  let assert Ok("OK") =
    valkyrie.set(conn, "test:binary", binary_value, option.None, 1000)
  let assert Ok(retrieved) = valkyrie.get(conn, "test:binary", 1000)
  retrieved |> should.equal(binary_value)
}

pub fn empty_operations_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") = valkyrie.set(conn, "test:empty", "", option.None, 1000)
  let assert Ok("") = valkyrie.get(conn, "test:empty", 1000)

  let assert Error(_) = valkyrie.get(conn, "", 1000)

  let assert Ok(5) = valkyrie.append(conn, "test:empty", "hello", 1000)
  let assert Ok("hello") = valkyrie.get(conn, "test:empty", 1000)
}

pub fn integer_boundary_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    valkyrie.set(conn, "test:bigint", "2147483647", option.None, 1000)
  let assert Ok(2_147_483_648) = valkyrie.incr(conn, "test:bigint", 1000)

  let assert Ok("OK") =
    valkyrie.set(conn, "test:negint", "1", option.None, 1000)
  let assert Ok(0) = valkyrie.decr(conn, "test:negint", 1000)
  let assert Ok(-1) = valkyrie.decr(conn, "test:negint", 1000)
  let assert Ok(-11) = valkyrie.decrby(conn, "test:negint", 10, 1000)
}

pub fn large_value_test() {
  use conn <- get_test_conn()

  let large_value = string.repeat("x", 100_000)
  let assert Ok("OK") =
    valkyrie.set(conn, "test:large", large_value, option.None, 1000)
  let assert Ok(retrieved) = valkyrie.get(conn, "test:large", 10_000)
  retrieved |> should.equal(large_value)

  let assert Ok(100_005) = valkyrie.append(conn, "test:large", "hello", 1000)
}

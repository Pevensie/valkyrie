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
import gleam/time/timestamp
import gleeunit
import gleeunit/should
import valkyrie
import valkyrie/resp

pub fn main() {
  gleeunit.main()
}

fn get_test_conn(next: fn(valkyrie.Connection) -> a) -> a {
  let assert Ok(conn) =
    valkyrie.default_config()
    |> valkyrie.start_pool(3, option.None, 1000)

  let res = next(conn)
  let assert Ok(_) = valkyrie.custom(conn, ["FLUSHDB"], 1000)
  let assert Ok(_) = valkyrie.shutdown(conn, 1000)
  res
}

fn get_supervised_conn(next: fn(valkyrie.Connection) -> a) -> a {
  let connection_name = process.new_name("valkyrie_test_pool")
  let child_spec =
    valkyrie.default_config()
    |> valkyrie.supervised_pool(3, option.Some(connection_name), 1000)

  let assert Ok(_started) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(child_spec)
    |> static_supervisor.start

  let conn = valkyrie.named_connection(connection_name)

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

  let assert Ok("PONG") = valkyrie.ping(conn, option.None, 1000)
  let assert Ok(_) = valkyrie.shutdown(conn, 1000)
}

pub fn pool_connection_test() {
  use client <- get_test_conn()
  let assert Ok("PONG") = valkyrie.ping(client, option.None, 1000)
}

pub fn supervised_pool_connection_test() {
  use client <- get_supervised_conn()
  let assert Ok("PONG") = valkyrie.ping(client, option.None, 1000)
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
  let assert Ok("PONG") = valkyrie.ping(conn, option.None, 1000)
  let assert Ok(_) = valkyrie.shutdown(conn, 1000)
}

// ---------------------------------- //
// ----- Escape hatch functions ----- //
// ---------------------------------- //

pub fn custom_command_test() {
  use conn <- get_test_conn()

  let assert Ok([resp.BulkString("Hello World")]) =
    valkyrie.custom(conn, ["ECHO", "Hello World"], 1000)

  let assert Ok([resp.BulkString("Custom ping")]) =
    valkyrie.custom(conn, ["PING", "Custom ping"], 1000)
}

// ---------------------------- //
// ----- Scalar functions ----- //
// ---------------------------- //

pub fn set_get_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    valkyrie.set(conn, "test:key", "test value", option.None, 1000)

  let assert Ok("test value") = valkyrie.get(conn, "test:key", 1000)
}

pub fn get_nonexistent_key_test() {
  use conn <- get_test_conn()

  let assert Error(valkyrie.NotFound) =
    valkyrie.get(conn, "nonexistent:key", 1000)
}

pub fn set_with_options_test() {
  use conn <- get_test_conn()

  // Test NX (if not exists) condition
  let nx_options =
    valkyrie.SetOptions(
      existence_condition: option.Some(valkyrie.IfNotExists),
      return_old: False,
      expiry_option: option.None,
    )
  let assert Ok("OK") =
    valkyrie.set(conn, "test:set:nx", "first", option.Some(nx_options), 1000)

  // Should fail because key exists
  let assert Error(_) =
    valkyrie.set(conn, "test:set:nx", "second", option.Some(nx_options), 1000)

  // Test XX (if exists) condition
  let xx_options =
    valkyrie.SetOptions(
      existence_condition: option.Some(valkyrie.IfExists),
      return_old: False,
      expiry_option: option.None,
    )
  let assert Ok("OK") =
    valkyrie.set(conn, "test:set:nx", "updated", option.Some(xx_options), 1000)

  // Should fail because key doesn't exist
  let assert Error(_) =
    valkyrie.set(
      conn,
      "test:set:nonexistent",
      "value",
      option.Some(xx_options),
      1000,
    )

  // Test return old value
  let return_old_options =
    valkyrie.SetOptions(
      existence_condition: option.None,
      return_old: True,
      expiry_option: option.None,
    )
  let assert Ok("updated") =
    valkyrie.set(
      conn,
      "test:set:nx",
      "newest",
      option.Some(return_old_options),
      1000,
    )

  // Test expiry options
  let expiry_seconds_options =
    valkyrie.SetOptions(
      existence_condition: option.None,
      return_old: False,
      expiry_option: option.Some(valkyrie.ExpirySeconds(60)),
    )
  let assert Ok("OK") =
    valkyrie.set(
      conn,
      "test:set:expire",
      "value",
      option.Some(expiry_seconds_options),
      1000,
    )

  let expiry_millis_options =
    valkyrie.SetOptions(
      existence_condition: option.None,
      return_old: False,
      expiry_option: option.Some(valkyrie.ExpiryMilliseconds(60_000)),
    )
  let assert Ok("OK") =
    valkyrie.set(
      conn,
      "test:set:expire_ms",
      "value",
      option.Some(expiry_millis_options),
      1000,
    )

  // Test KEEPTTL option
  let assert Ok(True) =
    valkyrie.expire(conn, "test:set:expire", 30, option.None, 1000)

  let keep_ttl_options =
    valkyrie.SetOptions(
      existence_condition: option.None,
      return_old: False,
      expiry_option: option.Some(valkyrie.KeepTtl),
    )
  let assert Ok("OK") =
    valkyrie.set(
      conn,
      "test:set:expire",
      "new_value",
      option.Some(keep_ttl_options),
      1000,
    )
}

pub fn mset_mget_test() {
  use conn <- get_test_conn()

  let pairs = [
    #("test:mset:1", "value1"),
    #("test:mset:2", "value2"),
    #("test:mset:3", "value3"),
  ]

  let assert Ok("OK") = valkyrie.mset(conn, pairs, 1000)

  let keys = ["test:mset:1", "test:mset:2", "test:mset:3", "test:mset:4"]
  let assert Ok(values) = valkyrie.mget(conn, keys, 1000)

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

  let assert Ok(5) = valkyrie.append(conn, "test:append", "Hello", 1000)
  let assert Ok(11) = valkyrie.append(conn, "test:append", " World", 1000)
  let assert Ok("Hello World") = valkyrie.get(conn, "test:append", 1000)
}

pub fn exists_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    valkyrie.set(conn, "test:exists:1", "value", option.None, 1000)
  let assert Ok("OK") =
    valkyrie.set(conn, "test:exists:2", "value", option.None, 1000)

  let assert Ok(2) =
    valkyrie.exists(conn, ["test:exists:1", "test:exists:2"], 1000)
  let assert Ok(1) =
    valkyrie.exists(conn, ["test:exists:1", "nonexistent"], 1000)
  let assert Ok(0) =
    valkyrie.exists(conn, ["nonexistent1", "nonexistent2"], 1000)
}

pub fn del_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    valkyrie.set(conn, "test:del:1", "value", option.None, 1000)
  let assert Ok("OK") =
    valkyrie.set(conn, "test:del:2", "value", option.None, 1000)

  let assert Ok(2) = valkyrie.del(conn, ["test:del:1", "test:del:2"], 1000)
  let assert Ok(0) = valkyrie.del(conn, ["nonexistent"], 1000)
}

pub fn keys_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    valkyrie.set(conn, "test:keys:foo", "1", option.None, 1000)
  let assert Ok("OK") =
    valkyrie.set(conn, "test:keys:bar", "2", option.None, 1000)
  let assert Ok("OK") =
    valkyrie.set(conn, "test:keys:baz", "3", option.None, 1000)

  let assert Ok(keys) = valkyrie.keys(conn, "test:keys:*", 1000)
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
    let assert Ok("OK") = valkyrie.set(conn, key, "value", option.None, 1000)
  })

  let assert Ok(#(keys, cursor)) =
    valkyrie.scan(conn, 0, option.None, 10, option.None, 1000)
  keys |> list.length |> should.not_equal(0)

  case cursor {
    0 -> Nil
    _ -> {
      let assert Ok(_) =
        valkyrie.scan(conn, cursor, option.None, 10, option.None, 1000)
      Nil
    }
  }
}

pub fn scan_pattern_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    valkyrie.set(conn, "test:scanpat:1", "1", option.None, 1000)
  let assert Ok("OK") =
    valkyrie.set(conn, "test:scanpat:2", "2", option.None, 1000)
  let assert Ok("OK") = valkyrie.set(conn, "other:key", "3", option.None, 1000)

  let assert Ok(#(keys, _)) =
    valkyrie.scan(conn, 0, option.Some("test:scanpat:*"), 10, option.None, 1000)

  keys
  |> list.all(fn(key) { string.starts_with(key, "test:scanpat:") })
  |> should.be_true
}

pub fn scan_key_type_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    valkyrie.set(conn, "test:scan:type:string", "value", option.None, 1000)
  let assert Ok(1) = valkyrie.lpush(conn, "test:scan:type:list", ["item"], 1000)

  let assert Ok(#(["test:scan:type:list"], _)) =
    valkyrie.scan(conn, 0, option.None, 10, option.Some(valkyrie.List), 1000)
}

pub fn scan_pattern_and_key_type_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    valkyrie.set(conn, "test:scan:pat1:string", "value", option.None, 1000)
  let assert Ok("OK") =
    valkyrie.set(conn, "test:scan:pat2:string", "value", option.None, 1000)
  let assert Ok(1) = valkyrie.lpush(conn, "test:scan:pat1:list", ["item"], 1000)

  let assert Ok(#(["test:scan:pat1:string"], _)) =
    valkyrie.scan(
      conn,
      0,
      option.Some("test:scan:pat1:*"),
      10,
      option.Some(valkyrie.String),
      1000,
    )
}

pub fn key_type_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    valkyrie.set(conn, "test:type:string", "value", option.None, 1000)
  let assert Ok(valkyrie.String) =
    valkyrie.key_type(conn, "test:type:string", 1000)

  let assert Ok(_) = valkyrie.lpush(conn, "test:type:list", ["item"], 1000)
  let assert Ok(valkyrie.List) = valkyrie.key_type(conn, "test:type:list", 1000)

  let assert Error(valkyrie.NotFound) =
    valkyrie.key_type(conn, "test:type:nonexistent", 1000)
}

pub fn rename_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    valkyrie.set(conn, "test:rename:old", "value", option.None, 1000)
  let assert Ok("OK") =
    valkyrie.rename(conn, "test:rename:old", "test:rename:new", 1000)

  let assert Error(valkyrie.NotFound) =
    valkyrie.get(conn, "test:rename:old", 1000)
  let assert Ok("value") = valkyrie.get(conn, "test:rename:new", 1000)
}

pub fn renamenx_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    valkyrie.set(conn, "test:renamenx:old", "value1", option.None, 1000)
  let assert Ok("OK") =
    valkyrie.set(conn, "test:renamenx:existing", "value2", option.None, 1000)

  // Should succeed - target doesn't exist
  let assert Ok(1) =
    valkyrie.renamenx(conn, "test:renamenx:old", "test:renamenx:new", 1000)

  let assert Ok("OK") =
    valkyrie.set(conn, "test:renamenx:another", "value3", option.None, 1000)

  // Should fail - target exists
  let assert Ok(0) =
    valkyrie.renamenx(
      conn,
      "test:renamenx:another",
      "test:renamenx:existing",
      1000,
    )
}

pub fn random_key_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    valkyrie.set(conn, "test:random:1", "1", option.None, 1000)
  let assert Ok("OK") =
    valkyrie.set(conn, "test:random:2", "2", option.None, 1000)

  let assert Ok(key) = valkyrie.randomkey(conn, 1000)
  key |> string.length |> should.not_equal(0)
}

// Numeric Operations
pub fn incr_test() {
  use conn <- get_test_conn()

  let assert Ok(1) = valkyrie.incr(conn, "test:incr", 1000)
  let assert Ok(2) = valkyrie.incr(conn, "test:incr", 1000)
  let assert Ok(3) = valkyrie.incr(conn, "test:incr", 1000)
}

pub fn incrby_test() {
  use conn <- get_test_conn()

  let assert Ok(5) = valkyrie.incrby(conn, "test:incrby", 5, 1000)
  let assert Ok(15) = valkyrie.incrby(conn, "test:incrby", 10, 1000)
  let assert Ok(12) = valkyrie.incrby(conn, "test:incrby", -3, 1000)
}

pub fn incrbyfloat_test() {
  use conn <- get_test_conn()

  let assert Ok(result1) =
    valkyrie.incrbyfloat(conn, "test:incrbyfloat", 2.5, 1000)
  result1 |> should.equal(2.5)

  let assert Ok(result2) =
    valkyrie.incrbyfloat(conn, "test:incrbyfloat", 1.5, 1000)
  result2 |> should.equal(4.0)
}

pub fn decr_test() {
  use conn <- get_test_conn()

  let assert Ok(-1) = valkyrie.decr(conn, "test:decr", 1000)
  let assert Ok(-2) = valkyrie.decr(conn, "test:decr", 1000)
}

pub fn decrby_test() {
  use conn <- get_test_conn()

  let assert Ok(-5) = valkyrie.decrby(conn, "test:decrby", 5, 1000)
  let assert Ok(-15) = valkyrie.decrby(conn, "test:decrby", 10, 1000)
  let assert Ok(-12) = valkyrie.decrby(conn, "test:decrby", -3, 1000)
}

pub fn ping_test() {
  use conn <- get_test_conn()

  let assert Ok("PONG") = valkyrie.ping(conn, option.None, 1000)
}

pub fn ping_message_test() {
  use conn <- get_test_conn()

  let assert Ok("Hello Redis") =
    valkyrie.ping(conn, option.Some("Hello Redis"), 1000)
}

pub fn expire_expiretime_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    valkyrie.set(conn, "test:expire", "value", option.None, 1000)

  bool.guard(when: is_keydb(), return: Nil, otherwise: fn() {
    let assert Ok(valkyrie.NoExpiration) =
      valkyrie.expiretime(conn, "test:expire", 1000)
    Nil
  })

  let assert Ok(True) =
    valkyrie.expire(conn, "test:expire", 10, option.None, 1000)

  bool.guard(when: is_keydb(), return: Nil, otherwise: fn() {
    let assert Ok(valkyrie.ExpiresAfter(expire_time)) =
      valkyrie.expiretime(conn, "test:expire", 1000)
    { expire_time > 0 } |> should.be_true
  })

  let assert Ok("value") = valkyrie.get(conn, "test:expire", 1000)

  let assert Ok(True) =
    valkyrie.expire(conn, "test:expire", -1, option.None, 1000)
  let assert Error(valkyrie.NotFound) = valkyrie.get(conn, "test:expire", 1000)

  let assert Ok(False) =
    valkyrie.expire(conn, "test:expire:nonexistent", 10, option.None, 1000)
}

pub fn expire_condition_variations_test() {
  use <- bool.guard(when: is_keydb(), return: Ok(False))
  use conn <- get_test_conn()

  // Set up test keys for IfNoExpiry condition
  let assert Ok("OK") =
    valkyrie.set(conn, "test:expire:no_ttl_1", "value", option.None, 1000)
  let assert Ok("OK") =
    valkyrie.set(conn, "test:expire:with_ttl_1", "value", option.None, 1000)
  let assert Ok(True) =
    valkyrie.expire(conn, "test:expire:with_ttl_1", 3600, option.None, 1000)

  // Test IfNoExpiry condition
  let assert Ok(True) =
    valkyrie.expire(
      conn,
      "test:expire:no_ttl_1",
      60,
      option.Some(valkyrie.IfNoExpiry),
      1000,
    )
  let assert Ok(False) =
    valkyrie.expire(
      conn,
      "test:expire:with_ttl_1",
      60,
      option.Some(valkyrie.IfNoExpiry),
      1000,
    )

  // Set up fresh test keys for IfHasExpiry condition
  let assert Ok("OK") =
    valkyrie.set(conn, "test:expire:no_ttl_2", "value", option.None, 1000)
  let assert Ok("OK") =
    valkyrie.set(conn, "test:expire:with_ttl_2", "value", option.None, 1000)
  let assert Ok(True) =
    valkyrie.expire(conn, "test:expire:with_ttl_2", 3600, option.None, 1000)

  // Test IfHasExpiry condition
  let assert Ok(False) =
    valkyrie.expire(
      conn,
      "test:expire:no_ttl_2",
      60,
      option.Some(valkyrie.IfHasExpiry),
      1000,
    )
  let assert Ok(True) =
    valkyrie.expire(
      conn,
      "test:expire:with_ttl_2",
      60,
      option.Some(valkyrie.IfHasExpiry),
      1000,
    )

  // Set up test key for comparison conditions
  let assert Ok("OK") =
    valkyrie.set(conn, "test:expire:with_ttl_3", "value", option.None, 1000)
  let assert Ok(True) =
    valkyrie.expire(conn, "test:expire:with_ttl_3", 3600, option.None, 1000)

  // Test IfGreaterThan condition (7200 > 3600, should succeed)
  let assert Ok(True) =
    valkyrie.expire(
      conn,
      "test:expire:with_ttl_3",
      7200,
      option.Some(valkyrie.IfGreaterThan),
      1000,
    )

  // Test IfLessThan condition (30 < 7200, should succeed)
  let assert Ok(True) =
    valkyrie.expire(
      conn,
      "test:expire:with_ttl_3",
      30,
      option.Some(valkyrie.IfLessThan),
      1000,
    )
}

pub fn pexpire_pexpiretime_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    valkyrie.set(conn, "test:pexpire", "value", option.None, 1000)

  bool.guard(when: is_keydb(), return: Nil, otherwise: fn() {
    let assert Ok(valkyrie.NoExpiration) =
      valkyrie.pexpiretime(conn, "test:pexpire", 1000)
    Nil
  })

  let assert Ok(True) =
    valkyrie.pexpire(conn, "test:pexpire", 10_000, option.None, 1000)

  bool.guard(when: is_keydb(), return: Nil, otherwise: fn() {
    let assert Ok(valkyrie.ExpiresAfter(expire_time)) =
      valkyrie.pexpiretime(conn, "test:pexpire", 1000)
    { expire_time > 0 } |> should.be_true
  })

  let assert Ok("value") = valkyrie.get(conn, "test:pexpire", 1000)

  let assert Ok(True) =
    valkyrie.pexpire(conn, "test:pexpire", -1, option.None, 1000)
  let assert Error(valkyrie.NotFound) = valkyrie.get(conn, "test:expire", 1000)

  let assert Ok(False) =
    valkyrie.pexpire(
      conn,
      "test:pexpire:nonexistent",
      10_000,
      option.None,
      1000,
    )
}

pub fn expireat_test() {
  use conn <- get_test_conn()

  let far_future_unix_seconds = 2_000_000_000

  let assert Ok("OK") =
    valkyrie.set(conn, "test:expire", "value", option.None, 1000)
  let assert Ok(True) =
    valkyrie.expireat(
      conn,
      "test:expire",
      timestamp.from_unix_seconds(far_future_unix_seconds),
      option.None,
      1000,
    )
  let assert Ok("value") = valkyrie.get(conn, "test:expire", 1000)

  // Correctly rounds a fractional number of seconds
  let assert Ok(True) =
    valkyrie.expireat(
      conn,
      "test:expire",
      // 500 milliseconds over above time
      timestamp.from_unix_seconds_and_nanoseconds(
        far_future_unix_seconds,
        500_000_000,
      ),
      option.None,
      1000,
    )

  // Setting a timestamp in the past deletes the key
  let assert Ok(True) =
    valkyrie.expireat(
      conn,
      "test:expire",
      timestamp.from_unix_seconds(0),
      option.None,
      1000,
    )

  let assert Error(valkyrie.NotFound) = valkyrie.get(conn, "test:expire", 1000)

  let assert Ok(False) =
    valkyrie.expire(conn, "test:expire:nonexistent", 10, option.None, 1000)
}

pub fn persist_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    valkyrie.set(conn, "test:persist", "value", option.None, 1000)
  let assert Ok(True) =
    valkyrie.expire(conn, "test:persist", 10, option.None, 1000)
  let assert Ok(True) = valkyrie.persist(conn, "test:persist", 1000)

  let assert Ok("OK") =
    valkyrie.set(conn, "test:persist:no_ttl", "value", option.None, 1000)
  let assert Ok(False) = valkyrie.persist(conn, "test:persist:no_ttl", 1000)
}

// -------------------------- //
// ----- List functions ----- //
// -------------------------- //

pub fn lpush_rpush_test() {
  use conn <- get_test_conn()

  let assert Ok(2) =
    valkyrie.lpush(conn, "test:list", ["first", "second"], 1000)
  let assert Ok(4) =
    valkyrie.rpush(conn, "test:list", ["third", "fourth"], 1000)

  let assert Ok(items) = valkyrie.lrange(conn, "test:list", 0, -1, 1000)
  items |> should.equal(["second", "first", "third", "fourth"])
}

pub fn lpushx_rpushx_test() {
  use conn <- get_test_conn()

  // Should fail on non-existent list
  let assert Ok(0) = valkyrie.lpushx(conn, "test:listx", ["value"], 1000)
  let assert Ok(0) = valkyrie.rpushx(conn, "test:listx", ["value"], 1000)

  // Create list first
  let assert Ok(_) = valkyrie.lpush(conn, "test:listx", ["initial"], 1000)

  // Now should work
  let assert Ok(2) = valkyrie.lpushx(conn, "test:listx", ["left"], 1000)
  let assert Ok(3) = valkyrie.rpushx(conn, "test:listx", ["right"], 1000)

  let assert Ok(items) = valkyrie.lrange(conn, "test:listx", 0, -1, 1000)
  items |> should.equal(["left", "initial", "right"])
}

pub fn llen_test() {
  use conn <- get_test_conn()

  let assert Ok(0) = valkyrie.llen(conn, "test:llen", 1000)

  let assert Ok(_) = valkyrie.rpush(conn, "test:llen", ["a", "b", "c"], 1000)
  let assert Ok(3) = valkyrie.llen(conn, "test:llen", 1000)
}

pub fn lrange_test() {
  use conn <- get_test_conn()

  let assert Ok(_) =
    valkyrie.rpush(conn, "test:lrange", ["a", "b", "c", "d", "e"], 1000)

  let assert Ok(all) = valkyrie.lrange(conn, "test:lrange", 0, -1, 1000)
  all |> should.equal(["a", "b", "c", "d", "e"])

  let assert Ok(subset) = valkyrie.lrange(conn, "test:lrange", 1, 3, 1000)
  subset |> should.equal(["b", "c", "d"])
}

pub fn lpop_rpop_test() {
  use conn <- get_test_conn()

  let assert Ok(_) = valkyrie.rpush(conn, "test:pop", ["a", "b", "c"], 1000)

  let assert Ok("a") = valkyrie.lpop(conn, "test:pop", 1, 1000)
  let assert Ok("c") = valkyrie.rpop(conn, "test:pop", 1, 1000)
  let assert Ok(1) = valkyrie.llen(conn, "test:pop", 1000)
}

pub fn lindex_test() {
  use conn <- get_test_conn()

  let assert Ok(_) = valkyrie.rpush(conn, "test:lindex", ["a", "b", "c"], 1000)

  let assert Ok("a") = valkyrie.lindex(conn, "test:lindex", 0, 1000)
  let assert Ok("b") = valkyrie.lindex(conn, "test:lindex", 1, 1000)
  let assert Ok("c") = valkyrie.lindex(conn, "test:lindex", -1, 1000)
  let assert Error(valkyrie.NotFound) =
    valkyrie.lindex(conn, "test:lindex", 10, 1000)
}

pub fn lrem_test() {
  use conn <- get_test_conn()

  let assert Ok(_) =
    valkyrie.rpush(conn, "test:lrem", ["a", "b", "a", "c", "a"], 1000)

  let assert Ok(2) = valkyrie.lrem(conn, "test:lrem", 2, "a", 1000)

  let assert Ok(items) = valkyrie.lrange(conn, "test:lrem", 0, -1, 1000)
  items |> should.equal(["b", "c", "a"])
}

pub fn lset_test() {
  use conn <- get_test_conn()

  let assert Ok(_) = valkyrie.rpush(conn, "test:lset", ["a", "b", "c"], 1000)
  let assert Ok("OK") = valkyrie.lset(conn, "test:lset", 1, "B", 1000)

  let assert Ok(items) = valkyrie.lrange(conn, "test:lset", 0, -1, 1000)
  items |> should.equal(["a", "B", "c"])
}

pub fn linsert_test() {
  use conn <- get_test_conn()

  let assert Ok(_) = valkyrie.rpush(conn, "test:linsert", ["a", "c"], 1000)
  let assert Ok(3) =
    valkyrie.linsert(conn, "test:linsert", valkyrie.Before, "c", "b", 1000)

  let assert Ok(items) = valkyrie.lrange(conn, "test:linsert", 0, -1, 1000)
  items |> should.equal(["a", "b", "c"])
}

// ------------------------- //
// ----- Set functions ----- //
// ------------------------- //

pub fn sadd_scard_test() {
  use conn <- get_test_conn()

  let assert Ok(3) =
    valkyrie.sadd(conn, "test:set", ["apple", "banana", "cherry"], 1000)
  let assert Ok(3) = valkyrie.scard(conn, "test:set", 1000)

  let assert Ok(1) = valkyrie.sadd(conn, "test:set", ["apple", "date"], 1000)
  let assert Ok(4) = valkyrie.scard(conn, "test:set", 1000)
}

pub fn sismember_test() {
  use conn <- get_test_conn()

  let assert Ok(_) =
    valkyrie.sadd(conn, "test:set:members", ["red", "green", "blue"], 1000)

  let assert Ok(True) =
    valkyrie.sismember(conn, "test:set:members", "red", 1000)
  let assert Ok(True) =
    valkyrie.sismember(conn, "test:set:members", "green", 1000)
  let assert Ok(False) =
    valkyrie.sismember(conn, "test:set:members", "yellow", 1000)
}

pub fn smembers_test() {
  use conn <- get_test_conn()

  let assert Ok(_) = valkyrie.sadd(conn, "test:set:all", ["x", "y", "z"], 1000)

  let assert Ok(members) = valkyrie.smembers(conn, "test:set:all", 1000)
  members |> set.contains("x") |> should.be_true
  members |> set.contains("y") |> should.be_true
  members |> set.contains("z") |> should.be_true
  members |> set.size |> should.equal(3)
}

pub fn sscan_test() {
  use conn <- get_test_conn()

  let members = list.range(1, 20) |> list.map(int.to_string)
  let assert Ok(_) = valkyrie.sadd(conn, "test:set:scan", members, 1000)

  let assert Ok(#(scanned_members, cursor)) =
    valkyrie.sscan(conn, "test:set:scan", 0, option.None, 10, 1000)
  scanned_members |> list.length |> should.not_equal(0)

  case cursor {
    0 -> Nil
    _ -> {
      let assert Ok(_) =
        valkyrie.sscan(conn, "test:set:scan", cursor, option.None, 10, 1000)
      Nil
    }
  }
}

pub fn sscan_pattern_test() {
  use conn <- get_test_conn()

  let assert Ok(_) =
    valkyrie.sadd(
      conn,
      "test:set:pattern",
      ["user:1", "user:2", "admin:1", "guest:1"],
      1000,
    )

  let assert Ok(#(user_members, _)) =
    valkyrie.sscan(conn, "test:set:pattern", 0, option.Some("user:*"), 10, 1000)

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

pub fn zrevrank_test() {
  use conn <- get_test_conn()

  let members = [
    #("lowest", valkyrie.Double(1.0)),
    #("middle", valkyrie.Double(5.0)),
    #("highest", valkyrie.Double(10.0)),
  ]

  let assert Ok(_) =
    valkyrie.zadd(
      conn,
      "test:zset:revrank",
      members,
      valkyrie.IfNotExistsInSet,
      False,
      1000,
    )

  // Test reverse ranks (highest score = rank 0)
  let assert Ok(0) =
    valkyrie.zrevrank(conn, "test:zset:revrank", "highest", 1000)
  let assert Ok(1) =
    valkyrie.zrevrank(conn, "test:zset:revrank", "middle", 1000)
  let assert Ok(2) =
    valkyrie.zrevrank(conn, "test:zset:revrank", "lowest", 1000)

  // Test non-existent member
  let assert Error(valkyrie.NotFound) =
    valkyrie.zrevrank(conn, "test:zset:revrank", "nonexistent", 1000)

  // Test non-existent key
  let assert Error(valkyrie.NotFound) =
    valkyrie.zrevrank(conn, "test:zset:nonexistent", "member", 1000)
}

pub fn zrevrank_withscore_test() {
  // KeyDB doesn't support WITHSCORE in zrevrank
  use <- bool.guard(when: is_keydb(), return: Ok(#(0, valkyrie.Double(0.0))))
  use conn <- get_test_conn()

  let members = [
    #("lowest", valkyrie.Double(1.0)),
    #("middle", valkyrie.Double(5.0)),
    #("highest", valkyrie.Double(10.0)),
  ]

  let assert Ok(_) =
    valkyrie.zadd(
      conn,
      "test:zset:revrank_score",
      members,
      valkyrie.IfNotExistsInSet,
      False,
      1000,
    )

  // Test reverse rank with score
  let assert Ok(#(0, valkyrie.Double(10.0))) =
    valkyrie.zrevrank_withscore(
      conn,
      "test:zset:revrank_score",
      "highest",
      1000,
    )
  let assert Ok(#(1, valkyrie.Double(5.0))) =
    valkyrie.zrevrank_withscore(conn, "test:zset:revrank_score", "middle", 1000)
  let assert Ok(#(2, valkyrie.Double(1.0))) =
    valkyrie.zrevrank_withscore(conn, "test:zset:revrank_score", "lowest", 1000)

  // Test non-existent member
  let assert Error(valkyrie.NotFound) =
    valkyrie.zrevrank_withscore(
      conn,
      "test:zset:revrank_score",
      "nonexistent",
      1000,
    )
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
    valkyrie.zscan(conn, "test:zset:scan", 0, option.None, 10, 1000)
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
    valkyrie.zscan(
      conn,
      "test:zset:scanpattern",
      0,
      option.Some("prefix_*"),
      10,
      1000,
    )
  scan_result |> list.length |> should.equal(3)
}

pub fn zrandmember_test() {
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
      "test:zset:random",
      members,
      valkyrie.IfNotExistsInSet,
      False,
      1000,
    )

  // Test getting 2 random members
  let assert Ok(random_members) =
    valkyrie.zrandmember(conn, "test:zset:random", 2, 1000)
  random_members |> list.length |> should.equal(2)

  // Test getting 1 random member
  let assert Ok(single_member) =
    valkyrie.zrandmember(conn, "test:zset:random", 1, 1000)
  single_member |> list.length |> should.equal(1)

  // Test empty set
  let assert Ok(empty_members) =
    valkyrie.zrandmember(conn, "test:zset:empty", 2, 1000)
  empty_members |> list.length |> should.equal(0)
}

pub fn zpopmin_zpopmax_test() {
  use conn <- get_test_conn()

  let members = [
    #("low", valkyrie.Double(1.0)),
    #("medium", valkyrie.Double(5.0)),
    #("high", valkyrie.Double(10.0)),
  ]

  let assert Ok(_) =
    valkyrie.zadd(
      conn,
      "test:zset:pop",
      members,
      valkyrie.IfNotExistsInSet,
      False,
      1000,
    )

  // Test zpopmin
  let assert Ok(min_members) = valkyrie.zpopmin(conn, "test:zset:pop", 1, 1000)
  min_members |> should.equal([#("low", valkyrie.Double(1.0))])

  // Test zpopmax
  let assert Ok(max_members) = valkyrie.zpopmax(conn, "test:zset:pop", 1, 1000)
  max_members |> should.equal([#("high", valkyrie.Double(10.0))])

  // Verify remaining member
  let assert Ok(1) = valkyrie.zcard(conn, "test:zset:pop", 1000)

  // Test popping multiple
  let assert Ok(_) =
    valkyrie.zadd(
      conn,
      "test:zset:multi",
      [
        #("a", valkyrie.Double(1.0)),
        #("b", valkyrie.Double(2.0)),
        #("c", valkyrie.Double(3.0)),
      ],
      valkyrie.IfNotExistsInSet,
      False,
      1000,
    )

  let assert Ok(multi_min) = valkyrie.zpopmin(conn, "test:zset:multi", 2, 1000)
  multi_min |> list.length |> should.equal(2)

  // Test empty set
  let assert Ok(empty_min) = valkyrie.zpopmin(conn, "test:zset:empty", 1, 1000)
  empty_min |> list.length |> should.equal(0)

  let assert Ok(empty_max) = valkyrie.zpopmax(conn, "test:zset:empty", 1, 1000)
  empty_max |> list.length |> should.equal(0)
}

pub fn zrange_test() {
  use conn <- get_test_conn()

  let members = [
    #("first", valkyrie.Double(1.0)),
    #("second", valkyrie.Double(2.0)),
    #("third", valkyrie.Double(3.0)),
    #("fourth", valkyrie.Double(4.0)),
    #("fifth", valkyrie.Double(5.0)),
  ]

  let assert Ok(_) =
    valkyrie.zadd(
      conn,
      "test:zset:range",
      members,
      valkyrie.IfNotExistsInSet,
      False,
      1000,
    )

  // Test basic range
  let assert Ok([
    #("first", valkyrie.Double(1.0)),
    #("second", valkyrie.Double(2.0)),
    #("third", valkyrie.Double(3.0)),
  ]) =
    valkyrie.zrange(
      conn,
      "test:zset:range",
      valkyrie.NumericInclusive(0),
      valkyrie.NumericInclusive(2),
      False,
      1000,
    )

  // Test reverse range
  let assert Ok([
    #("fifth", valkyrie.Double(5.0)),
    #("fourth", valkyrie.Double(4.0)),
    #("third", valkyrie.Double(3.0)),
  ]) =
    valkyrie.zrange(
      conn,
      "test:zset:range",
      valkyrie.NumericInclusive(0),
      valkyrie.NumericInclusive(2),
      True,
      1000,
    )

  // Test different inclusive bounds
  let assert Ok([
    #("second", valkyrie.Double(2.0)),
    #("third", valkyrie.Double(3.0)),
    #("fourth", valkyrie.Double(4.0)),
  ]) =
    valkyrie.zrange(
      conn,
      "test:zset:range",
      valkyrie.NumericInclusive(1),
      valkyrie.NumericInclusive(3),
      False,
      1000,
    )

  // Test empty range
  let assert Ok([]) =
    valkyrie.zrange(
      conn,
      "test:zset:empty",
      valkyrie.NumericInclusive(0),
      valkyrie.NumericInclusive(2),
      False,
      1000,
    )
}

pub fn zrange_byscore_test() {
  use conn <- get_test_conn()

  let members = [
    #("low1", valkyrie.Double(1.0)),
    #("low2", valkyrie.Double(1.5)),
    #("medium", valkyrie.Double(5.0)),
    #("high1", valkyrie.Double(10.0)),
    #("high2", valkyrie.Double(15.0)),
  ]

  let assert Ok(_) =
    valkyrie.zadd(
      conn,
      "test:zset:byscore",
      members,
      valkyrie.IfNotExistsInSet,
      False,
      1000,
    )

  // Test basic score range
  let assert Ok([
    #("low1", valkyrie.Double(1.0)),
    #("low2", valkyrie.Double(1.5)),
  ]) =
    valkyrie.zrange_byscore(
      conn,
      "test:zset:byscore",
      valkyrie.NumericInclusive(valkyrie.Double(1.0)),
      valkyrie.NumericExclusive(valkyrie.Double(5.0)),
      False,
      1000,
    )

  // Test reversed score range
  let assert Ok([
    #("high2", valkyrie.Double(15.0)),
    #("high1", valkyrie.Double(10.0)),
  ]) =
    valkyrie.zrange_byscore(
      conn,
      "test:zset:byscore",
      valkyrie.NumericInclusive(valkyrie.Double(15.0)),
      valkyrie.NumericInclusive(valkyrie.Double(10.0)),
      True,
      1000,
    )

  // Test empty range
  let assert Ok([]) =
    valkyrie.zrange_byscore(
      conn,
      "test:zset:byscore",
      valkyrie.NumericInclusive(valkyrie.Double(20.0)),
      valkyrie.NumericInclusive(valkyrie.Double(25.0)),
      False,
      1000,
    )
}

pub fn zrange_bylex_test() {
  use conn <- get_test_conn()

  // Create sorted set where all members have the same score
  let members = [
    #("apple", valkyrie.Double(0.0)),
    #("banana", valkyrie.Double(0.0)),
    #("cherry", valkyrie.Double(0.0)),
    #("date", valkyrie.Double(0.0)),
    #("elderberry", valkyrie.Double(0.0)),
  ]

  let assert Ok(_) =
    valkyrie.zadd(
      conn,
      "test:zset:bylex",
      members,
      valkyrie.IfNotExistsInSet,
      False,
      1000,
    )

  // Test basic lexicographic range
  let assert Ok(["banana", "cherry"]) =
    valkyrie.zrange_bylex(
      conn,
      "test:zset:bylex",
      valkyrie.LexInclusive("banana"),
      valkyrie.LexExclusive("date"),
      False,
      1000,
    )

  // Test reverse lexicographic range
  let assert Ok(["elderberry", "date", "cherry"]) =
    valkyrie.zrange_bylex(
      conn,
      "test:zset:bylex",
      valkyrie.LexInclusive("elderberry"),
      valkyrie.LexExclusive("banana"),
      True,
      1000,
    )

  // Test empty range
  let assert Ok([]) =
    valkyrie.zrange_bylex(
      conn,
      "test:zset:bylex",
      valkyrie.LexExclusive("fig"),
      valkyrie.LexExclusive("guava"),
      False,
      1000,
    )
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
    valkyrie.hscan(conn, "test:hash:scan", 0, option.None, 10, 1000)
  items |> list.length |> should.equal(4)
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
    valkyrie.hscan(
      conn,
      "test:hash:scanpattern",
      0,
      option.Some("prefix_*"),
      10,
      1000,
    )
  items |> list.length |> should.equal(3)
}

// -------------------------------- //
// ----- Edge cases and tests ----- //
// -------------------------------- //

pub fn unicode_test() {
  use conn <- get_test_conn()

  let unicode_value = "Hello 世界 🌍"
  let assert Ok("OK") =
    valkyrie.set(conn, "test:unicode", unicode_value, option.None, 1000)
  let assert Ok(retrieved) = valkyrie.get(conn, "test:unicode", 1000)
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

pub fn pool_error_scenarios_test() {
  // Test pool creation with invalid config
  let invalid_config =
    valkyrie.Config(
      host: "nonexistent.redis.server.invalid",
      port: 6379,
      auth: valkyrie.NoAuth,
    )

  // This should fail to create connections but not panic
  let pool_result = invalid_config |> valkyrie.start_pool(1, option.None, 100)
  case pool_result {
    Error(_) -> Nil
    Ok(conn) -> {
      // If it somehow succeeds, clean up
      let _ = valkyrie.shutdown(conn, 1000)
      Nil
    }
  }
  pool_result |> should.be_error
}

pub fn edge_case_operations_test() {
  use conn <- get_test_conn()

  // Test operations on empty/non-existent keys
  let assert Ok(0) = valkyrie.llen(conn, "nonexistent:list", 1000)
  let assert Ok([]) = valkyrie.lrange(conn, "nonexistent:list", 0, -1, 1000)
  let assert Error(valkyrie.NotFound) =
    valkyrie.lpop(conn, "nonexistent:list", 1, 1000)

  // Test very large list operations
  let large_items = list.range(1, 1000) |> list.map(int.to_string)
  let assert Ok(1000) =
    valkyrie.rpush(conn, "test:large_list", large_items, 5000)
  let assert Ok(1000) = valkyrie.llen(conn, "test:large_list", 1000)

  // Test hash with many fields
  let large_hash =
    list.range(1, 100)
    |> list.fold(dict.new(), fn(acc, i) {
      dict.insert(acc, "field" <> int.to_string(i), "value" <> int.to_string(i))
    })
  let assert Ok(100) = valkyrie.hset(conn, "test:large_hash", large_hash, 5000)
  let assert Ok(100) = valkyrie.hlen(conn, "test:large_hash", 1000)

  // Test set with many members
  let large_set_members = list.range(1, 100) |> list.map(int.to_string)
  let assert Ok(100) =
    valkyrie.sadd(conn, "test:large_set", large_set_members, 5000)
  let assert Ok(100) = valkyrie.scard(conn, "test:large_set", 1000)
}

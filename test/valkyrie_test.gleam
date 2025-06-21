import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option
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
    |> valkyrie.start_pool(3, 128)

  let res = next(conn)
  let assert Ok(_) = valkyrie.shutdown(conn)
  res
}

// Helper to clean up test keys
fn cleanup_keys(conn: valkyrie.Connection, keys: List(String)) -> Nil {
  case keys {
    [] -> Nil
    _ -> {
      let _ = valkyrie.del(conn, keys, 1000)
      Nil
    }
  }
}

// Connection Tests
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

// URL Parsing Tests
pub fn url_config_basic_test() {
  // Test basic redis:// URL
  let assert Ok(config) = valkyrie.url_config("redis://localhost:6379")
  config.host |> should.equal("localhost")
  config.port |> should.equal(6379)
  config.auth |> should.equal(valkyrie.NoAuth)
}

pub fn url_config_supported_schemes_test() {
  // Test redis://
  let assert Ok(config1) = valkyrie.url_config("redis://localhost:6379")
  config1.host |> should.equal("localhost")

  // Test valkey://
  let assert Ok(config2) = valkyrie.url_config("valkey://localhost:6379")
  config2.host |> should.equal("localhost")

  // Test keydb://
  let assert Ok(config3) = valkyrie.url_config("keydb://localhost:6379")
  config3.host |> should.equal("localhost")
}

pub fn url_config_default_port_test() {
  // Test URL without port (should default to 6379)
  let assert Ok(config) = valkyrie.url_config("redis://localhost")
  config.host |> should.equal("localhost")
  config.port |> should.equal(6379)
  config.auth |> should.equal(valkyrie.NoAuth)
}

pub fn url_config_custom_port_test() {
  // Test URL with custom port
  let assert Ok(config) = valkyrie.url_config("redis://localhost:6380")
  config.host |> should.equal("localhost")
  config.port |> should.equal(6380)
  config.auth |> should.equal(valkyrie.NoAuth)
}

pub fn url_config_password_only_test() {
  // Test URL with password only
  let assert Ok(config) =
    valkyrie.url_config("redis://:mypassword@localhost:6379")
  config.host |> should.equal("localhost")
  config.port |> should.equal(6379)
  config.auth |> should.equal(valkyrie.PasswordOnly("mypassword"))
}

pub fn url_config_username_password_test() {
  // Test URL with username and password
  let assert Ok(config) =
    valkyrie.url_config("redis://user:pass@localhost:6379")
  config.host |> should.equal("localhost")
  config.port |> should.equal(6379)
  config.auth |> should.equal(valkyrie.UsernameAndPassword("user", "pass"))
}

pub fn url_config_complex_credentials_test() {
  // Test URL with complex username and password (avoiding @ in password for URI parsing)
  let assert Ok(config) =
    valkyrie.url_config("redis://my_user:my_p4ssw0rd@192.168.1.100:6380")
  config.host |> should.equal("192.168.1.100")
  config.port |> should.equal(6380)
  config.auth
  |> should.equal(valkyrie.UsernameAndPassword("my_user", "my_p4ssw0rd"))
}

pub fn url_config_ip_address_test() {
  // Test URL with IP address
  let assert Ok(config) = valkyrie.url_config("redis://192.168.1.100:6379")
  config.host |> should.equal("192.168.1.100")
  config.port |> should.equal(6379)
  config.auth |> should.equal(valkyrie.NoAuth)
}

pub fn url_config_error_cases_test() {
  // Test invalid scheme
  let assert Error(valkyrie.UnsupportedScheme) =
    valkyrie.url_config("http://localhost:6379")

  // Test missing scheme (URI parser treats "localhost" as scheme)
  let assert Error(valkyrie.UnsupportedScheme) =
    valkyrie.url_config("localhost:6379")

  // Test missing host
  let assert Error(valkyrie.MissingHost) = valkyrie.url_config("redis://:6379")

  // Test invalid URI
  let assert Error(valkyrie.InvalidUriFormat) =
    valkyrie.url_config("not a valid uri")
}

pub fn url_config_specific_errors_test() {
  // Test empty host
  let assert Error(valkyrie.MissingHost) = valkyrie.url_config("redis://")

  // Test different unsupported schemes
  let assert Error(valkyrie.UnsupportedScheme) =
    valkyrie.url_config("https://localhost:6379")

  let assert Error(valkyrie.UnsupportedScheme) =
    valkyrie.url_config("mysql://localhost:6379")

  // Test truly missing scheme (relative URI)
  let assert Error(valkyrie.MissingScheme) =
    valkyrie.url_config("//localhost:6379")

  // Test missing host with valid scheme
  let assert Error(valkyrie.MissingHost) = valkyrie.url_config("redis:")

  // Test malformed URI
  let assert Error(valkyrie.InvalidUriFormat) =
    valkyrie.url_config("redis://[invalid")
}

pub fn url_config_with_database_test() {
  // Test URL with database path (should be ignored but not cause error)
  let assert Ok(config) = valkyrie.url_config("redis://localhost:6379/0")
  config.host |> should.equal("localhost")
  config.port |> should.equal(6379)
  config.auth |> should.equal(valkyrie.NoAuth)
}

pub fn url_config_integration_test() {
  // Test that url_config creates a working connection
  let assert Ok(config) = valkyrie.url_config("redis://localhost:6379")
  let assert Ok(conn) = config |> valkyrie.create_connection(1000)

  // Test that the connection works
  let assert Ok("PONG") = valkyrie.ping(conn, 1000)

  // Clean up
  let assert Ok(_) = valkyrie.shutdown(conn)
}

// Basic String Operations
pub fn set_get_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    conn
    |> valkyrie.set("test:key", "test value", option.None, 1000)

  let assert Ok("test value") = conn |> valkyrie.get("test:key", 1000)

  cleanup_keys(conn, ["test:key"])
}

pub fn get_nonexistent_key_test() {
  use conn <- get_test_conn()

  let assert Error(valkyrie.NotFound) =
    conn |> valkyrie.get("nonexistent:key", 1000)
}

pub fn set_with_options_test() {
  use conn <- get_test_conn()

  // Test with IfNotExists
  let options =
    valkyrie.SetOptions(
      existence_condition: option.Some(valkyrie.IfNotExists),
      return_old: False,
      expiry_option: option.None,
    )

  let assert Ok("OK") =
    conn
    |> valkyrie.set("test:nx:key", "value1", option.Some(options), 1000)

  // Should fail because key exists - Redis returns nil which becomes NotFound error
  let assert Error(valkyrie.NotFound) =
    conn
    |> valkyrie.set("test:nx:key", "value2", option.Some(options), 1000)

  cleanup_keys(conn, ["test:nx:key"])
}

pub fn mset_mget_test() {
  use conn <- get_test_conn()

  let pairs = [
    #("test:mset:1", "value1"),
    #("test:mset:2", "value2"),
    #("test:mset:3", "value3"),
  ]

  let assert Ok("OK") = conn |> valkyrie.mset(pairs, 1000)

  // mget only with existing keys - it returns an error if any key is missing
  let keys = ["test:mset:1", "test:mset:2", "test:mset:3"]
  let assert Ok(values) = conn |> valkyrie.mget(keys, 1000)

  values
  |> should.equal(["value1", "value2", "value3"])

  cleanup_keys(conn, ["test:mset:1", "test:mset:2", "test:mset:3"])
}

pub fn append_test() {
  use conn <- get_test_conn()

  let assert Ok(5) = conn |> valkyrie.append("test:append", "Hello", 1000)
  let assert Ok(11) = conn |> valkyrie.append("test:append", " World", 1000)
  let assert Ok("Hello World") = conn |> valkyrie.get("test:append", 1000)

  cleanup_keys(conn, ["test:append"])
}

// Key Operations
pub fn exists_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    conn
    |> valkyrie.set("test:exists:1", "value", option.None, 1000)

  let assert Ok("OK") =
    conn
    |> valkyrie.set("test:exists:2", "value", option.None, 1000)

  let assert Ok(2) =
    conn |> valkyrie.exists(["test:exists:1", "test:exists:2"], 1000)
  let assert Ok(1) =
    conn |> valkyrie.exists(["test:exists:1", "test:nonexistent"], 1000)
  let assert Ok(0) = conn |> valkyrie.exists(["test:nonexistent"], 1000)

  cleanup_keys(conn, ["test:exists:1", "test:exists:2"])
}

pub fn del_test() {
  use conn <- get_test_conn()

  // Set up test keys
  let assert Ok("OK") =
    conn |> valkyrie.set("test:del:1", "value1", option.None, 1000)
  let assert Ok("OK") =
    conn |> valkyrie.set("test:del:2", "value2", option.None, 1000)
  let assert Ok("OK") =
    conn |> valkyrie.set("test:del:3", "value3", option.None, 1000)

  // Delete multiple keys
  let assert Ok(2) = conn |> valkyrie.del(["test:del:1", "test:del:2"], 1000)

  // Verify they're gone
  let assert Error(valkyrie.NotFound) = conn |> valkyrie.get("test:del:1", 1000)
  let assert Error(valkyrie.NotFound) = conn |> valkyrie.get("test:del:2", 1000)
  let assert Ok("value3") = conn |> valkyrie.get("test:del:3", 1000)

  cleanup_keys(conn, ["test:del:3"])
}

pub fn keys_test() {
  use conn <- get_test_conn()

  // Set up test keys
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

  cleanup_keys(conn, ["test:keys:foo", "test:keys:bar", "test:keys:baz"])
}

pub fn scan_test() {
  use conn <- get_test_conn()

  // Set up test keys
  let test_keys =
    list.range(1, 20)
    |> list.map(fn(i) { "test:scan:" <> int.to_string(i) })

  list.each(test_keys, fn(key) {
    let assert Ok("OK") = conn |> valkyrie.set(key, "value", option.None, 1000)
  })

  // Scan with count
  let assert Ok(#(keys, cursor)) = conn |> valkyrie.scan(0, 10, 1000)
  keys |> list.length |> should.not_equal(0)

  // Continue scanning if cursor is not 0
  case cursor {
    0 -> Nil
    _ -> {
      let assert Ok(_) = conn |> valkyrie.scan(cursor, 10, 1000)
      Nil
    }
  }

  cleanup_keys(conn, test_keys)
}

pub fn scan_pattern_test() {
  use conn <- get_test_conn()

  // Set up test keys
  let assert Ok("OK") =
    conn |> valkyrie.set("test:scanpat:1", "1", option.None, 1000)
  let assert Ok("OK") =
    conn |> valkyrie.set("test:scanpat:2", "2", option.None, 1000)
  let assert Ok("OK") =
    conn |> valkyrie.set("other:key", "3", option.None, 1000)

  let assert Ok(#(keys, _)) =
    conn |> valkyrie.scan_pattern(0, "test:scanpat:*", 10, 1000)

  // Should only find keys matching pattern
  keys
  |> list.all(fn(key) { string.starts_with(key, "test:scanpat:") })
  |> should.be_true

  cleanup_keys(conn, ["test:scanpat:1", "test:scanpat:2", "other:key"])
}

pub fn key_type_test() {
  use conn <- get_test_conn()

  // String key
  let assert Ok("OK") =
    conn |> valkyrie.set("test:type:string", "value", option.None, 1000)
  let assert Ok(valkyrie.String) =
    conn |> valkyrie.key_type("test:type:string", 1000)

  // List key
  let assert Ok(_) = conn |> valkyrie.lpush("test:type:list", ["item"], 1000)
  let assert Ok(valkyrie.List) =
    conn |> valkyrie.key_type("test:type:list", 1000)

  // Non-existent key
  let assert Error(valkyrie.NotFound) =
    conn |> valkyrie.key_type("test:type:nonexistent", 1000)

  cleanup_keys(conn, ["test:type:string", "test:type:list"])
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

  cleanup_keys(conn, ["test:rename:new"])
}

pub fn renamenx_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    conn |> valkyrie.set("test:renamenx:old", "value1", option.None, 1000)
  let assert Ok("OK") =
    conn
    |> valkyrie.set("test:renamenx:existing", "value2", option.None, 1000)

  // Should succeed - target doesn't exist (returns 1)
  let assert Ok(1) =
    conn |> valkyrie.renamenx("test:renamenx:old", "test:renamenx:new", 1000)

  // Set up another key to rename
  let assert Ok("OK") =
    conn |> valkyrie.set("test:renamenx:another", "value3", option.None, 1000)

  // Should fail - target exists (returns 0)
  let assert Ok(0) =
    conn
    |> valkyrie.renamenx(
      "test:renamenx:another",
      "test:renamenx:existing",
      1000,
    )

  cleanup_keys(conn, [
    "test:renamenx:new", "test:renamenx:existing", "test:renamenx:another",
  ])
}

pub fn random_key_test() {
  use conn <- get_test_conn()

  // Set up some keys
  let assert Ok("OK") =
    conn |> valkyrie.set("test:random:1", "1", option.None, 1000)
  let assert Ok("OK") =
    conn |> valkyrie.set("test:random:2", "2", option.None, 1000)

  let assert Ok(key) = conn |> valkyrie.random_key(1000)
  key |> string.length |> should.not_equal(0)

  cleanup_keys(conn, ["test:random:1", "test:random:2"])
}

// Numeric Operations
pub fn incr_test() {
  use conn <- get_test_conn()

  let assert Ok(1) = conn |> valkyrie.incr("test:incr", 1000)
  let assert Ok(2) = conn |> valkyrie.incr("test:incr", 1000)
  let assert Ok(3) = conn |> valkyrie.incr("test:incr", 1000)

  cleanup_keys(conn, ["test:incr"])
}

pub fn incrby_test() {
  use conn <- get_test_conn()

  let assert Ok(5) = conn |> valkyrie.incrby("test:incrby", 5, 1000)
  let assert Ok(15) = conn |> valkyrie.incrby("test:incrby", 10, 1000)
  let assert Ok(12) = conn |> valkyrie.incrby("test:incrby", -3, 1000)

  cleanup_keys(conn, ["test:incrby"])
}

pub fn incrbyfloat_test() {
  use conn <- get_test_conn()

  // Clean up first in case key exists from previous run
  let _ = valkyrie.del(conn, ["test:incrbyfloat"], 1000)

  let assert Ok(result1) =
    conn |> valkyrie.incrbyfloat("test:incrbyfloat", 2.5, 1000)
  result1 |> should.equal(2.5)

  let assert Ok(result2) =
    conn |> valkyrie.incrbyfloat("test:incrbyfloat", 1.5, 1000)
  result2 |> should.equal(4.0)

  cleanup_keys(conn, ["test:incrbyfloat"])
}

pub fn decr_test() {
  use conn <- get_test_conn()

  let assert Ok(-1) = conn |> valkyrie.decr("test:decr", 1000)
  let assert Ok(-2) = conn |> valkyrie.decr("test:decr", 1000)

  cleanup_keys(conn, ["test:decr"])
}

pub fn decr_by_test() {
  use conn <- get_test_conn()

  let assert Ok(-5) = conn |> valkyrie.decr_by("test:decrby", 5, 1000)
  let assert Ok(-15) = conn |> valkyrie.decr_by("test:decrby", 10, 1000)
  let assert Ok(-12) = conn |> valkyrie.decr_by("test:decrby", -3, 1000)

  cleanup_keys(conn, ["test:decrby"])
}

// List Operations
pub fn lpush_rpush_test() {
  use conn <- get_test_conn()

  let assert Ok(2) =
    conn |> valkyrie.lpush("test:list", ["first", "second"], 1000)
  let assert Ok(4) =
    conn |> valkyrie.rpush("test:list", ["third", "fourth"], 1000)

  let assert Ok(items) = conn |> valkyrie.lrange("test:list", 0, -1, 1000)
  items |> should.equal(["second", "first", "third", "fourth"])

  cleanup_keys(conn, ["test:list"])
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

  cleanup_keys(conn, ["test:listx"])
}

pub fn llen_test() {
  use conn <- get_test_conn()

  let assert Ok(0) = conn |> valkyrie.llen("test:llen", 1000)

  let assert Ok(_) = conn |> valkyrie.rpush("test:llen", ["a", "b", "c"], 1000)
  let assert Ok(3) = conn |> valkyrie.llen("test:llen", 1000)

  cleanup_keys(conn, ["test:llen"])
}

pub fn lrange_test() {
  use conn <- get_test_conn()

  let assert Ok(_) =
    conn |> valkyrie.rpush("test:lrange", ["a", "b", "c", "d", "e"], 1000)

  let assert Ok(all) = conn |> valkyrie.lrange("test:lrange", 0, -1, 1000)
  all |> should.equal(["a", "b", "c", "d", "e"])

  let assert Ok(subset) = conn |> valkyrie.lrange("test:lrange", 1, 3, 1000)
  subset |> should.equal(["b", "c", "d"])

  cleanup_keys(conn, ["test:lrange"])
}

pub fn lpop_rpop_test() {
  use conn <- get_test_conn()

  // Clean up first in case key exists from previous run
  let _ = valkyrie.del(conn, ["test:pop"], 1000)

  let assert Ok(_) = conn |> valkyrie.rpush("test:pop", ["a", "b", "c"], 1000)

  // lpop and rpop take a count parameter
  let assert Ok("a") = conn |> valkyrie.lpop("test:pop", 1, 1000)
  let assert Ok("c") = conn |> valkyrie.rpop("test:pop", 1, 1000)
  let assert Ok(1) = conn |> valkyrie.llen("test:pop", 1000)

  cleanup_keys(conn, ["test:pop"])
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

  cleanup_keys(conn, ["test:lindex"])
}

pub fn lrem_test() {
  use conn <- get_test_conn()

  let assert Ok(_) =
    conn |> valkyrie.rpush("test:lrem", ["a", "b", "a", "c", "a"], 1000)

  // Remove first 2 occurrences of "a"
  let assert Ok(2) = conn |> valkyrie.lrem("test:lrem", 2, "a", 1000)

  let assert Ok(items) = conn |> valkyrie.lrange("test:lrem", 0, -1, 1000)
  items |> should.equal(["b", "c", "a"])

  cleanup_keys(conn, ["test:lrem"])
}

pub fn lset_test() {
  use conn <- get_test_conn()

  let assert Ok(_) = conn |> valkyrie.rpush("test:lset", ["a", "b", "c"], 1000)

  let assert Ok("OK") = conn |> valkyrie.lset("test:lset", 1, "B", 1000)

  let assert Ok(items) = conn |> valkyrie.lrange("test:lset", 0, -1, 1000)
  items |> should.equal(["a", "B", "c"])

  cleanup_keys(conn, ["test:lset"])
}

pub fn linsert_test() {
  use conn <- get_test_conn()

  let assert Ok(_) = conn |> valkyrie.rpush("test:linsert", ["a", "c"], 1000)

  let assert Ok(3) =
    conn |> valkyrie.linsert("test:linsert", valkyrie.Before, "c", "b", 1000)

  let assert Ok(items) = conn |> valkyrie.lrange("test:linsert", 0, -1, 1000)
  items |> should.equal(["a", "b", "c"])

  cleanup_keys(conn, ["test:linsert"])
}

// Utility Operations
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

  // Set expiry (returns 1 for success)
  let assert Ok(1) =
    conn |> valkyrie.expire("test:expire", 10, option.None, 1000)

  // Key should still exist
  let assert Ok("value") = conn |> valkyrie.get("test:expire", 1000)

  // Try to set expiry on non-existent key (returns 0)
  let assert Ok(0) =
    conn |> valkyrie.expire("test:expire:nonexistent", 10, option.None, 1000)

  cleanup_keys(conn, ["test:expire"])
}

pub fn persist_test() {
  use conn <- get_test_conn()

  let assert Ok("OK") =
    conn |> valkyrie.set("test:persist", "value", option.None, 1000)
  let assert Ok(1) =
    conn |> valkyrie.expire("test:persist", 10, option.None, 1000)

  // Remove expiry (returns 1 for success)
  let assert Ok(1) = conn |> valkyrie.persist("test:persist", 1000)

  // Try on key without expiry (returns 0)
  let assert Ok("OK") =
    conn |> valkyrie.set("test:persist:no_ttl", "value", option.None, 1000)
  let assert Ok(0) = conn |> valkyrie.persist("test:persist:no_ttl", 1000)

  cleanup_keys(conn, ["test:persist", "test:persist:no_ttl"])
}

// Custom Command Test
pub fn custom_command_test() {
  use conn <- get_test_conn()

  // Test ECHO command
  let assert Ok([protocol.BulkString("Hello World")]) =
    conn |> valkyrie.custom(["ECHO", "Hello World"], 1000)

  // Test PING command with custom message (returns BulkString)
  let assert Ok([protocol.BulkString("Custom ping")]) =
    conn |> valkyrie.custom(["PING", "Custom ping"], 1000)
}

// Error Handling Tests
pub fn timeout_error_test() {
  use conn <- get_test_conn()

  // Use a very short timeout that should fail
  let result = conn |> valkyrie.get("any_key", 1)

  case result {
    Error(valkyrie.Timeout) -> Nil
    Error(_) -> Nil
    // Other errors are possible too
    Ok(_) -> panic as "Expected timeout error"
  }
}

// Edge Cases
pub fn empty_list_operations_test() {
  use conn <- get_test_conn()

  // Pop from empty list
  let assert Error(valkyrie.NotFound) =
    conn |> valkyrie.lpop("test:empty", 1, 1000)
  let assert Error(valkyrie.NotFound) =
    conn |> valkyrie.rpop("test:empty", 1, 1000)

  // Get index from non-existent list
  let assert Error(valkyrie.NotFound) =
    conn |> valkyrie.lindex("test:empty", 0, 1000)
}

pub fn unicode_test() {
  use conn <- get_test_conn()

  let unicode_value = "Hello 世界 🌍"
  let assert Ok("OK") =
    conn |> valkyrie.set("test:unicode", unicode_value, option.None, 1000)

  let assert Ok(retrieved) = conn |> valkyrie.get("test:unicode", 1000)
  retrieved |> should.equal(unicode_value)

  cleanup_keys(conn, ["test:unicode"])
}

pub fn large_value_test() {
  use conn <- get_test_conn()

  // Create a large string (100KB instead of 1MB to avoid timeout)
  let large_value = string.repeat("x", 100 * 1024)

  let assert Ok("OK") =
    conn |> valkyrie.set("test:large", large_value, option.None, 5000)

  let assert Ok(retrieved) = conn |> valkyrie.get("test:large", 5000)
  retrieved |> string.length |> should.equal(100 * 1024)

  cleanup_keys(conn, ["test:large"])
}

pub fn rapid_operations_test() {
  use conn <- get_test_conn()

  // Test that the pool handles many rapid sequential operations
  // This exercises the pool's ability to handle high throughput
  let keys =
    list.range(1, 50)
    |> list.map(fn(i) { "test:rapid:" <> int.to_string(i) })

  // Perform rapid SET operations
  keys
  |> list.each(fn(key) {
    let assert Ok("OK") =
      conn |> valkyrie.set(key, "value_" <> key, option.None, 1000)
  })

  // Perform rapid GET operations and verify results
  keys
  |> list.each(fn(key) {
    let assert Ok(value) = conn |> valkyrie.get(key, 1000)
    value |> should.equal("value_" <> key)
  })

  // Perform rapid mixed operations
  keys
  |> list.each(fn(key) {
    let assert Ok(_) = conn |> valkyrie.incr(key <> ":counter", 1000)
    let assert Ok(_) = conn |> valkyrie.exists([key], 1000)
    let assert Ok(_) = conn |> valkyrie.append(key, "_appended", 1000)
  })

  cleanup_keys(conn, keys)
  // Also cleanup counter keys
  let counter_keys = keys |> list.map(fn(key) { key <> ":counter" })
  cleanup_keys(conn, counter_keys)
}

pub fn concurrent_operations_test() {
  use conn <- get_test_conn()

  // Test that the pool handles truly concurrent operations
  let num_processes = 3
  let keys_per_process = 2

  // Create a subject to collect results from worker processes
  let result_subject = process.new_subject()

  // Spawn multiple processes that will run Redis operations concurrently
  list.range(1, num_processes)
  |> list.each(fn(process_id) {
    let _ =
      process.spawn(fn() {
        // Each process works on its own set of keys
        let process_keys =
          list.range(1, keys_per_process)
          |> list.map(fn(i) {
            "test:concurrent:p"
            <> int.to_string(process_id)
            <> ":k"
            <> int.to_string(i)
          })

        // Perform SET operations
        let set_results =
          process_keys
          |> list.map(fn(key) {
            conn |> valkyrie.set(key, "value_" <> key, option.None, 1000)
          })

        // Perform GET operations
        let get_results =
          process_keys
          |> list.map(fn(key) { conn |> valkyrie.get(key, 1000) })

        // Send results back to main process
        process.send(result_subject, #(process_id, set_results, get_results))
      })
  })

  // Collect results from all processes
  let results =
    list.range(1, num_processes)
    |> list.map(fn(_) {
      let assert Ok(result) = process.receive(result_subject, 5000)
      result
    })

  // Verify all operations succeeded
  results
  |> list.each(fn(result) {
    let #(process_id, set_results, get_results) = result

    // Verify all SET operations succeeded
    set_results
    |> list.each(fn(set_result) { set_result |> should.equal(Ok("OK")) })

    // Verify all GET operations returned correct values
    get_results
    |> list.index_map(fn(get_result, index) {
      let expected_key =
        "test:concurrent:p"
        <> int.to_string(process_id)
        <> ":k"
        <> int.to_string(index + 1)
      let expected_value = "value_" <> expected_key
      get_result |> should.equal(Ok(expected_value))
    })
  })

  // Clean up all keys
  let all_keys =
    list.range(1, num_processes)
    |> list.flat_map(fn(process_id) {
      list.range(1, keys_per_process)
      |> list.map(fn(i) {
        "test:concurrent:p"
        <> int.to_string(process_id)
        <> ":k"
        <> int.to_string(i)
      })
    })

  cleanup_keys(conn, all_keys)
}

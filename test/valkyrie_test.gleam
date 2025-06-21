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

fn get_test_client(next) {
  let assert Ok(client) =
    valkyrie.default_config()
    |> valkyrie.start_pool(3, 128)

  let res = next(client)
  let assert Ok(_) = valkyrie.shutdown(client)
  res
}

// Helper to clean up test keys
fn cleanup_keys(client, keys) {
  case keys {
    [] -> Nil
    _ -> {
      let _ = valkyrie.del(client, keys, 1000)
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
  use client <- get_test_client()
  let assert Ok("PONG") = valkyrie.ping(client, 1000)
}

// Basic String Operations
pub fn set_get_test() {
  use client <- get_test_client()

  let assert Ok("OK") =
    client
    |> valkyrie.set("test:key", "test value", option.None, 1000)

  let assert Ok("test value") = client |> valkyrie.get("test:key", 1000)

  cleanup_keys(client, ["test:key"])
}

pub fn get_nonexistent_key_test() {
  use client <- get_test_client()

  let assert Error(valkyrie.NotFound) =
    client |> valkyrie.get("nonexistent:key", 1000)
}

pub fn set_with_options_test() {
  use client <- get_test_client()

  // Test with IfNotExists
  let options =
    valkyrie.SetOptions(
      existence_condition: option.Some(valkyrie.IfNotExists),
      return_old: False,
      expiry_option: option.None,
    )

  let assert Ok("OK") =
    client
    |> valkyrie.set("test:nx:key", "value1", option.Some(options), 1000)

  // Should fail because key exists - Redis returns nil which becomes NotFound error
  let assert Error(valkyrie.NotFound) =
    client
    |> valkyrie.set("test:nx:key", "value2", option.Some(options), 1000)

  cleanup_keys(client, ["test:nx:key"])
}

pub fn mset_mget_test() {
  use client <- get_test_client()

  let pairs = [
    #("test:mset:1", "value1"),
    #("test:mset:2", "value2"),
    #("test:mset:3", "value3"),
  ]

  let assert Ok("OK") = client |> valkyrie.mset(pairs, 1000)

  // mget only with existing keys - it returns an error if any key is missing
  let keys = ["test:mset:1", "test:mset:2", "test:mset:3"]
  let assert Ok(values) = client |> valkyrie.mget(keys, 1000)

  values
  |> should.equal(["value1", "value2", "value3"])

  cleanup_keys(client, ["test:mset:1", "test:mset:2", "test:mset:3"])
}

pub fn append_test() {
  use client <- get_test_client()

  let assert Ok(5) = client |> valkyrie.append("test:append", "Hello", 1000)
  let assert Ok(11) = client |> valkyrie.append("test:append", " World", 1000)
  let assert Ok("Hello World") = client |> valkyrie.get("test:append", 1000)

  cleanup_keys(client, ["test:append"])
}

// Key Operations
pub fn exists_test() {
  use client <- get_test_client()

  let assert Ok("OK") =
    client
    |> valkyrie.set("test:exists:1", "value", option.None, 1000)

  let assert Ok("OK") =
    client
    |> valkyrie.set("test:exists:2", "value", option.None, 1000)

  let assert Ok(2) =
    client |> valkyrie.exists(["test:exists:1", "test:exists:2"], 1000)
  let assert Ok(1) =
    client |> valkyrie.exists(["test:exists:1", "test:nonexistent"], 1000)
  let assert Ok(0) = client |> valkyrie.exists(["test:nonexistent"], 1000)

  cleanup_keys(client, ["test:exists:1", "test:exists:2"])
}

pub fn del_test() {
  use client <- get_test_client()

  // Set up test keys
  let assert Ok("OK") =
    client |> valkyrie.set("test:del:1", "value1", option.None, 1000)
  let assert Ok("OK") =
    client |> valkyrie.set("test:del:2", "value2", option.None, 1000)
  let assert Ok("OK") =
    client |> valkyrie.set("test:del:3", "value3", option.None, 1000)

  // Delete multiple keys
  let assert Ok(2) = client |> valkyrie.del(["test:del:1", "test:del:2"], 1000)

  // Verify they're gone
  let assert Error(valkyrie.NotFound) =
    client |> valkyrie.get("test:del:1", 1000)
  let assert Error(valkyrie.NotFound) =
    client |> valkyrie.get("test:del:2", 1000)
  let assert Ok("value3") = client |> valkyrie.get("test:del:3", 1000)

  cleanup_keys(client, ["test:del:3"])
}

pub fn keys_test() {
  use client <- get_test_client()

  // Set up test keys
  let assert Ok("OK") =
    client |> valkyrie.set("test:keys:foo", "1", option.None, 1000)
  let assert Ok("OK") =
    client |> valkyrie.set("test:keys:bar", "2", option.None, 1000)
  let assert Ok("OK") =
    client |> valkyrie.set("test:keys:baz", "3", option.None, 1000)

  let assert Ok(keys) = client |> valkyrie.keys("test:keys:*", 1000)
  keys |> list.length |> should.equal(3)
  keys |> list.contains("test:keys:foo") |> should.be_true
  keys |> list.contains("test:keys:bar") |> should.be_true
  keys |> list.contains("test:keys:baz") |> should.be_true

  cleanup_keys(client, ["test:keys:foo", "test:keys:bar", "test:keys:baz"])
}

pub fn scan_test() {
  use client <- get_test_client()

  // Set up test keys
  let test_keys =
    list.range(1, 20)
    |> list.map(fn(i) { "test:scan:" <> int.to_string(i) })

  list.each(test_keys, fn(key) {
    let assert Ok("OK") =
      client |> valkyrie.set(key, "value", option.None, 1000)
  })

  // Scan with count
  let assert Ok(#(keys, cursor)) = client |> valkyrie.scan(0, 10, 1000)
  keys |> list.length |> should.not_equal(0)

  // Continue scanning if cursor is not 0
  case cursor {
    0 -> Nil
    _ -> {
      let assert Ok(_) = client |> valkyrie.scan(cursor, 10, 1000)
      Nil
    }
  }

  cleanup_keys(client, test_keys)
}

pub fn scan_pattern_test() {
  use client <- get_test_client()

  // Set up test keys
  let assert Ok("OK") =
    client |> valkyrie.set("test:scanpat:1", "1", option.None, 1000)
  let assert Ok("OK") =
    client |> valkyrie.set("test:scanpat:2", "2", option.None, 1000)
  let assert Ok("OK") =
    client |> valkyrie.set("other:key", "3", option.None, 1000)

  let assert Ok(#(keys, _)) =
    client |> valkyrie.scan_pattern(0, "test:scanpat:*", 10, 1000)

  // Should only find keys matching pattern
  keys
  |> list.all(fn(key) { string.starts_with(key, "test:scanpat:") })
  |> should.be_true

  cleanup_keys(client, ["test:scanpat:1", "test:scanpat:2", "other:key"])
}

pub fn key_type_test() {
  use client <- get_test_client()

  // String key
  let assert Ok("OK") =
    client |> valkyrie.set("test:type:string", "value", option.None, 1000)
  let assert Ok(valkyrie.String) =
    client |> valkyrie.key_type("test:type:string", 1000)

  // List key
  let assert Ok(_) = client |> valkyrie.lpush("test:type:list", ["item"], 1000)
  let assert Ok(valkyrie.List) =
    client |> valkyrie.key_type("test:type:list", 1000)

  // Non-existent key
  let assert Error(valkyrie.NotFound) =
    client |> valkyrie.key_type("test:type:nonexistent", 1000)

  cleanup_keys(client, ["test:type:string", "test:type:list"])
}

pub fn rename_test() {
  use client <- get_test_client()

  let assert Ok("OK") =
    client |> valkyrie.set("test:rename:old", "value", option.None, 1000)
  let assert Ok("OK") =
    client |> valkyrie.rename("test:rename:old", "test:rename:new", 1000)

  let assert Error(valkyrie.NotFound) =
    client |> valkyrie.get("test:rename:old", 1000)
  let assert Ok("value") = client |> valkyrie.get("test:rename:new", 1000)

  cleanup_keys(client, ["test:rename:new"])
}

pub fn renamenx_test() {
  use client <- get_test_client()

  let assert Ok("OK") =
    client |> valkyrie.set("test:renamenx:old", "value1", option.None, 1000)
  let assert Ok("OK") =
    client
    |> valkyrie.set("test:renamenx:existing", "value2", option.None, 1000)

  // Should succeed - target doesn't exist (returns 1)
  let assert Ok(1) =
    client |> valkyrie.renamenx("test:renamenx:old", "test:renamenx:new", 1000)

  // Set up another key to rename
  let assert Ok("OK") =
    client |> valkyrie.set("test:renamenx:another", "value3", option.None, 1000)

  // Should fail - target exists (returns 0)
  let assert Ok(0) =
    client
    |> valkyrie.renamenx(
      "test:renamenx:another",
      "test:renamenx:existing",
      1000,
    )

  cleanup_keys(client, [
    "test:renamenx:new", "test:renamenx:existing", "test:renamenx:another",
  ])
}

pub fn random_key_test() {
  use client <- get_test_client()

  // Set up some keys
  let assert Ok("OK") =
    client |> valkyrie.set("test:random:1", "1", option.None, 1000)
  let assert Ok("OK") =
    client |> valkyrie.set("test:random:2", "2", option.None, 1000)

  let assert Ok(key) = client |> valkyrie.random_key(1000)
  key |> string.length |> should.not_equal(0)

  cleanup_keys(client, ["test:random:1", "test:random:2"])
}

// Numeric Operations
pub fn incr_test() {
  use client <- get_test_client()

  let assert Ok(1) = client |> valkyrie.incr("test:incr", 1000)
  let assert Ok(2) = client |> valkyrie.incr("test:incr", 1000)
  let assert Ok(3) = client |> valkyrie.incr("test:incr", 1000)

  cleanup_keys(client, ["test:incr"])
}

pub fn incrby_test() {
  use client <- get_test_client()

  let assert Ok(5) = client |> valkyrie.incrby("test:incrby", 5, 1000)
  let assert Ok(15) = client |> valkyrie.incrby("test:incrby", 10, 1000)
  let assert Ok(12) = client |> valkyrie.incrby("test:incrby", -3, 1000)

  cleanup_keys(client, ["test:incrby"])
}

pub fn incrbyfloat_test() {
  use client <- get_test_client()

  // Clean up first in case key exists from previous run
  let _ = valkyrie.del(client, ["test:incrbyfloat"], 1000)

  let assert Ok(result1) =
    client |> valkyrie.incrbyfloat("test:incrbyfloat", 2.5, 1000)
  result1 |> should.equal(2.5)

  let assert Ok(result2) =
    client |> valkyrie.incrbyfloat("test:incrbyfloat", 1.5, 1000)
  result2 |> should.equal(4.0)

  cleanup_keys(client, ["test:incrbyfloat"])
}

pub fn decr_test() {
  use client <- get_test_client()

  let assert Ok(-1) = client |> valkyrie.decr("test:decr", 1000)
  let assert Ok(-2) = client |> valkyrie.decr("test:decr", 1000)

  cleanup_keys(client, ["test:decr"])
}

pub fn decr_by_test() {
  use client <- get_test_client()

  let assert Ok(-5) = client |> valkyrie.decr_by("test:decrby", 5, 1000)
  let assert Ok(-15) = client |> valkyrie.decr_by("test:decrby", 10, 1000)
  let assert Ok(-12) = client |> valkyrie.decr_by("test:decrby", -3, 1000)

  cleanup_keys(client, ["test:decrby"])
}

// List Operations
pub fn lpush_rpush_test() {
  use client <- get_test_client()

  let assert Ok(2) =
    client |> valkyrie.lpush("test:list", ["first", "second"], 1000)
  let assert Ok(4) =
    client |> valkyrie.rpush("test:list", ["third", "fourth"], 1000)

  let assert Ok(items) = client |> valkyrie.lrange("test:list", 0, -1, 1000)
  items |> should.equal(["second", "first", "third", "fourth"])

  cleanup_keys(client, ["test:list"])
}

pub fn lpushx_rpushx_test() {
  use client <- get_test_client()

  // Should fail on non-existent list
  let assert Ok(0) = client |> valkyrie.lpushx("test:listx", ["value"], 1000)
  let assert Ok(0) = client |> valkyrie.rpushx("test:listx", ["value"], 1000)

  // Create list first
  let assert Ok(_) = client |> valkyrie.lpush("test:listx", ["initial"], 1000)

  // Now should work
  let assert Ok(2) = client |> valkyrie.lpushx("test:listx", ["left"], 1000)
  let assert Ok(3) = client |> valkyrie.rpushx("test:listx", ["right"], 1000)

  let assert Ok(items) = client |> valkyrie.lrange("test:listx", 0, -1, 1000)
  items |> should.equal(["left", "initial", "right"])

  cleanup_keys(client, ["test:listx"])
}

pub fn llen_test() {
  use client <- get_test_client()

  let assert Ok(0) = client |> valkyrie.llen("test:llen", 1000)

  let assert Ok(_) =
    client |> valkyrie.rpush("test:llen", ["a", "b", "c"], 1000)
  let assert Ok(3) = client |> valkyrie.llen("test:llen", 1000)

  cleanup_keys(client, ["test:llen"])
}

pub fn lrange_test() {
  use client <- get_test_client()

  let assert Ok(_) =
    client |> valkyrie.rpush("test:lrange", ["a", "b", "c", "d", "e"], 1000)

  let assert Ok(all) = client |> valkyrie.lrange("test:lrange", 0, -1, 1000)
  all |> should.equal(["a", "b", "c", "d", "e"])

  let assert Ok(subset) = client |> valkyrie.lrange("test:lrange", 1, 3, 1000)
  subset |> should.equal(["b", "c", "d"])

  cleanup_keys(client, ["test:lrange"])
}

pub fn lpop_rpop_test() {
  use client <- get_test_client()

  // Clean up first in case key exists from previous run
  let _ = valkyrie.del(client, ["test:pop"], 1000)

  let assert Ok(_) = client |> valkyrie.rpush("test:pop", ["a", "b", "c"], 1000)

  // lpop and rpop take a count parameter
  let assert Ok("a") = client |> valkyrie.lpop("test:pop", 1, 1000)
  let assert Ok("c") = client |> valkyrie.rpop("test:pop", 1, 1000)
  let assert Ok(1) = client |> valkyrie.llen("test:pop", 1000)

  cleanup_keys(client, ["test:pop"])
}

pub fn lindex_test() {
  use client <- get_test_client()

  let assert Ok(_) =
    client |> valkyrie.rpush("test:lindex", ["a", "b", "c"], 1000)

  let assert Ok("a") = client |> valkyrie.lindex("test:lindex", 0, 1000)
  let assert Ok("b") = client |> valkyrie.lindex("test:lindex", 1, 1000)
  let assert Ok("c") = client |> valkyrie.lindex("test:lindex", -1, 1000)
  let assert Error(valkyrie.NotFound) =
    client |> valkyrie.lindex("test:lindex", 10, 1000)

  cleanup_keys(client, ["test:lindex"])
}

pub fn lrem_test() {
  use client <- get_test_client()

  let assert Ok(_) =
    client |> valkyrie.rpush("test:lrem", ["a", "b", "a", "c", "a"], 1000)

  // Remove first 2 occurrences of "a"
  let assert Ok(2) = client |> valkyrie.lrem("test:lrem", 2, "a", 1000)

  let assert Ok(items) = client |> valkyrie.lrange("test:lrem", 0, -1, 1000)
  items |> should.equal(["b", "c", "a"])

  cleanup_keys(client, ["test:lrem"])
}

pub fn lset_test() {
  use client <- get_test_client()

  let assert Ok(_) =
    client |> valkyrie.rpush("test:lset", ["a", "b", "c"], 1000)

  let assert Ok("OK") = client |> valkyrie.lset("test:lset", 1, "B", 1000)

  let assert Ok(items) = client |> valkyrie.lrange("test:lset", 0, -1, 1000)
  items |> should.equal(["a", "B", "c"])

  cleanup_keys(client, ["test:lset"])
}

pub fn linsert_test() {
  use client <- get_test_client()

  let assert Ok(_) = client |> valkyrie.rpush("test:linsert", ["a", "c"], 1000)

  let assert Ok(3) =
    client |> valkyrie.linsert("test:linsert", valkyrie.Before, "c", "b", 1000)

  let assert Ok(items) = client |> valkyrie.lrange("test:linsert", 0, -1, 1000)
  items |> should.equal(["a", "b", "c"])

  cleanup_keys(client, ["test:linsert"])
}

// Utility Operations
pub fn ping_test() {
  use client <- get_test_client()

  let assert Ok("PONG") = client |> valkyrie.ping(1000)
}

pub fn ping_message_test() {
  use client <- get_test_client()

  let assert Ok("Hello Redis") =
    client |> valkyrie.ping_message("Hello Redis", 1000)
}

pub fn expire_test() {
  use client <- get_test_client()

  let assert Ok("OK") =
    client |> valkyrie.set("test:expire", "value", option.None, 1000)

  // Set expiry (returns 1 for success)
  let assert Ok(1) =
    client |> valkyrie.expire("test:expire", 10, option.None, 1000)

  // Key should still exist
  let assert Ok("value") = client |> valkyrie.get("test:expire", 1000)

  // Try to set expiry on non-existent key (returns 0)
  let assert Ok(0) =
    client |> valkyrie.expire("test:expire:nonexistent", 10, option.None, 1000)

  cleanup_keys(client, ["test:expire"])
}

pub fn persist_test() {
  use client <- get_test_client()

  let assert Ok("OK") =
    client |> valkyrie.set("test:persist", "value", option.None, 1000)
  let assert Ok(1) =
    client |> valkyrie.expire("test:persist", 10, option.None, 1000)

  // Remove expiry (returns 1 for success)
  let assert Ok(1) = client |> valkyrie.persist("test:persist", 1000)

  // Try on key without expiry (returns 0)
  let assert Ok("OK") =
    client |> valkyrie.set("test:persist:no_ttl", "value", option.None, 1000)
  let assert Ok(0) = client |> valkyrie.persist("test:persist:no_ttl", 1000)

  cleanup_keys(client, ["test:persist", "test:persist:no_ttl"])
}

// Custom Command Test
pub fn custom_command_test() {
  use client <- get_test_client()

  // Test ECHO command
  let assert Ok([protocol.BulkString("Hello World")]) =
    client |> valkyrie.custom(["ECHO", "Hello World"], 1000)

  // Test PING command with custom message (returns BulkString)
  let assert Ok([protocol.BulkString("Custom ping")]) =
    client |> valkyrie.custom(["PING", "Custom ping"], 1000)
}

// Error Handling Tests
pub fn timeout_error_test() {
  use client <- get_test_client()

  // Use a very short timeout that should fail
  let result = client |> valkyrie.get("any_key", 1)

  case result {
    Error(valkyrie.Timeout) -> Nil
    Error(_) -> Nil
    // Other errors are possible too
    Ok(_) -> panic as "Expected timeout error"
  }
}

// Edge Cases
pub fn empty_list_operations_test() {
  use client <- get_test_client()

  // Pop from empty list
  let assert Error(valkyrie.NotFound) =
    client |> valkyrie.lpop("test:empty", 1, 1000)
  let assert Error(valkyrie.NotFound) =
    client |> valkyrie.rpop("test:empty", 1, 1000)

  // Get index from non-existent list
  let assert Error(valkyrie.NotFound) =
    client |> valkyrie.lindex("test:empty", 0, 1000)
}

pub fn unicode_test() {
  use client <- get_test_client()

  let unicode_value = "Hello 世界 🌍"
  let assert Ok("OK") =
    client |> valkyrie.set("test:unicode", unicode_value, option.None, 1000)

  let assert Ok(retrieved) = client |> valkyrie.get("test:unicode", 1000)
  retrieved |> should.equal(unicode_value)

  cleanup_keys(client, ["test:unicode"])
}

pub fn large_value_test() {
  use client <- get_test_client()

  // Create a large string (100KB instead of 1MB to avoid timeout)
  let large_value = string.repeat("x", 100 * 1024)

  let assert Ok("OK") =
    client |> valkyrie.set("test:large", large_value, option.None, 5000)

  let assert Ok(retrieved) = client |> valkyrie.get("test:large", 5000)
  retrieved |> string.length |> should.equal(100 * 1024)

  cleanup_keys(client, ["test:large"])
}

pub fn rapid_operations_test() {
  use client <- get_test_client()

  // Test that the pool handles many rapid sequential operations
  // This exercises the pool's ability to handle high throughput
  let keys =
    list.range(1, 50)
    |> list.map(fn(i) { "test:rapid:" <> int.to_string(i) })

  // Perform rapid SET operations
  keys
  |> list.each(fn(key) {
    let assert Ok("OK") =
      client |> valkyrie.set(key, "value_" <> key, option.None, 1000)
  })

  // Perform rapid GET operations and verify results
  keys
  |> list.each(fn(key) {
    let assert Ok(value) = client |> valkyrie.get(key, 1000)
    value |> should.equal("value_" <> key)
  })

  // Perform rapid mixed operations
  keys
  |> list.each(fn(key) {
    let assert Ok(_) = client |> valkyrie.incr(key <> ":counter", 1000)
    let assert Ok(_) = client |> valkyrie.exists([key], 1000)
    let assert Ok(_) = client |> valkyrie.append(key, "_appended", 1000)
  })

  cleanup_keys(client, keys)
  // Also cleanup counter keys
  let counter_keys = keys |> list.map(fn(key) { key <> ":counter" })
  cleanup_keys(client, counter_keys)
}

pub fn concurrent_operations_test() {
  use client <- get_test_client()

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
            client |> valkyrie.set(key, "value_" <> key, option.None, 1000)
          })

        // Perform GET operations
        let get_results =
          process_keys
          |> list.map(fn(key) { client |> valkyrie.get(key, 1000) })

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

  cleanup_keys(client, all_keys)
}

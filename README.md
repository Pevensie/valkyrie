# 🌌 Valkyrie

[![Package Version](https://img.shields.io/hexpm/v/valkyrie)](https://hex.pm/packages/valkyrie)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/valkyrie/)

A lightweight, high performance Gleam client for Valkey, KeyDB, Redis, Dragonfly and
other Redis-compatible databases.

## Installation

```sh
gleam add valkyrie
```

## Usage

The recommended way to use Valkyrie is by starting a connection pool as part of your
application's supervision tree.

Once you have a connection, the API is the same for both single connections (not
recommended) and pooled connections.

```gleam
import gleam/erlang/process
import gleam/option
import gleam/otp/static_supervisor as supervisor
import valkyrie

pub fn main() {
  // Create a name to interact with the connection pool once it's started under the
  // static supervisor.
  let pool_name = process.new_name("connection_pool")

  // Define a pool of 10 connections to the default Redis instance on localhost.
  let valkyrie_child_spec =
    valkyrie.default_config()
    |> valkyrie.supervised_pool(
      size: 10,
      name: option.Some(pool_name),
      timeout: 1000,
    )

  // Start the pool under a supervisor
  let assert Ok(_started) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(valkyrie_child_spec)
    |> supervisor.start

  // Get the connection now that the pool is started
  let conn = valkyrie.named_connection(pool_name)

  // Use the connection.
  let assert Ok(_) = valkyrie.set(conn, "key", "value", option.None, 1000)
  let assert Ok("value") = valkyrie.get(conn, "key", 1000)
}
```

### Pipelines and Transactions

Valkyrie supports pipelining multiple commands to reduce round-trips to the server.
Use `pipeline.exec` to execute commands in a pipeline, or `pipeline.exec_transaction`
to wrap commands in a `MULTI`/`EXEC` transaction for atomic execution.

```gleam
import gleam/list
import valkyrie
import valkyrie/pipeline
import valkyrie/resp

pub fn main() {
  // ... set up connection as above ...

  // Execute multiple commands in a single round-trip
  let assert Ok(results) =
    pipeline.new()
    |> pipeline.set("counter", "0", option.None)
    |> pipeline.incr("counter")
    |> pipeline.incr("counter")
    |> pipeline.incr("counter")
    |> pipeline.get("counter")
    |> pipeline.exec(conn, 1000)

  // Results are returned in order
  let assert Ok(resp.BulkString("3")) = list.last(results)

  // Or use a transaction for atomic execution
  let assert Ok(tx_results) =
    pipeline.new()
    |> pipeline.incr("counter")
    |> pipeline.incr("counter")
    |> pipeline.exec_transaction(conn, 1000)

  // Transaction results contain only the command outputs
  let assert [resp.Integer(4), resp.Integer(5)] = tx_results
}
```

### Running the example

The above example can be found in
[`dev/valkyrie/example.gleam`](dev/valkyrie/example.gleam). and can be run with:

```bash
docker compose --profile valkey up -d
# You can also use profiles 'redis', 'keydb' or 'dragonfly'
gleam run -m valkyrie/example
```

## Why use Valkyrie?

### Performance

Unlike other Redis libraries in the BEAM ecosystem, Valkyrie does not use actors or
gen servers to manage connections. Instead, Valkyrie maintains a pool of TCP connections
to the Redis-compatible database, and uses those directly to execute commands.

This reduces the overhead of using the library, and prevents unnecessary copying of data
between the connection process and the calling process.

This means Valkyrie should be significantly faster than alternatives, especially when
sending or receiving large amounts of data.

### Compatibility

Valkyrie tries to adhere to the Redis command APIs as much as possible, with some minor
changes to make working with Redis-compatible databases a more idiomatic Gleam
experience (e.g. replacing an integer boolean with an actual boolean).

Where possible, Valkyrie uses Gleam's type system to ensure you can't pass invalid
arguments or combinations of arguments that would result in an error, while
remaining as close to the spec as possible.

Commands are generally named after their Redis counterparts, with some minor
additions where adding a specific keyword changes the input or output type of
the command.

All Valkyrie functions are tested against:

- [Redis](https://redis.io/) versions 7 and 8
- [Valkey](https://valkey.io/) versions 7 and 8
- [KeyDB](https://www.keydb.dev/) latest
- [Dragonfly](https://dragonflydb.io/) latest

## Development

In order to run tests for Valkyrie, you'll need to have a Redis-compatible database
running on `localhost:6379`. You can start one using the provided Docker Compose
file:

```bash
docker compose --profile valkey up -d
# You can also use profiles 'redis', 'keydb' or 'dragonfly'
```

If you have the [Mise](https://mise.jdx.dev/) package manager installed, you can run
tests sequentially against all profiles using:

```bash
mise run test
```

This will leave a local Valkey instance running for testing purposes.

## With thanks

Valkyrie builds upon the RESP implementation used in the
[Radish](https://github.com/massivefermion/radish) library for Gleam, which was
licensed under Apache 2.0 as of commit
[6068a05](https://github.com/massivefermion/radish/tree/6068a0525759c2930e6d88ddd04d0d87aada628e).
Thanks to @massivefermion for their work on Radish.

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
  let assert Ok(_) = echo valkyrie.set(conn, "key", "value", option.None, 1000)
  let assert Ok(_) = echo valkyrie.get(conn, "key", 1000)
  // Do more stuff...
}

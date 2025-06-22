import gleam/erlang/process
import gleam/option
import gleam/otp/static_supervisor as supervisor
import valkyrie

pub fn main() {
  // Create a subject to receive the connection once the supervision tree has been
  // started. Use a named subject to make sure we can always receive the connection,
  // even if our original process crashes.
  let conn_receiver_name = process.new_name("valkyrie_conn_receiver")
  let assert Ok(_) = process.register(process.self(), conn_receiver_name)

  let conn_receiver = process.named_subject(conn_receiver_name)

  // Define a pool of 10 connections to the default Redis instance on localhost.
  let valkyrie_child_spec =
    valkyrie.default_config()
    |> valkyrie.supervised_pool(
      receiver: conn_receiver,
      size: 10,
      timeout: 1000,
    )

  // Start the pool under a supervisor
  let assert Ok(_started) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(valkyrie_child_spec)
    |> supervisor.start

  // Receive the connection now that the pool is started
  let assert Ok(conn) = process.receive(conn_receiver, 1000)

  // Use the connection.
  let assert Ok(_) = echo valkyrie.set(conn, "key", "value", option.None, 1000)
  let assert Ok(_) = echo valkyrie.get(conn, "key", 1000)
  // Do more stuff...
}

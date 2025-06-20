import gleam/option
import valkyrie

import gleeunit

pub fn main() {
  gleeunit.main()
}

fn get_test_client(next) {
  let assert Ok(client) =
    valkyrie.default_config()
    |> valkyrie.create_connection(128)

  let res = next(client)
  let assert Ok(_) = valkyrie.shutdown(client)
  res
}

pub fn roundtrip_test() {
  use client <- get_test_client()
  let assert Ok("OK") =
    client
    |> valkyrie.set("key", "value", option.None, 1000)

  let assert Ok("value") = client |> valkyrie.get("key", 1000)
}

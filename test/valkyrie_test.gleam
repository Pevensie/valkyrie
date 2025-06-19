import valkyrie

import gleeunit

pub fn main() {
  gleeunit.main()
}

fn get_test_client(next) {
  let assert Ok(client) =
    valkyrie.default_config()
    |> valkyrie.timeout(128)
    |> valkyrie.start

  let res = next(client)
  let assert Ok(_) = valkyrie.shutdown(client)
  res
}

pub fn roundtrip_test() {
  use client <- get_test_client()
  let assert Ok(_) =
    client
    |> valkyrie.set("key", "value", 1000)

  let assert Ok("value") = client |> valkyrie.get("key", 1000)
}

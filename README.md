![valkyrie](https://raw.githubusercontent.com/Pevensie/valkyrie/main/banner.jpg)

[![Package Version](https://img.shields.io/hexpm/v/valkyrie)](https://hex.pm/packages/valkyrie)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/valkyrie/)

# valkyrie

A Gleam client for Valkey, KeyDB, Redis and other tools with compatible APIs

## <img width=32 src="https://raw.githubusercontent.com/Pevensie/valkyrie/main/icon.png"> Quick start

```sh
gleam test  # Run the tests
gleam shell # Run an Erlang shell
```

## <img width=32 src="https://raw.githubusercontent.com/Pevensie/valkyrie/main/icon.png"> Installation

```sh
gleam add valkyrie
```

## <img width=32 src="https://raw.githubusercontent.com/Pevensie/valkyrie/main/icon.png"> Usage

```gleam
import valkyrie
import valkyrie/list

pub fn main() {
  let assert Ok(client) =
    valkyrie.default_config()
    |> valkyrie.auth(valkyrie.PasswordOnly("password"))
    |> valkyrie.start

  valkyrie.set(client, "requests", "64", 128)
  valkyrie.expire(client, "requests", 60, 128)
  valkyrie.decr(client, "requests", 128)

  list.lpush(
    client,
    "names",
    ["Gary", "Andy", "Nicholas", "Danny", "Shaun", "Ed"],
    128,
  )
  list.lpop(client, "names", 128)
}
```

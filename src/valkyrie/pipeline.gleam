import gleam/list
import gleam/option
import valkyrie.{type Connection, type KeyType, execute_bits, key_type_to_string}
import valkyrie/internal/commands
import valkyrie/resp

pub opaque type Pipeline {
  Pipeline(
    /// A list of commands in REVERSE order
    commands: List(List(String)),
  )
}

pub fn new() -> Pipeline {
  Pipeline(commands: [])
}

pub fn exec(pipeline: Pipeline, conn: Connection, timeout: Int) {
  execute_bits(
    conn,
    list.reverse(pipeline.commands) |> resp.encode_pipeline,
    timeout,
  )
}

pub fn keys(pipeline: Pipeline, pattern: String) -> Pipeline {
  Pipeline(commands: [commands.keys(pattern), ..pipeline.commands])
}

pub fn scan(
  pipeline: Pipeline,
  cursor: Int,
  pattern_filter: option.Option(String),
  count: Int,
  key_type_filter: option.Option(KeyType),
) {
  let command =
    commands.scan(
      cursor,
      pattern_filter,
      count,
      option.map(key_type_filter, key_type_to_string),
    )
  Pipeline(commands: [command, ..pipeline.commands])
}

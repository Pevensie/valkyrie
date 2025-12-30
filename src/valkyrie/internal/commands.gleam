import gleam/int
import gleam/option

pub fn keys(pattern: String) -> List(String) {
  ["KEYS", pattern]
}

pub fn scan(
  cursor: Int,
  pattern_filter: option.Option(String),
  count: Int,
  key_type_filter: option.Option(String),
) {
  let modifiers = case key_type_filter {
    option.Some(key_type) -> ["TYPE", key_type]
    option.None -> []
  }
  let modifiers = ["COUNT", int.to_string(count), ..modifiers]
  let modifiers = case pattern_filter {
    option.Some(pattern) -> ["MATCH", pattern, ..modifiers]
    option.None -> modifiers
  }
  ["SCAN", int.to_string(cursor), ..modifiers]
}

import gleam/int
import gleam/list

import valkyrie/internal/protocol

pub fn add(key: String, values: List(String)) {
  ["SADD", key]
  |> list.append(values)
  |> protocol.encode_command
}

pub fn card(key: String) {
  ["SCARD", key]
  |> protocol.encode_command
}

pub fn is_member(key: String, value: String) {
  ["SISMEMBER", key, value]
  |> protocol.encode_command
}

pub fn members(key: String) {
  ["SMEMBERS", key]
  |> protocol.encode_command
}

pub fn scan(key: String, cursor: Int, count: Int) {
  ["SSCAN", key, int.to_string(cursor), "COUNT", int.to_string(count)]
  |> protocol.encode_command
}

pub fn scan_pattern(key: String, cursor: Int, pattern: String, count: Int) {
  [
    "SSCAN",
    key,
    int.to_string(cursor),
    "MATCH",
    pattern,
    "COUNT",
    int.to_string(count),
  ]
  |> protocol.encode_command
}

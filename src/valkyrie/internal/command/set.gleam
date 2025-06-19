import gleam/int
import gleam/list

import valkyrie/internal/command

pub fn add(key: String, values: List(String)) {
  ["SADD", key]
  |> list.append(values)
  |> command.prepare
}

pub fn card(key: String) {
  ["SCARD", key]
  |> command.prepare
}

pub fn is_member(key: String, value: String) {
  ["SISMEMBER", key, value]
  |> command.prepare
}

pub fn members(key: String) {
  ["SMEMBERS", key]
  |> command.prepare
}

pub fn scan(key: String, cursor: Int, count: Int) {
  ["SSCAN", key, int.to_string(cursor), "COUNT", int.to_string(count)]
  |> command.prepare
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
  |> command.prepare
}

import gleam/float
import gleam/int
import gleam/list
import gleam/option

import valkyrie/internal/command

pub fn lpush(key: String, values: List(String)) {
  ["LPUSH", key]
  |> list.append(values)
  |> command.prepare
}

pub fn rpush(key: String, values: List(String)) {
  ["RPUSH", key]
  |> list.append(values)
  |> command.prepare
}

pub fn lpushx(key: String, values: List(String)) {
  ["LPUSHX", key]
  |> list.append(values)
  |> command.prepare
}

pub fn rpushx(key: String, values: List(String)) {
  ["RPUSHX", key]
  |> list.append(values)
  |> command.prepare
}

pub fn len(key: String) {
  ["LLEN", key]
  |> command.prepare
}

pub fn range(key: String, start: Int, end: Int) {
  ["LRANGE", key, int.to_string(start), int.to_string(end)]
  |> command.prepare
}

pub fn lpop(key: String, count: option.Option(Int)) {
  case count {
    option.None -> ["LPOP", key]
    option.Some(count) -> ["LPOP", key, int.to_string(count)]
  }
  |> command.prepare
}

pub fn blpop(keys: List(String), timeout: Int) {
  let assert Ok(timeout) =
    timeout
    |> int.to_float
    |> float.divide(1000.0)

  ["BLPOP"]
  |> list.append(keys)
  |> list.append([
    timeout
    |> float.round
    |> int.to_string,
  ])
  |> command.prepare
}

pub fn rpop(key: String, count: option.Option(Int)) {
  case count {
    option.None -> ["RPOP", key]
    option.Some(count) -> ["RPOP", key, int.to_string(count)]
  }
  |> command.prepare
}

pub fn brpop(keys: List(String), timeout: Int) {
  let assert Ok(timeout) =
    timeout
    |> int.to_float
    |> float.divide(1000.0)

  ["BRPOP"]
  |> list.append(keys)
  |> list.append([
    timeout
    |> float.round
    |> int.to_string,
  ])
  |> command.prepare
}

pub fn index(key: String, index: Int) {
  ["LINDEX", key, int.to_string(index)]
  |> command.prepare
}

pub fn rem(key: String, count: Int, value: String) {
  ["LREM", key, int.to_string(count), value]
  |> command.prepare
}

pub fn set(key: String, index: Int, value: String) {
  ["LSET", key, int.to_string(index), value]
  |> command.prepare
}

pub fn insert_after(key: String, pivot: String, value: String) {
  ["LINSERT", key, "AFTER", pivot, value]
  |> command.prepare
}

pub fn insert_before(key: String, pivot: String, value: String) {
  ["LINSERT", key, "BEFORE", pivot, value]
  |> command.prepare
}

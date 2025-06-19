import gleam/float
import gleam/int
import gleam/list

import valkyrie/internal/command

pub fn set(key: String, fields: List(#(String, String))) {
  ["HSET", key]
  |> list.append(list.flat_map(fields, fn(kv) { [kv.0, kv.1] }))
  |> command.prepare
}

pub fn set_new(key: String, field: String, value: String) {
  ["HSETNX", key, field, value]
  |> command.prepare
}

pub fn keys(key: String) {
  ["HKEYS", key]
  |> command.prepare
}

pub fn len(key: String) {
  ["HLEN", key]
  |> command.prepare
}

pub fn get(key: String, field: String) {
  ["HGET", key, field]
  |> command.prepare
}

pub fn get_all(key: String) {
  ["HGETALL", key]
  |> command.prepare
}

pub fn mget(key: String, fields: List(String)) {
  ["HMGET", key]
  |> list.append(fields)
  |> command.prepare
}

pub fn strlen(key: String, field: String) {
  ["HSTRLEN", key, field]
  |> command.prepare
}

pub fn vals(key: String) {
  ["HVALS", key]
  |> command.prepare
}

pub fn del(key: String, fields: List(String)) {
  ["HDEL", key]
  |> list.append(fields)
  |> command.prepare
}

pub fn exists(key: String, field: String) {
  ["HEXISTS", key, field]
  |> command.prepare
}

pub fn incr_by(key: String, field: String, value: Int) {
  ["HINCRBY", key, field, int.to_string(value)]
  |> command.prepare
}

pub fn incr_by_float(key: String, field: String, value: Float) {
  ["HINCRBYFLOAT", key, field, float.to_string(value)]
  |> command.prepare
}

pub fn scan(key: String, cursor: Int, count: Int) {
  ["HSCAN", key, int.to_string(cursor), "COUNT", int.to_string(count)]
  |> command.prepare
}

pub fn scan_pattern(key: String, cursor: Int, pattern: String, count: Int) {
  [
    "HSCAN",
    key,
    int.to_string(cursor),
    "MATCH",
    pattern,
    "COUNT",
    int.to_string(count),
  ]
  |> command.prepare
}

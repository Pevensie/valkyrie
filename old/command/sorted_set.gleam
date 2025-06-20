import gleam/int
import gleam/list

import valkyrie/internal/protocol

pub fn add_new(key: String, members: List(#(String, String))) {
  ["ZADD", key, "NX", "CH"]
  |> list.append(list.map(members, fn(member) { member.1 <> member.0 }))
  |> protocol.encode_command
}

pub fn upsert(key: String, members: List(#(String, String))) {
  ["ZADD", key, "CH"]
  |> list.append(list.map(members, fn(member) { member.1 <> member.0 }))
  |> protocol.encode_command
}

pub fn upsert_only_lower_scores(key: String, members: List(#(String, String))) {
  ["ZADD", key, "LT", "CH"]
  |> list.append(list.map(members, fn(member) { member.1 <> member.0 }))
  |> protocol.encode_command
}

pub fn upsert_only_higher_scores(key: String, members: List(#(String, String))) {
  ["ZADD", key, "GT", "CH"]
  |> list.append(list.map(members, fn(member) { member.1 <> member.0 }))
  |> protocol.encode_command
}

pub fn update(key: String, members: List(#(String, String))) {
  ["ZADD", key, "XX", "CH"]
  |> list.append(list.map(members, fn(member) { member.1 <> member.0 }))
  |> protocol.encode_command
}

pub fn update_only_lower_scores(key: String, members: List(#(String, String))) {
  ["ZADD", key, "XX", "LT", "CH"]
  |> list.append(list.map(members, fn(member) { member.1 <> member.0 }))
  |> protocol.encode_command
}

pub fn update_only_higher_scores(key: String, members: List(#(String, String))) {
  ["ZADD", key, "XX", "GT", "CH"]
  |> list.append(list.map(members, fn(member) { member.1 <> member.0 }))
  |> protocol.encode_command
}

pub fn incr_by(key: String, member: String, change_in_score: String) {
  ["ZINCRBY", key, change_in_score, member]
  |> protocol.encode_command
}

pub fn card(key: String) {
  ["ZCARD", key]
  |> protocol.encode_command
}

pub fn count(key: String, min: String, max: String) {
  ["ZCOUNT", key, min, max]
  |> protocol.encode_command
}

pub fn score(key: String, member: String) {
  ["ZSCORE", key, member]
  |> protocol.encode_command
}

pub fn scan(key: String, cursor: Int, count: Int) {
  ["ZSCAN", key, int.to_string(cursor), "COUNT", int.to_string(count)]
  |> protocol.encode_command
}

pub fn scan_pattern(key: String, cursor: Int, pattern: String, count: Int) {
  [
    "ZSCAN",
    key,
    int.to_string(cursor),
    "MATCH",
    pattern,
    "COUNT",
    int.to_string(count),
  ]
  |> protocol.encode_command
}

pub fn rem(key: String, members: List(String)) {
  ["ZREM", key]
  |> list.append(members)
  |> protocol.encode_command
}

pub fn random_members(key: String, count: Int) {
  ["ZRANDMEMBER", key, int.to_string(count), "WITHSCORES"]
  |> protocol.encode_command
}

pub fn rank(key: String, member: String) {
  ["ZRANK", key, member, "WITHSCORE"]
  |> protocol.encode_command
}

pub fn reverse_rank(key: String, member: String) {
  ["ZREVRANK", key, member, "WITHSCORE"]
  |> protocol.encode_command
}

pub fn pop_min(key: String, count: Int) {
  ["ZPOPMIN", key, int.to_string(count)]
  |> protocol.encode_command
}

pub fn pop_max(key: String, count: Int) {
  ["ZPOPMAX", key, int.to_string(count)]
  |> protocol.encode_command
}

pub fn range(key: String, start: Int, stop: Int) {
  ["ZRANGE", key, int.to_string(start), int.to_string(stop), "WITHSCORES"]
  |> protocol.encode_command
}

pub fn reverse_range(key: String, start: Int, stop: Int) {
  ["ZREVRANGE", key, int.to_string(start), int.to_string(stop), "WITHSCORES"]
  |> protocol.encode_command
}

pub fn range_by_score(key: String, min: String, max: String) {
  ["ZRANGEBYSCORE", key, min, max, "WITHSCORES"]
  |> protocol.encode_command
}

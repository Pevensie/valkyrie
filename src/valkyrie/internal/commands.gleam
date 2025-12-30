import gleam/dict
import gleam/float
import gleam/int
import gleam/list
import gleam/option

// ---------------------------- //
// ----- Scalar functions ----- //
// ---------------------------- //

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

pub fn exists(keys: List(String)) -> List(String) {
  ["EXISTS", ..keys]
}

pub fn get(key: String) -> List(String) {
  ["GET", key]
}

pub fn mget(keys: List(String)) -> List(String) {
  ["MGET", ..keys]
}

pub fn append(key: String, value: String) -> List(String) {
  ["APPEND", key, value]
}

pub fn set(key: String, value: String, modifiers: List(String)) -> List(String) {
  ["SET", key, value, ..modifiers]
}

pub fn mset(kv_list: List(#(String, String))) -> List(String) {
  let kv_list =
    kv_list
    |> list.map(fn(kv) { [kv.0, kv.1] })
    |> list.flatten
  ["MSET", ..kv_list]
}

pub fn del(keys: List(String)) -> List(String) {
  ["DEL", ..keys]
}

pub fn incr(key: String) -> List(String) {
  ["INCR", key]
}

pub fn incrby(key: String, value: Int) -> List(String) {
  ["INCRBY", key, int.to_string(value)]
}

pub fn incrbyfloat(key: String, value: Float) -> List(String) {
  ["INCRBYFLOAT", key, float.to_string(value)]
}

pub fn decr(key: String) -> List(String) {
  ["DECR", key]
}

pub fn decrby(key: String, value: Int) -> List(String) {
  ["DECRBY", key, int.to_string(value)]
}

pub fn randomkey() -> List(String) {
  ["RANDOMKEY"]
}

pub fn key_type(key: String) -> List(String) {
  ["TYPE", key]
}

pub fn rename(key: String, new_key: String) -> List(String) {
  ["RENAME", key, new_key]
}

pub fn renamenx(key: String, new_key: String) -> List(String) {
  ["RENAMENX", key, new_key]
}

pub fn persist(key: String) -> List(String) {
  ["PERSIST", key]
}

pub fn ping(message: option.Option(String)) -> List(String) {
  case message {
    option.None -> ["PING"]
    option.Some(msg) -> ["PING", msg]
  }
}

pub fn expire(
  key: String,
  ttl: Int,
  expiry_condition: List(String),
) -> List(String) {
  ["EXPIRE", key, int.to_string(ttl), ..expiry_condition]
}

pub fn pexpire(
  key: String,
  ttl: Int,
  expiry_condition: List(String),
) -> List(String) {
  ["PEXPIRE", key, int.to_string(ttl), ..expiry_condition]
}

pub fn expireat(
  key: String,
  unix_seconds: Int,
  expiry_condition: List(String),
) -> List(String) {
  ["EXPIREAT", key, int.to_string(unix_seconds), ..expiry_condition]
}

pub fn expiretime(key: String) -> List(String) {
  ["EXPIRETIME", key]
}

pub fn pexpiretime(key: String) -> List(String) {
  ["PEXPIRETIME", key]
}

// -------------------------- //
// ----- List functions ----- //
// -------------------------- //

pub fn lpush(key: String, values: List(String)) -> List(String) {
  ["LPUSH", key, ..values]
}

pub fn rpush(key: String, values: List(String)) -> List(String) {
  ["RPUSH", key, ..values]
}

pub fn lpushx(key: String, values: List(String)) -> List(String) {
  ["LPUSHX", key, ..values]
}

pub fn rpushx(key: String, values: List(String)) -> List(String) {
  ["RPUSHX", key, ..values]
}

pub fn llen(key: String) -> List(String) {
  ["LLEN", key]
}

pub fn lrange(key: String, start: Int, end: Int) -> List(String) {
  ["LRANGE", key, int.to_string(start), int.to_string(end)]
}

pub fn lpop(key: String, count: Int) -> List(String) {
  ["LPOP", key, int.to_string(count)]
}

pub fn rpop(key: String, count: Int) -> List(String) {
  ["RPOP", key, int.to_string(count)]
}

pub fn lindex(key: String, index: Int) -> List(String) {
  ["LINDEX", key, int.to_string(index)]
}

pub fn lrem(key: String, count: Int, value: String) -> List(String) {
  ["LREM", key, int.to_string(count), value]
}

pub fn lset(key: String, index: Int, value: String) -> List(String) {
  ["LSET", key, int.to_string(index), value]
}

pub fn linsert(
  key: String,
  position: String,
  pivot: String,
  value: String,
) -> List(String) {
  ["LINSERT", key, position, pivot, value]
}

// ------------------------- //
// ----- Set functions ----- //
// ------------------------- //

pub fn sadd(key: String, values: List(String)) -> List(String) {
  ["SADD", key, ..values]
}

pub fn scard(key: String) -> List(String) {
  ["SCARD", key]
}

pub fn sismember(key: String, value: String) -> List(String) {
  ["SISMEMBER", key, value]
}

pub fn smembers(key: String) -> List(String) {
  ["SMEMBERS", key]
}

pub fn sscan(
  key: String,
  cursor: Int,
  pattern_filter: option.Option(String),
  count: Int,
) -> List(String) {
  let modifiers = case pattern_filter {
    option.Some(pattern) -> ["MATCH", pattern, "COUNT", int.to_string(count)]
    option.None -> ["COUNT", int.to_string(count)]
  }
  ["SSCAN", key, int.to_string(cursor), ..modifiers]
}

// -------------------------------- //
// ----- Sorted set functions ----- //
// -------------------------------- //

pub fn zadd(key: String, modifiers_and_members: List(String)) -> List(String) {
  ["ZADD", key, ..modifiers_and_members]
}

pub fn zincrby(key: String, delta: String, member: String) -> List(String) {
  ["ZINCRBY", key, delta, member]
}

pub fn zcard(key: String) -> List(String) {
  ["ZCARD", key]
}

pub fn zcount(key: String, min: String, max: String) -> List(String) {
  ["ZCOUNT", key, min, max]
}

pub fn zscore(key: String, member: String) -> List(String) {
  ["ZSCORE", key, member]
}

pub fn zscan(
  key: String,
  cursor: Int,
  pattern_filter: option.Option(String),
  count: Int,
) -> List(String) {
  let modifiers = case pattern_filter {
    option.Some(pattern) -> ["MATCH", pattern, "COUNT", int.to_string(count)]
    option.None -> ["COUNT", int.to_string(count)]
  }
  ["ZSCAN", key, int.to_string(cursor), ..modifiers]
}

pub fn zrem(key: String, members: List(String)) -> List(String) {
  ["ZREM", key, ..members]
}

pub fn zrandmember(key: String, count: Int) -> List(String) {
  ["ZRANDMEMBER", key, int.to_string(count), "WITHSCORES"]
}

pub fn zpopmin(key: String, count: Int) -> List(String) {
  ["ZPOPMIN", key, int.to_string(count)]
}

pub fn zpopmax(key: String, count: Int) -> List(String) {
  ["ZPOPMAX", key, int.to_string(count)]
}

pub fn zrange(
  key: String,
  start: String,
  stop: String,
  modifiers: List(String),
) -> List(String) {
  ["ZRANGE", key, start, stop, ..modifiers]
}

pub fn zrank(key: String, member: String) -> List(String) {
  ["ZRANK", key, member]
}

pub fn zrank_withscore(key: String, member: String) -> List(String) {
  ["ZRANK", key, member, "WITHSCORE"]
}

pub fn zrevrank(key: String, member: String) -> List(String) {
  ["ZREVRANK", key, member]
}

pub fn zrevrank_withscore(key: String, member: String) -> List(String) {
  ["ZREVRANK", key, member, "WITHSCORE"]
}

// -------------------------- //
// ----- Hash functions ----- //
// -------------------------- //

pub fn hset(key: String, values: dict.Dict(String, String)) -> List(String) {
  let values =
    values
    |> dict.to_list
    |> list.flat_map(fn(item) { [item.0, item.1] })
  ["HSET", key, ..values]
}

pub fn hsetnx(key: String, field: String, value: String) -> List(String) {
  ["HSETNX", key, field, value]
}

pub fn hlen(key: String) -> List(String) {
  ["HLEN", key]
}

pub fn hkeys(key: String) -> List(String) {
  ["HKEYS", key]
}

pub fn hget(key: String, field: String) -> List(String) {
  ["HGET", key, field]
}

pub fn hgetall(key: String) -> List(String) {
  ["HGETALL", key]
}

pub fn hmget(key: String, fields: List(String)) -> List(String) {
  ["HMGET", key, ..fields]
}

pub fn hstrlen(key: String, field: String) -> List(String) {
  ["HSTRLEN", key, field]
}

pub fn hvals(key: String) -> List(String) {
  ["HVALS", key]
}

pub fn hdel(key: String, fields: List(String)) -> List(String) {
  ["HDEL", key, ..fields]
}

pub fn hexists(key: String, field: String) -> List(String) {
  ["HEXISTS", key, field]
}

pub fn hincrby(key: String, field: String, value: Int) -> List(String) {
  ["HINCRBY", key, field, int.to_string(value)]
}

pub fn hincrbyfloat(key: String, field: String, value: Float) -> List(String) {
  ["HINCRBYFLOAT", key, field, float.to_string(value)]
}

pub fn hscan(
  key: String,
  cursor: Int,
  pattern_filter: option.Option(String),
  count: Int,
) -> List(String) {
  let modifiers = case pattern_filter {
    option.Some(pattern) -> ["MATCH", pattern, "COUNT", int.to_string(count)]
    option.None -> ["COUNT", int.to_string(count)]
  }
  ["HSCAN", key, int.to_string(cursor), ..modifiers]
}

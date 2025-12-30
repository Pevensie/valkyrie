import gleam/dict
import gleam/float
import gleam/int
import gleam/list
import gleam/option
import gleam/result
import valkyrie.{
  type Connection, type ExpireCondition, type InsertPosition, type KeyType,
  type LexBound, type NumericBound, type Score, type SetOptions,
  type ZAddCondition, RespError, execute_bits, key_type_to_string,
}
import valkyrie/internal/commands
import valkyrie/resp

pub opaque type Pipeline {
  Pipeline(
    /// A list of commands in REVERSE order
    commands: List(List(String)),
  )
}

/// Create a new empty pipeline.
pub fn new() -> Pipeline {
  Pipeline(commands: [])
}

/// Execute all commands in the pipeline and return the results.
///
/// Returns a list of `resp.Value` for each command in the order they were added.
/// Returns `Ok([])` immediately if the pipeline is empty.
pub fn exec(pipeline: Pipeline, conn: Connection, timeout: Int) {
  case pipeline.commands {
    [] -> Ok([])
    _ ->
      execute_bits(
        conn,
        list.reverse(pipeline.commands) |> resp.encode_pipeline,
        timeout,
      )
  }
}

/// Execute commands in the pipeline in a Redis transaction with MULTI and EXEC.
///
/// Returns a list of `resp.Value` for each command in the order they were added.
/// Returns `Ok([])` immediately if the pipeline is empty.
pub fn exec_transaction(pipeline: Pipeline, conn: Connection, timeout: Int) {
  case pipeline.commands {
    [] -> Ok([])
    _ -> {
      let with_exec = [["EXEC"], ..pipeline.commands]
      let with_multi = [["MULTI"], ..list.reverse(with_exec)]
      use results <- result.try(execute_bits(
        conn,
        resp.encode_pipeline(with_multi),
        timeout,
      ))

      case list.last(results) {
        Ok(resp.Array(exec_results)) -> Ok(exec_results)
        _ -> Error(RespError("Expected EXEC response to be an array"))
      }
    }
  }
}

// ---------------------------- //
// ----- Scalar functions ----- //
// ---------------------------- //

/// Adds KEYS to the pipeline. See [`valkyrie.keys`](#keys) for more details.
pub fn keys(pipeline: Pipeline, pattern: String) -> Pipeline {
  Pipeline(commands: [commands.keys(pattern), ..pipeline.commands])
}

/// Adds SCAN to the pipeline. See [`valkyrie.scan`](#scan) for more details.
pub fn scan(
  pipeline: Pipeline,
  cursor: Int,
  pattern_filter: option.Option(String),
  count: Int,
  key_type_filter: option.Option(KeyType),
) -> Pipeline {
  let command =
    commands.scan(
      cursor,
      pattern_filter,
      count,
      option.map(key_type_filter, key_type_to_string),
    )
  Pipeline(commands: [command, ..pipeline.commands])
}

/// Adds EXISTS to the pipeline. See [`valkyrie.exists`](#exists) for more details.
pub fn exists(pipeline: Pipeline, keys: List(String)) -> Pipeline {
  Pipeline(commands: [commands.exists(keys), ..pipeline.commands])
}

/// Adds GET to the pipeline. See [`valkyrie.get`](#get) for more details.
pub fn get(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.get(key), ..pipeline.commands])
}

/// Adds MGET to the pipeline. See [`valkyrie.mget`](#mget) for more details.
pub fn mget(pipeline: Pipeline, keys: List(String)) -> Pipeline {
  Pipeline(commands: [commands.mget(keys), ..pipeline.commands])
}

/// Adds APPEND to the pipeline. See [`valkyrie.append`](#append) for more details.
pub fn append(pipeline: Pipeline, key: String, value: String) -> Pipeline {
  Pipeline(commands: [commands.append(key, value), ..pipeline.commands])
}

/// Adds SET to the pipeline. See [`valkyrie.set`](#set) for more details.
pub fn set(
  pipeline: Pipeline,
  key: String,
  value: String,
  options: option.Option(SetOptions),
) -> Pipeline {
  let modifiers = set_options_to_modifiers(options)
  Pipeline(commands: [commands.set(key, value, modifiers), ..pipeline.commands])
}

fn set_options_to_modifiers(options: option.Option(SetOptions)) -> List(String) {
  options
  |> option.map(fn(options) {
    let modifiers = case options.expiry_option {
      option.Some(valkyrie.KeepTtl) -> ["KEEPTTL"]
      option.Some(valkyrie.ExpirySeconds(value)) -> ["EX", int.to_string(value)]
      option.Some(valkyrie.ExpiryMilliseconds(value)) -> [
        "PX",
        int.to_string(value),
      ]
      option.Some(valkyrie.ExpiresAtUnixSeconds(value)) -> [
        "EXAT",
        int.to_string(value),
      ]
      option.Some(valkyrie.ExpiresAtUnixMilliseconds(value)) -> [
        "PXAT",
        int.to_string(value),
      ]
      option.None -> []
    }
    let modifiers = case options.return_old {
      True -> ["GET", ..modifiers]
      False -> modifiers
    }
    let modifiers = case options.existence_condition {
      option.Some(valkyrie.IfNotExists) -> ["NX", ..modifiers]
      option.Some(valkyrie.IfExists) -> ["XX", ..modifiers]
      option.None -> modifiers
    }
    modifiers
  })
  |> option.unwrap([])
}

/// Adds MSET to the pipeline. See [`valkyrie.mset`](#mset) for more details.
pub fn mset(pipeline: Pipeline, kv_list: List(#(String, String))) -> Pipeline {
  Pipeline(commands: [commands.mset(kv_list), ..pipeline.commands])
}

/// Adds DEL to the pipeline. See [`valkyrie.del`](#del) for more details.
pub fn del(pipeline: Pipeline, keys: List(String)) -> Pipeline {
  Pipeline(commands: [commands.del(keys), ..pipeline.commands])
}

/// Adds INCR to the pipeline. See [`valkyrie.incr`](#incr) for more details.
pub fn incr(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.incr(key), ..pipeline.commands])
}

/// Adds INCRBY to the pipeline. See [`valkyrie.incrby`](#incrby) for more details.
pub fn incrby(pipeline: Pipeline, key: String, value: Int) -> Pipeline {
  Pipeline(commands: [commands.incrby(key, value), ..pipeline.commands])
}

/// Adds INCRBYFLOAT to the pipeline. See [`valkyrie.incrbyfloat`](#incrbyfloat) for more details.
pub fn incrbyfloat(pipeline: Pipeline, key: String, value: Float) -> Pipeline {
  Pipeline(commands: [commands.incrbyfloat(key, value), ..pipeline.commands])
}

/// Adds DECR to the pipeline. See [`valkyrie.decr`](#decr) for more details.
pub fn decr(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.decr(key), ..pipeline.commands])
}

/// Adds DECRBY to the pipeline. See [`valkyrie.decrby`](#decrby) for more details.
pub fn decrby(pipeline: Pipeline, key: String, value: Int) -> Pipeline {
  Pipeline(commands: [commands.decrby(key, value), ..pipeline.commands])
}

/// Adds RANDOMKEY to the pipeline. See [`valkyrie.randomkey`](#randomkey) for more details.
pub fn randomkey(pipeline: Pipeline) -> Pipeline {
  Pipeline(commands: [commands.randomkey(), ..pipeline.commands])
}

/// Adds TYPE to the pipeline. See [`valkyrie.key_type`](#key_type) for more details.
pub fn key_type(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.key_type(key), ..pipeline.commands])
}

/// Adds RENAME to the pipeline. See [`valkyrie.rename`](#rename) for more details.
pub fn rename(pipeline: Pipeline, key: String, new_key: String) -> Pipeline {
  Pipeline(commands: [commands.rename(key, new_key), ..pipeline.commands])
}

/// Adds RENAMENX to the pipeline. See [`valkyrie.renamenx`](#renamenx) for more details.
pub fn renamenx(pipeline: Pipeline, key: String, new_key: String) -> Pipeline {
  Pipeline(commands: [commands.renamenx(key, new_key), ..pipeline.commands])
}

/// Adds PERSIST to the pipeline. See [`valkyrie.persist`](#persist) for more details.
pub fn persist(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.persist(key), ..pipeline.commands])
}

/// Adds PING to the pipeline. See [`valkyrie.ping`](#ping) for more details.
pub fn ping(pipeline: Pipeline, message: option.Option(String)) -> Pipeline {
  Pipeline(commands: [commands.ping(message), ..pipeline.commands])
}

/// Adds EXPIRE to the pipeline. See [`valkyrie.expire`](#expire) for more details.
pub fn expire(
  pipeline: Pipeline,
  key: String,
  ttl: Int,
  condition: option.Option(ExpireCondition),
) -> Pipeline {
  let expiry_condition = expire_condition_to_modifiers(condition)
  Pipeline(commands: [
    commands.expire(key, ttl, expiry_condition),
    ..pipeline.commands
  ])
}

fn expire_condition_to_modifiers(
  condition: option.Option(ExpireCondition),
) -> List(String) {
  case condition {
    option.Some(valkyrie.IfNoExpiry) -> ["NX"]
    option.Some(valkyrie.IfHasExpiry) -> ["XX"]
    option.Some(valkyrie.IfGreaterThan) -> ["GT"]
    option.Some(valkyrie.IfLessThan) -> ["LT"]
    option.None -> []
  }
}

/// Adds PEXPIRE to the pipeline. See [`valkyrie.pexpire`](#pexpire) for more details.
pub fn pexpire(
  pipeline: Pipeline,
  key: String,
  ttl: Int,
  condition: option.Option(ExpireCondition),
) -> Pipeline {
  let expiry_condition = expire_condition_to_modifiers(condition)
  Pipeline(commands: [
    commands.pexpire(key, ttl, expiry_condition),
    ..pipeline.commands
  ])
}

/// Adds EXPIREAT to the pipeline. See [`valkyrie.expireat`](#expireat) for more details.
pub fn expireat(
  pipeline: Pipeline,
  key: String,
  unix_seconds: Int,
  condition: option.Option(ExpireCondition),
) -> Pipeline {
  let expiry_condition = expire_condition_to_modifiers(condition)
  Pipeline(commands: [
    commands.expireat(key, unix_seconds, expiry_condition),
    ..pipeline.commands
  ])
}

/// Adds EXPIRETIME to the pipeline. See [`valkyrie.expiretime`](#expiretime) for more details.
pub fn expiretime(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.expiretime(key), ..pipeline.commands])
}

/// Adds PEXPIRETIME to the pipeline. See [`valkyrie.pexpiretime`](#pexpiretime) for more details.
pub fn pexpiretime(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.pexpiretime(key), ..pipeline.commands])
}

// -------------------------- //
// ----- List functions ----- //
// -------------------------- //

/// Adds LPUSH to the pipeline. See [`valkyrie.lpush`](#lpush) for more details.
pub fn lpush(pipeline: Pipeline, key: String, values: List(String)) -> Pipeline {
  Pipeline(commands: [commands.lpush(key, values), ..pipeline.commands])
}

/// Adds RPUSH to the pipeline. See [`valkyrie.rpush`](#rpush) for more details.
pub fn rpush(pipeline: Pipeline, key: String, values: List(String)) -> Pipeline {
  Pipeline(commands: [commands.rpush(key, values), ..pipeline.commands])
}

/// Adds LPUSHX to the pipeline. See [`valkyrie.lpushx`](#lpushx) for more details.
pub fn lpushx(pipeline: Pipeline, key: String, values: List(String)) -> Pipeline {
  Pipeline(commands: [commands.lpushx(key, values), ..pipeline.commands])
}

/// Adds RPUSHX to the pipeline. See [`valkyrie.rpushx`](#rpushx) for more details.
pub fn rpushx(pipeline: Pipeline, key: String, values: List(String)) -> Pipeline {
  Pipeline(commands: [commands.rpushx(key, values), ..pipeline.commands])
}

/// Adds LLEN to the pipeline. See [`valkyrie.llen`](#llen) for more details.
pub fn llen(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.llen(key), ..pipeline.commands])
}

/// Adds LRANGE to the pipeline. See [`valkyrie.lrange`](#lrange) for more details.
pub fn lrange(pipeline: Pipeline, key: String, start: Int, end: Int) -> Pipeline {
  Pipeline(commands: [commands.lrange(key, start, end), ..pipeline.commands])
}

/// Adds LPOP to the pipeline. See [`valkyrie.lpop`](#lpop) for more details.
pub fn lpop(pipeline: Pipeline, key: String, count: Int) -> Pipeline {
  Pipeline(commands: [commands.lpop(key, count), ..pipeline.commands])
}

/// Adds RPOP to the pipeline. See [`valkyrie.rpop`](#rpop) for more details.
pub fn rpop(pipeline: Pipeline, key: String, count: Int) -> Pipeline {
  Pipeline(commands: [commands.rpop(key, count), ..pipeline.commands])
}

/// Adds LINDEX to the pipeline. See [`valkyrie.lindex`](#lindex) for more details.
pub fn lindex(pipeline: Pipeline, key: String, index: Int) -> Pipeline {
  Pipeline(commands: [commands.lindex(key, index), ..pipeline.commands])
}

/// Adds LREM to the pipeline. See [`valkyrie.lrem`](#lrem) for more details.
pub fn lrem(
  pipeline: Pipeline,
  key: String,
  count: Int,
  value: String,
) -> Pipeline {
  Pipeline(commands: [commands.lrem(key, count, value), ..pipeline.commands])
}

/// Adds LSET to the pipeline. See [`valkyrie.lset`](#lset) for more details.
pub fn lset(
  pipeline: Pipeline,
  key: String,
  index: Int,
  value: String,
) -> Pipeline {
  Pipeline(commands: [commands.lset(key, index, value), ..pipeline.commands])
}

/// Adds LINSERT to the pipeline. See [`valkyrie.linsert`](#linsert) for more details.
pub fn linsert(
  pipeline: Pipeline,
  key: String,
  position: InsertPosition,
  pivot: String,
  value: String,
) -> Pipeline {
  let position_str = insert_position_to_string(position)
  Pipeline(commands: [
    commands.linsert(key, position_str, pivot, value),
    ..pipeline.commands
  ])
}

fn insert_position_to_string(position: InsertPosition) -> String {
  case position {
    valkyrie.Before -> "BEFORE"
    valkyrie.After -> "AFTER"
  }
}

// ------------------------- //
// ----- Set functions ----- //
// ------------------------- //

/// Adds SADD to the pipeline. See [`valkyrie.sadd`](#sadd) for more details.
pub fn sadd(pipeline: Pipeline, key: String, values: List(String)) -> Pipeline {
  Pipeline(commands: [commands.sadd(key, values), ..pipeline.commands])
}

/// Adds SCARD to the pipeline. See [`valkyrie.scard`](#scard) for more details.
pub fn scard(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.scard(key), ..pipeline.commands])
}

/// Adds SISMEMBER to the pipeline. See [`valkyrie.sismember`](#sismember) for more details.
pub fn sismember(pipeline: Pipeline, key: String, value: String) -> Pipeline {
  Pipeline(commands: [commands.sismember(key, value), ..pipeline.commands])
}

/// Adds SMEMBERS to the pipeline. See [`valkyrie.smembers`](#smembers) for more details.
pub fn smembers(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.smembers(key), ..pipeline.commands])
}

/// Adds SSCAN to the pipeline. See [`valkyrie.sscan`](#sscan) for more details.
pub fn sscan(
  pipeline: Pipeline,
  key: String,
  cursor: Int,
  pattern_filter: option.Option(String),
  count: Int,
) -> Pipeline {
  Pipeline(commands: [
    commands.sscan(key, cursor, pattern_filter, count),
    ..pipeline.commands
  ])
}

// -------------------------------- //
// ----- Sorted set functions ----- //
// -------------------------------- //

/// Adds ZADD to the pipeline. See [`valkyrie.zadd`](#zadd) for more details.
pub fn zadd(
  pipeline: Pipeline,
  key: String,
  members: List(#(String, Score)),
  condition: ZAddCondition,
  return_changed: Bool,
) -> Pipeline {
  let modifiers_and_members =
    zadd_build_modifiers_and_members(members, condition, return_changed)
  Pipeline(commands: [
    commands.zadd(key, modifiers_and_members),
    ..pipeline.commands
  ])
}

fn zadd_build_modifiers_and_members(
  members: List(#(String, Score)),
  condition: ZAddCondition,
  return_changed: Bool,
) -> List(String) {
  let changed_modifier = case return_changed {
    True -> ["CH"]
    False -> []
  }
  let modifiers = case condition {
    valkyrie.IfNotExistsInSet -> ["NX", ..changed_modifier]
    valkyrie.IfExistsInSet -> ["XX", ..changed_modifier]
    valkyrie.IfScoreLessThanExisting -> ["XX", "LT", ..changed_modifier]
    valkyrie.IfScoreGreaterThanExisting -> ["XX", "GT", ..changed_modifier]
  }
  list.append(
    modifiers,
    list.flat_map(members, fn(member) { [score_to_string(member.1), member.0] }),
  )
}

fn score_to_string(score: Score) -> String {
  case score {
    valkyrie.Infinity -> "+inf"
    valkyrie.NegativeInfinity -> "-inf"
    valkyrie.Double(value) -> float.to_string(value)
  }
}

/// Adds ZINCRBY to the pipeline. See [`valkyrie.zincrby`](#zincrby) for more details.
pub fn zincrby(
  pipeline: Pipeline,
  key: String,
  member: String,
  delta: Score,
) -> Pipeline {
  Pipeline(commands: [
    commands.zincrby(key, score_to_string(delta), member),
    ..pipeline.commands
  ])
}

/// Adds ZCARD to the pipeline. See [`valkyrie.zcard`](#zcard) for more details.
pub fn zcard(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.zcard(key), ..pipeline.commands])
}

/// Adds ZCOUNT to the pipeline. See [`valkyrie.zcount`](#zcount) for more details.
pub fn zcount(
  pipeline: Pipeline,
  key: String,
  min: Score,
  max: Score,
) -> Pipeline {
  Pipeline(commands: [
    commands.zcount(key, score_to_string(min), score_to_string(max)),
    ..pipeline.commands
  ])
}

/// Adds ZSCORE to the pipeline. See [`valkyrie.zscore`](#zscore) for more details.
pub fn zscore(pipeline: Pipeline, key: String, member: String) -> Pipeline {
  Pipeline(commands: [commands.zscore(key, member), ..pipeline.commands])
}

/// Adds ZSCAN to the pipeline. See [`valkyrie.zscan`](#zscan) for more details.
pub fn zscan(
  pipeline: Pipeline,
  key: String,
  cursor: Int,
  pattern_filter: option.Option(String),
  count: Int,
) -> Pipeline {
  Pipeline(commands: [
    commands.zscan(key, cursor, pattern_filter, count),
    ..pipeline.commands
  ])
}

/// Adds ZREM to the pipeline. See [`valkyrie.zrem`](#zrem) for more details.
pub fn zrem(pipeline: Pipeline, key: String, members: List(String)) -> Pipeline {
  Pipeline(commands: [commands.zrem(key, members), ..pipeline.commands])
}

/// Adds ZRANDMEMBER to the pipeline. See [`valkyrie.zrandmember`](#zrandmember) for more details.
pub fn zrandmember(pipeline: Pipeline, key: String, count: Int) -> Pipeline {
  Pipeline(commands: [commands.zrandmember(key, count), ..pipeline.commands])
}

/// Adds ZPOPMIN to the pipeline. See [`valkyrie.zpopmin`](#zpopmin) for more details.
pub fn zpopmin(pipeline: Pipeline, key: String, count: Int) -> Pipeline {
  Pipeline(commands: [commands.zpopmin(key, count), ..pipeline.commands])
}

/// Adds ZPOPMAX to the pipeline. See [`valkyrie.zpopmax`](#zpopmax) for more details.
pub fn zpopmax(pipeline: Pipeline, key: String, count: Int) -> Pipeline {
  Pipeline(commands: [commands.zpopmax(key, count), ..pipeline.commands])
}

/// Adds ZRANGE to the pipeline. See [`valkyrie.zrange`](#zrange) for more details.
pub fn zrange(
  pipeline: Pipeline,
  key: String,
  start: NumericBound(Int),
  stop: NumericBound(Int),
  reverse: Bool,
) -> Pipeline {
  let modifiers = case reverse {
    True -> ["REV", "WITHSCORES"]
    False -> ["WITHSCORES"]
  }
  Pipeline(commands: [
    commands.zrange(
      key,
      numeric_bound_to_string(start, int.to_string),
      numeric_bound_to_string(stop, int.to_string),
      modifiers,
    ),
    ..pipeline.commands
  ])
}

fn numeric_bound_to_string(
  bound: NumericBound(a),
  to_string_func: fn(a) -> String,
) -> String {
  case bound {
    valkyrie.NumericInclusive(value) -> to_string_func(value)
    valkyrie.NumericExclusive(value) -> "(" <> to_string_func(value)
  }
}

/// Adds ZRANGE BYSCORE to the pipeline. See [`valkyrie.zrange_byscore`](#zrange_byscore) for more details.
pub fn zrange_byscore(
  pipeline: Pipeline,
  key: String,
  start: NumericBound(Score),
  stop: NumericBound(Score),
  reverse: Bool,
) -> Pipeline {
  let modifiers = case reverse {
    True -> ["BYSCORE", "REV", "WITHSCORES"]
    False -> ["BYSCORE", "WITHSCORES"]
  }
  Pipeline(commands: [
    commands.zrange(
      key,
      numeric_bound_to_string(start, score_to_string),
      numeric_bound_to_string(stop, score_to_string),
      modifiers,
    ),
    ..pipeline.commands
  ])
}

/// Adds ZRANGE BYLEX to the pipeline. See [`valkyrie.zrange_bylex`](#zrange_bylex) for more details.
pub fn zrange_bylex(
  pipeline: Pipeline,
  key: String,
  start: LexBound,
  stop: LexBound,
  reverse: Bool,
) -> Pipeline {
  let modifiers = case reverse {
    True -> ["BYLEX", "REV"]
    False -> ["BYLEX"]
  }
  Pipeline(commands: [
    commands.zrange(
      key,
      lex_bound_to_string(start),
      lex_bound_to_string(stop),
      modifiers,
    ),
    ..pipeline.commands
  ])
}

fn lex_bound_to_string(bound: LexBound) -> String {
  case bound {
    valkyrie.LexInclusive(value) -> "[" <> value
    valkyrie.LexExclusive(value) -> "(" <> value
    valkyrie.LexPositiveInfinity -> "+"
    valkyrie.LexNegativeInfinity -> "-"
  }
}

/// Adds ZRANK to the pipeline. See [`valkyrie.zrank`](#zrank) for more details.
pub fn zrank(pipeline: Pipeline, key: String, member: String) -> Pipeline {
  Pipeline(commands: [commands.zrank(key, member), ..pipeline.commands])
}

/// Adds ZRANK WITHSCORE to the pipeline. See [`valkyrie.zrank_withscore`](#zrank_withscore) for more details.
pub fn zrank_withscore(
  pipeline: Pipeline,
  key: String,
  member: String,
) -> Pipeline {
  Pipeline(commands: [
    commands.zrank_withscore(key, member),
    ..pipeline.commands
  ])
}

/// Adds ZREVRANK to the pipeline. See [`valkyrie.zrevrank`](#zrevrank) for more details.
pub fn zrevrank(pipeline: Pipeline, key: String, member: String) -> Pipeline {
  Pipeline(commands: [commands.zrevrank(key, member), ..pipeline.commands])
}

/// Adds ZREVRANK WITHSCORE to the pipeline. See [`valkyrie.zrevrank_withscore`](#zrevrank_withscore) for more details.
pub fn zrevrank_withscore(
  pipeline: Pipeline,
  key: String,
  member: String,
) -> Pipeline {
  Pipeline(commands: [
    commands.zrevrank_withscore(key, member),
    ..pipeline.commands
  ])
}

// -------------------------- //
// ----- Hash functions ----- //
// -------------------------- //

/// Adds HSET to the pipeline. See [`valkyrie.hset`](#hset) for more details.
pub fn hset(
  pipeline: Pipeline,
  key: String,
  values: dict.Dict(String, String),
) -> Pipeline {
  Pipeline(commands: [commands.hset(key, values), ..pipeline.commands])
}

/// Adds HSETNX to the pipeline. See [`valkyrie.hsetnx`](#hsetnx) for more details.
pub fn hsetnx(
  pipeline: Pipeline,
  key: String,
  field: String,
  value: String,
) -> Pipeline {
  Pipeline(commands: [commands.hsetnx(key, field, value), ..pipeline.commands])
}

/// Adds HLEN to the pipeline. See [`valkyrie.hlen`](#hlen) for more details.
pub fn hlen(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.hlen(key), ..pipeline.commands])
}

/// Adds HKEYS to the pipeline. See [`valkyrie.hkeys`](#hkeys) for more details.
pub fn hkeys(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.hkeys(key), ..pipeline.commands])
}

/// Adds HGET to the pipeline. See [`valkyrie.hget`](#hget) for more details.
pub fn hget(pipeline: Pipeline, key: String, field: String) -> Pipeline {
  Pipeline(commands: [commands.hget(key, field), ..pipeline.commands])
}

/// Adds HGETALL to the pipeline. See [`valkyrie.hgetall`](#hgetall) for more details.
pub fn hgetall(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.hgetall(key), ..pipeline.commands])
}

/// Adds HMGET to the pipeline. See [`valkyrie.hmget`](#hmget) for more details.
pub fn hmget(pipeline: Pipeline, key: String, fields: List(String)) -> Pipeline {
  Pipeline(commands: [commands.hmget(key, fields), ..pipeline.commands])
}

/// Adds HSTRLEN to the pipeline. See [`valkyrie.hstrlen`](#hstrlen) for more details.
pub fn hstrlen(pipeline: Pipeline, key: String, field: String) -> Pipeline {
  Pipeline(commands: [commands.hstrlen(key, field), ..pipeline.commands])
}

/// Adds HVALS to the pipeline. See [`valkyrie.hvals`](#hvals) for more details.
pub fn hvals(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.hvals(key), ..pipeline.commands])
}

/// Adds HDEL to the pipeline. See [`valkyrie.hdel`](#hdel) for more details.
pub fn hdel(pipeline: Pipeline, key: String, fields: List(String)) -> Pipeline {
  Pipeline(commands: [commands.hdel(key, fields), ..pipeline.commands])
}

/// Adds HEXISTS to the pipeline. See [`valkyrie.hexists`](#hexists) for more details.
pub fn hexists(pipeline: Pipeline, key: String, field: String) -> Pipeline {
  Pipeline(commands: [commands.hexists(key, field), ..pipeline.commands])
}

/// Adds HINCRBY to the pipeline. See [`valkyrie.hincrby`](#hincrby) for more details.
pub fn hincrby(
  pipeline: Pipeline,
  key: String,
  field: String,
  value: Int,
) -> Pipeline {
  Pipeline(commands: [commands.hincrby(key, field, value), ..pipeline.commands])
}

/// Adds HINCRBYFLOAT to the pipeline. See [`valkyrie.hincrbyfloat`](#hincrbyfloat) for more details.
pub fn hincrbyfloat(
  pipeline: Pipeline,
  key: String,
  field: String,
  value: Float,
) -> Pipeline {
  Pipeline(commands: [
    commands.hincrbyfloat(key, field, value),
    ..pipeline.commands
  ])
}

/// Adds HSCAN to the pipeline. See [`valkyrie.hscan`](#hscan) for more details.
pub fn hscan(
  pipeline: Pipeline,
  key: String,
  cursor: Int,
  pattern_filter: option.Option(String),
  count: Int,
) -> Pipeline {
  Pipeline(commands: [
    commands.hscan(key, cursor, pattern_filter, count),
    ..pipeline.commands
  ])
}

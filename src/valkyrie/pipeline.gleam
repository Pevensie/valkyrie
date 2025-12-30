import gleam/dict
import gleam/int
import gleam/list
import gleam/option
import gleam/result
import valkyrie
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
///
/// Note: KeyDB wraps pipeline results in an outer array, while Redis returns
/// a flat list. You may need to handle this difference in your application.
pub fn exec(pipeline: Pipeline, conn: valkyrie.Connection, timeout: Int) {
  case pipeline.commands {
    [] -> Ok([])
    _ ->
      valkyrie.execute_bits(
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
pub fn exec_transaction(
  pipeline: Pipeline,
  conn: valkyrie.Connection,
  timeout: Int,
) {
  case pipeline.commands {
    [] -> Ok([])
    _ -> {
      let with_exec = [["EXEC"], ..pipeline.commands]
      let with_multi = [["MULTI"], ..list.reverse(with_exec)]
      use results <- result.try(valkyrie.execute_bits(
        conn,
        resp.encode_pipeline(with_multi),
        timeout,
      ))

      case list.last(results) {
        Ok(resp.Array(exec_results)) -> Ok(exec_results)
        _ -> Error(valkyrie.RespError("Expected EXEC response to be an array"))
      }
    }
  }
}

// ---------------------------- //
// ----- Scalar functions ----- //
// ---------------------------- //

/// Adds KEYS to the pipeline. See [`valkyrie.keys`](/valkyrie.html#keys) for more details.
pub fn keys(pipeline: Pipeline, pattern: String) -> Pipeline {
  Pipeline(commands: [commands.keys(pattern), ..pipeline.commands])
}

/// Adds SCAN to the pipeline. See [`valkyrie.scan`](/valkyrie.html#scan) for more details.
pub fn scan(
  pipeline: Pipeline,
  cursor: Int,
  pattern_filter: option.Option(String),
  count: Int,
  key_type_filter: option.Option(valkyrie.KeyType),
) -> Pipeline {
  let command =
    commands.scan(
      cursor,
      pattern_filter,
      count,
      option.map(key_type_filter, valkyrie.key_type_to_string),
    )
  Pipeline(commands: [command, ..pipeline.commands])
}

/// Adds EXISTS to the pipeline. See [`valkyrie.exists`](/valkyrie.html#exists) for more details.
pub fn exists(pipeline: Pipeline, keys: List(String)) -> Pipeline {
  Pipeline(commands: [commands.exists(keys), ..pipeline.commands])
}

/// Adds GET to the pipeline. See [`valkyrie.get`](/valkyrie.html#get) for more details.
pub fn get(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.get(key), ..pipeline.commands])
}

/// Adds MGET to the pipeline. See [`valkyrie.mget`](/valkyrie.html#mget) for more details.
pub fn mget(pipeline: Pipeline, keys: List(String)) -> Pipeline {
  Pipeline(commands: [commands.mget(keys), ..pipeline.commands])
}

/// Adds APPEND to the pipeline. See [`valkyrie.append`](/valkyrie.html#append) for more details.
pub fn append(pipeline: Pipeline, key: String, value: String) -> Pipeline {
  Pipeline(commands: [commands.append(key, value), ..pipeline.commands])
}

/// Adds SET to the pipeline. See [`valkyrie.set`](/valkyrie.html#set) for more details.
pub fn set(
  pipeline: Pipeline,
  key: String,
  value: String,
  options: option.Option(valkyrie.SetOptions),
) -> Pipeline {
  let modifiers = valkyrie.set_options_to_modifiers(options)
  Pipeline(commands: [commands.set(key, value, modifiers), ..pipeline.commands])
}

/// Adds MSET to the pipeline. See [`valkyrie.mset`](/valkyrie.html#mset) for more details.
pub fn mset(pipeline: Pipeline, kv_list: List(#(String, String))) -> Pipeline {
  Pipeline(commands: [commands.mset(kv_list), ..pipeline.commands])
}

/// Adds DEL to the pipeline. See [`valkyrie.del`](/valkyrie.html#del) for more details.
pub fn del(pipeline: Pipeline, keys: List(String)) -> Pipeline {
  Pipeline(commands: [commands.del(keys), ..pipeline.commands])
}

/// Adds INCR to the pipeline. See [`valkyrie.incr`](/valkyrie.html#incr) for more details.
pub fn incr(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.incr(key), ..pipeline.commands])
}

/// Adds INCRBY to the pipeline. See [`valkyrie.incrby`](/valkyrie.html#incrby) for more details.
pub fn incrby(pipeline: Pipeline, key: String, value: Int) -> Pipeline {
  Pipeline(commands: [commands.incrby(key, value), ..pipeline.commands])
}

/// Adds INCRBYFLOAT to the pipeline. See [`valkyrie.incrbyfloat`](/valkyrie.html#incrbyfloat) for more details.
pub fn incrbyfloat(pipeline: Pipeline, key: String, value: Float) -> Pipeline {
  Pipeline(commands: [commands.incrbyfloat(key, value), ..pipeline.commands])
}

/// Adds DECR to the pipeline. See [`valkyrie.decr`](/valkyrie.html#decr) for more details.
pub fn decr(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.decr(key), ..pipeline.commands])
}

/// Adds DECRBY to the pipeline. See [`valkyrie.decrby`](/valkyrie.html#decrby) for more details.
pub fn decrby(pipeline: Pipeline, key: String, value: Int) -> Pipeline {
  Pipeline(commands: [commands.decrby(key, value), ..pipeline.commands])
}

/// Adds RANDOMKEY to the pipeline. See [`valkyrie.randomkey`](/valkyrie.html#randomkey) for more details.
pub fn randomkey(pipeline: Pipeline) -> Pipeline {
  Pipeline(commands: [commands.randomkey(), ..pipeline.commands])
}

/// Adds TYPE to the pipeline. See [`valkyrie.key_type`](/valkyrie.html#key_type) for more details.
pub fn key_type(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.key_type(key), ..pipeline.commands])
}

/// Adds RENAME to the pipeline. See [`valkyrie.rename`](/valkyrie.html#rename) for more details.
pub fn rename(pipeline: Pipeline, key: String, new_key: String) -> Pipeline {
  Pipeline(commands: [commands.rename(key, new_key), ..pipeline.commands])
}

/// Adds RENAMENX to the pipeline. See [`valkyrie.renamenx`](/valkyrie.html#renamenx) for more details.
pub fn renamenx(pipeline: Pipeline, key: String, new_key: String) -> Pipeline {
  Pipeline(commands: [commands.renamenx(key, new_key), ..pipeline.commands])
}

/// Adds PERSIST to the pipeline. See [`valkyrie.persist`](/valkyrie.html#persist) for more details.
pub fn persist(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.persist(key), ..pipeline.commands])
}

/// Adds PING to the pipeline. See [`valkyrie.ping`](/valkyrie.html#ping) for more details.
pub fn ping(pipeline: Pipeline, message: option.Option(String)) -> Pipeline {
  Pipeline(commands: [commands.ping(message), ..pipeline.commands])
}

/// Adds EXPIRE to the pipeline. See [`valkyrie.expire`](/valkyrie.html#expire) for more details.
pub fn expire(
  pipeline: Pipeline,
  key: String,
  ttl: Int,
  condition: option.Option(valkyrie.ExpireCondition),
) -> Pipeline {
  let expiry_condition = valkyrie.expire_condition_to_modifiers(condition)
  Pipeline(commands: [
    commands.expire(key, ttl, expiry_condition),
    ..pipeline.commands
  ])
}

/// Adds PEXPIRE to the pipeline. See [`valkyrie.pexpire`](/valkyrie.html#pexpire) for more details.
pub fn pexpire(
  pipeline: Pipeline,
  key: String,
  ttl: Int,
  condition: option.Option(valkyrie.ExpireCondition),
) -> Pipeline {
  let expiry_condition = valkyrie.expire_condition_to_modifiers(condition)
  Pipeline(commands: [
    commands.pexpire(key, ttl, expiry_condition),
    ..pipeline.commands
  ])
}

/// Adds EXPIREAT to the pipeline. See [`valkyrie.expireat`](/valkyrie.html#expireat) for more details.
pub fn expireat(
  pipeline: Pipeline,
  key: String,
  unix_seconds: Int,
  condition: option.Option(valkyrie.ExpireCondition),
) -> Pipeline {
  let expiry_condition = valkyrie.expire_condition_to_modifiers(condition)
  Pipeline(commands: [
    commands.expireat(key, unix_seconds, expiry_condition),
    ..pipeline.commands
  ])
}

/// Adds EXPIRETIME to the pipeline. See [`valkyrie.expiretime`](/valkyrie.html#expiretime) for more details.
pub fn expiretime(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.expiretime(key), ..pipeline.commands])
}

/// Adds PEXPIRETIME to the pipeline. See [`valkyrie.pexpiretime`](/valkyrie.html#pexpiretime) for more details.
pub fn pexpiretime(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.pexpiretime(key), ..pipeline.commands])
}

// -------------------------- //
// ----- List functions ----- //
// -------------------------- //

/// Adds LPUSH to the pipeline. See [`valkyrie.lpush`](/valkyrie.html#lpush) for more details.
pub fn lpush(pipeline: Pipeline, key: String, values: List(String)) -> Pipeline {
  Pipeline(commands: [commands.lpush(key, values), ..pipeline.commands])
}

/// Adds RPUSH to the pipeline. See [`valkyrie.rpush`](/valkyrie.html#rpush) for more details.
pub fn rpush(pipeline: Pipeline, key: String, values: List(String)) -> Pipeline {
  Pipeline(commands: [commands.rpush(key, values), ..pipeline.commands])
}

/// Adds LPUSHX to the pipeline. See [`valkyrie.lpushx`](/valkyrie.html#lpushx) for more details.
pub fn lpushx(pipeline: Pipeline, key: String, values: List(String)) -> Pipeline {
  Pipeline(commands: [commands.lpushx(key, values), ..pipeline.commands])
}

/// Adds RPUSHX to the pipeline. See [`valkyrie.rpushx`](/valkyrie.html#rpushx) for more details.
pub fn rpushx(pipeline: Pipeline, key: String, values: List(String)) -> Pipeline {
  Pipeline(commands: [commands.rpushx(key, values), ..pipeline.commands])
}

/// Adds LLEN to the pipeline. See [`valkyrie.llen`](/valkyrie.html#llen) for more details.
pub fn llen(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.llen(key), ..pipeline.commands])
}

/// Adds LRANGE to the pipeline. See [`valkyrie.lrange`](/valkyrie.html#lrange) for more details.
pub fn lrange(pipeline: Pipeline, key: String, start: Int, end: Int) -> Pipeline {
  Pipeline(commands: [commands.lrange(key, start, end), ..pipeline.commands])
}

/// Adds LPOP to the pipeline. See [`valkyrie.lpop`](/valkyrie.html#lpop) for more details.
pub fn lpop(pipeline: Pipeline, key: String, count: Int) -> Pipeline {
  Pipeline(commands: [commands.lpop(key, count), ..pipeline.commands])
}

/// Adds RPOP to the pipeline. See [`valkyrie.rpop`](/valkyrie.html#rpop) for more details.
pub fn rpop(pipeline: Pipeline, key: String, count: Int) -> Pipeline {
  Pipeline(commands: [commands.rpop(key, count), ..pipeline.commands])
}

/// Adds LINDEX to the pipeline. See [`valkyrie.lindex`](/valkyrie.html#lindex) for more details.
pub fn lindex(pipeline: Pipeline, key: String, index: Int) -> Pipeline {
  Pipeline(commands: [commands.lindex(key, index), ..pipeline.commands])
}

/// Adds LREM to the pipeline. See [`valkyrie.lrem`](/valkyrie.html#lrem) for more details.
pub fn lrem(
  pipeline: Pipeline,
  key: String,
  count: Int,
  value: String,
) -> Pipeline {
  Pipeline(commands: [commands.lrem(key, count, value), ..pipeline.commands])
}

/// Adds LSET to the pipeline. See [`valkyrie.lset`](/valkyrie.html#lset) for more details.
pub fn lset(
  pipeline: Pipeline,
  key: String,
  index: Int,
  value: String,
) -> Pipeline {
  Pipeline(commands: [commands.lset(key, index, value), ..pipeline.commands])
}

/// Adds LINSERT to the pipeline. See [`valkyrie.linsert`](/valkyrie.html#linsert) for more details.
pub fn linsert(
  pipeline: Pipeline,
  key: String,
  position: valkyrie.InsertPosition,
  pivot: String,
  value: String,
) -> Pipeline {
  let position_str = valkyrie.insert_position_to_string(position)
  Pipeline(commands: [
    commands.linsert(key, position_str, pivot, value),
    ..pipeline.commands
  ])
}

// ------------------------- //
// ----- Set functions ----- //
// ------------------------- //

/// Adds SADD to the pipeline. See [`valkyrie.sadd`](/valkyrie.html#sadd) for more details.
pub fn sadd(pipeline: Pipeline, key: String, values: List(String)) -> Pipeline {
  Pipeline(commands: [commands.sadd(key, values), ..pipeline.commands])
}

/// Adds SCARD to the pipeline. See [`valkyrie.scard`](/valkyrie.html#scard) for more details.
pub fn scard(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.scard(key), ..pipeline.commands])
}

/// Adds SISMEMBER to the pipeline. See [`valkyrie.sismember`](/valkyrie.html#sismember) for more details.
pub fn sismember(pipeline: Pipeline, key: String, value: String) -> Pipeline {
  Pipeline(commands: [commands.sismember(key, value), ..pipeline.commands])
}

/// Adds SMEMBERS to the pipeline. See [`valkyrie.smembers`](/valkyrie.html#smembers) for more details.
pub fn smembers(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.smembers(key), ..pipeline.commands])
}

/// Adds SSCAN to the pipeline. See [`valkyrie.sscan`](/valkyrie.html#sscan) for more details.
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

/// Adds ZADD to the pipeline. See [`valkyrie.zadd`](/valkyrie.html#zadd) for more details.
pub fn zadd(
  pipeline: Pipeline,
  key: String,
  members: List(#(String, valkyrie.Score)),
  condition: valkyrie.ZAddCondition,
  return_changed: Bool,
) -> Pipeline {
  let modifiers_and_members =
    valkyrie.zadd_build_modifiers_and_members(
      members,
      condition,
      return_changed,
    )
  Pipeline(commands: [
    commands.zadd(key, modifiers_and_members),
    ..pipeline.commands
  ])
}

/// Adds ZINCRBY to the pipeline. See [`valkyrie.zincrby`](/valkyrie.html#zincrby) for more details.
pub fn zincrby(
  pipeline: Pipeline,
  key: String,
  member: String,
  delta: valkyrie.Score,
) -> Pipeline {
  Pipeline(commands: [
    commands.zincrby(key, valkyrie.score_to_string(delta), member),
    ..pipeline.commands
  ])
}

/// Adds ZCARD to the pipeline. See [`valkyrie.zcard`](/valkyrie.html#zcard) for more details.
pub fn zcard(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.zcard(key), ..pipeline.commands])
}

/// Adds ZCOUNT to the pipeline. See [`valkyrie.zcount`](/valkyrie.html#zcount) for more details.
pub fn zcount(
  pipeline: Pipeline,
  key: String,
  min: valkyrie.Score,
  max: valkyrie.Score,
) -> Pipeline {
  Pipeline(commands: [
    commands.zcount(
      key,
      valkyrie.score_to_string(min),
      valkyrie.score_to_string(max),
    ),
    ..pipeline.commands
  ])
}

/// Adds ZSCORE to the pipeline. See [`valkyrie.zscore`](/valkyrie.html#zscore) for more details.
pub fn zscore(pipeline: Pipeline, key: String, member: String) -> Pipeline {
  Pipeline(commands: [commands.zscore(key, member), ..pipeline.commands])
}

/// Adds ZSCAN to the pipeline. See [`valkyrie.zscan`](/valkyrie.html#zscan) for more details.
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

/// Adds ZREM to the pipeline. See [`valkyrie.zrem`](/valkyrie.html#zrem) for more details.
pub fn zrem(pipeline: Pipeline, key: String, members: List(String)) -> Pipeline {
  Pipeline(commands: [commands.zrem(key, members), ..pipeline.commands])
}

/// Adds ZRANDMEMBER to the pipeline. See [`valkyrie.zrandmember`](/valkyrie.html#zrandmember) for more details.
pub fn zrandmember(pipeline: Pipeline, key: String, count: Int) -> Pipeline {
  Pipeline(commands: [commands.zrandmember(key, count), ..pipeline.commands])
}

/// Adds ZPOPMIN to the pipeline. See [`valkyrie.zpopmin`](/valkyrie.html#zpopmin) for more details.
pub fn zpopmin(pipeline: Pipeline, key: String, count: Int) -> Pipeline {
  Pipeline(commands: [commands.zpopmin(key, count), ..pipeline.commands])
}

/// Adds ZPOPMAX to the pipeline. See [`valkyrie.zpopmax`](/valkyrie.html#zpopmax) for more details.
pub fn zpopmax(pipeline: Pipeline, key: String, count: Int) -> Pipeline {
  Pipeline(commands: [commands.zpopmax(key, count), ..pipeline.commands])
}

/// Adds ZRANGE to the pipeline. See [`valkyrie.zrange`](/valkyrie.html#zrange) for more details.
pub fn zrange(
  pipeline: Pipeline,
  key: String,
  start: valkyrie.NumericBound(Int),
  stop: valkyrie.NumericBound(Int),
  reverse: Bool,
) -> Pipeline {
  let modifiers = case reverse {
    True -> ["REV", "WITHSCORES"]
    False -> ["WITHSCORES"]
  }
  Pipeline(commands: [
    commands.zrange(
      key,
      valkyrie.numeric_bound_to_string(start, int.to_string),
      valkyrie.numeric_bound_to_string(stop, int.to_string),
      modifiers,
    ),
    ..pipeline.commands
  ])
}

/// Adds ZRANGE BYSCORE to the pipeline. See [`valkyrie.zrange_byscore`](/valkyrie.html#zrange_byscore) for more details.
pub fn zrange_byscore(
  pipeline: Pipeline,
  key: String,
  start: valkyrie.NumericBound(valkyrie.Score),
  stop: valkyrie.NumericBound(valkyrie.Score),
  reverse: Bool,
) -> Pipeline {
  let modifiers = case reverse {
    True -> ["BYSCORE", "REV", "WITHSCORES"]
    False -> ["BYSCORE", "WITHSCORES"]
  }
  Pipeline(commands: [
    commands.zrange(
      key,
      valkyrie.numeric_bound_to_string(start, valkyrie.score_to_string),
      valkyrie.numeric_bound_to_string(stop, valkyrie.score_to_string),
      modifiers,
    ),
    ..pipeline.commands
  ])
}

/// Adds ZRANGE BYLEX to the pipeline. See [`valkyrie.zrange_bylex`](/valkyrie.html#zrange_bylex) for more details.
pub fn zrange_bylex(
  pipeline: Pipeline,
  key: String,
  start: valkyrie.LexBound,
  stop: valkyrie.LexBound,
  reverse: Bool,
) -> Pipeline {
  let modifiers = case reverse {
    True -> ["BYLEX", "REV"]
    False -> ["BYLEX"]
  }
  Pipeline(commands: [
    commands.zrange(
      key,
      valkyrie.lex_bound_to_string(start),
      valkyrie.lex_bound_to_string(stop),
      modifiers,
    ),
    ..pipeline.commands
  ])
}

/// Adds ZRANK to the pipeline. See [`valkyrie.zrank`](/valkyrie.html#zrank) for more details.
pub fn zrank(pipeline: Pipeline, key: String, member: String) -> Pipeline {
  Pipeline(commands: [commands.zrank(key, member), ..pipeline.commands])
}

/// Adds ZRANK WITHSCORE to the pipeline. See [`valkyrie.zrank_withscore`](/valkyrie.html#zrank_withscore) for more details.
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

/// Adds ZREVRANK to the pipeline. See [`valkyrie.zrevrank`](/valkyrie.html#zrevrank) for more details.
pub fn zrevrank(pipeline: Pipeline, key: String, member: String) -> Pipeline {
  Pipeline(commands: [commands.zrevrank(key, member), ..pipeline.commands])
}

/// Adds ZREVRANK WITHSCORE to the pipeline. See [`valkyrie.zrevrank_withscore`](/valkyrie.html#zrevrank_withscore) for more details.
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

/// Adds HSET to the pipeline. See [`valkyrie.hset`](/valkyrie.html#hset) for more details.
pub fn hset(
  pipeline: Pipeline,
  key: String,
  values: dict.Dict(String, String),
) -> Pipeline {
  Pipeline(commands: [commands.hset(key, values), ..pipeline.commands])
}

/// Adds HSETNX to the pipeline. See [`valkyrie.hsetnx`](/valkyrie.html#hsetnx) for more details.
pub fn hsetnx(
  pipeline: Pipeline,
  key: String,
  field: String,
  value: String,
) -> Pipeline {
  Pipeline(commands: [commands.hsetnx(key, field, value), ..pipeline.commands])
}

/// Adds HLEN to the pipeline. See [`valkyrie.hlen`](/valkyrie.html#hlen) for more details.
pub fn hlen(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.hlen(key), ..pipeline.commands])
}

/// Adds HKEYS to the pipeline. See [`valkyrie.hkeys`](/valkyrie.html#hkeys) for more details.
pub fn hkeys(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.hkeys(key), ..pipeline.commands])
}

/// Adds HGET to the pipeline. See [`valkyrie.hget`](/valkyrie.html#hget) for more details.
pub fn hget(pipeline: Pipeline, key: String, field: String) -> Pipeline {
  Pipeline(commands: [commands.hget(key, field), ..pipeline.commands])
}

/// Adds HGETALL to the pipeline. See [`valkyrie.hgetall`](/valkyrie.html#hgetall) for more details.
pub fn hgetall(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.hgetall(key), ..pipeline.commands])
}

/// Adds HMGET to the pipeline. See [`valkyrie.hmget`](/valkyrie.html#hmget) for more details.
pub fn hmget(pipeline: Pipeline, key: String, fields: List(String)) -> Pipeline {
  Pipeline(commands: [commands.hmget(key, fields), ..pipeline.commands])
}

/// Adds HSTRLEN to the pipeline. See [`valkyrie.hstrlen`](/valkyrie.html#hstrlen) for more details.
pub fn hstrlen(pipeline: Pipeline, key: String, field: String) -> Pipeline {
  Pipeline(commands: [commands.hstrlen(key, field), ..pipeline.commands])
}

/// Adds HVALS to the pipeline. See [`valkyrie.hvals`](/valkyrie.html#hvals) for more details.
pub fn hvals(pipeline: Pipeline, key: String) -> Pipeline {
  Pipeline(commands: [commands.hvals(key), ..pipeline.commands])
}

/// Adds HDEL to the pipeline. See [`valkyrie.hdel`](/valkyrie.html#hdel) for more details.
pub fn hdel(pipeline: Pipeline, key: String, fields: List(String)) -> Pipeline {
  Pipeline(commands: [commands.hdel(key, fields), ..pipeline.commands])
}

/// Adds HEXISTS to the pipeline. See [`valkyrie.hexists`](/valkyrie.html#hexists) for more details.
pub fn hexists(pipeline: Pipeline, key: String, field: String) -> Pipeline {
  Pipeline(commands: [commands.hexists(key, field), ..pipeline.commands])
}

/// Adds HINCRBY to the pipeline. See [`valkyrie.hincrby`](/valkyrie.html#hincrby) for more details.
pub fn hincrby(
  pipeline: Pipeline,
  key: String,
  field: String,
  value: Int,
) -> Pipeline {
  Pipeline(commands: [commands.hincrby(key, field, value), ..pipeline.commands])
}

/// Adds HINCRBYFLOAT to the pipeline. See [`valkyrie.hincrbyfloat`](/valkyrie.html#hincrbyfloat) for more details.
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

/// Adds HSCAN to the pipeline. See [`valkyrie.hscan`](/valkyrie.html#hscan) for more details.
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

import gleam/bit_array
import gleam/dict
import gleam/float
import gleam/int
import gleam/list
import gleam/result
import gleam/set
import gleam/string

pub type Value {
  Nan
  Null
  Infinity
  Integer(Int)
  Boolean(Bool)
  Double(Float)
  BigNumber(Int)
  NegativeInfinity
  Push(List(Value))
  BulkError(String)
  BulkString(String)
  Array(List(Value))
  Set(set.Set(Value))
  SimpleError(String)
  IntegerAsDouble(Int)
  SimpleString(String)
  Map(dict.Dict(Value, Value))
}

fn value_name(value: Value) -> String {
  case value {
    Nan -> "Nan"
    Null -> "Null"
    Infinity -> "Infinity"
    Integer(_) -> "Integer"
    Boolean(_) -> "Boolean"
    Double(_) -> "Double"
    BigNumber(_) -> "BigNumber"
    NegativeInfinity -> "NegativeInfinity"
    Push(_) -> "Push"
    BulkError(_) -> "BulkError"
    BulkString(_) -> "BulkString"
    Array(_) -> "Array"
    Set(_) -> "Set"
    SimpleError(_) -> "SimpleError"
    IntegerAsDouble(_) -> "IntegerAsDouble"
    SimpleString(_) -> "SimpleString"
    Map(_) -> "Map"
  }
}

fn values_to_string(values: List(Value)) -> String {
  case values {
    [] -> "[]"
    [value] -> value_name(value)
    values ->
      values
      |> list.map(value_name)
      |> string.join(", ")
  }
}

pub fn error_string(expected expected: String, got got: List(Value)) -> String {
  "Expected " <> expected <> ", got " <> values_to_string(got)
}

// ----- Encoding ----- //

pub fn encode_command(parts: List(String)) {
  parts
  |> list.map(BulkString)
  |> Array
  |> encode_value
  |> bit_array.from_string
}

fn encode_value(value: Value) -> String {
  case value {
    Nan -> nan()
    Null -> null()
    Infinity -> infinity()
    Set(value) -> set(value)
    Map(value) -> map(value)
    Push(value) -> push(value)
    Array(value) -> array(value)
    Double(value) -> double(value)
    Boolean(value) -> boolean(value)
    Integer(value) -> integer(value)
    BigNumber(value) -> big_number(value)
    BulkError(value) -> bulk_error(value)
    BulkString(value) -> bulk_string(value)
    NegativeInfinity -> negative_infinity()
    SimpleError(value) -> simple_error(value)
    SimpleString(value) -> simple_string(value)
    IntegerAsDouble(value) -> integer_as_double(value)
  }
}

fn null() {
  "_\r\n"
}

fn nan() {
  ",nan\r\n"
}

fn infinity() {
  ",inf\r\n"
}

fn negative_infinity() {
  ",-inf\r\n"
}

fn integer(value: Int) {
  ":" <> int.to_string(value) <> "\r\n"
}

fn boolean(value: Bool) {
  case value {
    True -> "#t\r\n"
    False -> "#f\r\n"
  }
}

fn simple_string(value: String) {
  "+"
  <> {
    value
    |> string.replace("\r", "")
    |> string.replace("\n", "")
  }
  <> "\r\n"
}

fn bulk_string(value: String) {
  "$"
  <> {
    value
    |> string.byte_size
    |> int.to_string
  }
  <> "\r\n"
  <> value
  <> "\r\n"
}

fn simple_error(value: String) {
  "-"
  <> {
    value
    |> string.replace("\r", "")
    |> string.replace("\n", "")
  }
  <> "\r\n"
}

fn bulk_error(value: String) {
  "!"
  <> {
    value
    |> string.byte_size
    |> int.to_string
  }
  <> "\r\n"
  <> value
  <> "\r\n"
}

fn big_number(value: Int) {
  "(" <> int.to_string(value) <> "\r\n"
}

fn double(value: Float) {
  "," <> float.to_string(value) <> "\r\n"
}

fn integer_as_double(value: Int) {
  "," <> int.to_string(value) <> "\r\n"
}

fn array(value: List(Value)) {
  "*"
  <> {
    value
    |> list.length
    |> int.to_string
  }
  <> "\r\n"
  <> {
    list.map(value, encode_value)
    |> string.join("")
  }
}

fn map(value: dict.Dict(Value, Value)) {
  "%"
  <> {
    value
    |> dict.size
    |> int.to_string
  }
  <> "\r\n"
  <> {
    value
    |> dict.to_list
    |> list.map(fn(item) { encode_value(item.0) <> encode_value(item.1) })
    |> string.join("")
  }
}

fn push(value: List(Value)) {
  ">"
  <> {
    value
    |> list.length
    |> int.to_string
  }
  <> "\r\n"
  <> {
    list.map(value, encode_value)
    |> string.join("")
  }
}

fn set(value: set.Set(Value)) {
  let value = set.to_list(value)
  "~"
  <> {
    value
    |> list.length
    |> int.to_string
  }
  <> "\r\n"
  <> {
    list.map(value, encode_value)
    |> string.join("")
  }
}

// ----- Decoding ----- //

pub fn decode_value(value: BitArray) -> Result(List(Value), Nil) {
  decode_multiple(value, [])
}

fn decode_multiple(
  value: BitArray,
  storage: List(Value),
) -> Result(List(Value), Nil) {
  use value <- result.try(decode_message(value))

  let storage = list.append(storage, [value.0])
  case value {
    #(_, <<>>) -> Ok(storage)
    #(_, rest) -> decode_multiple(rest, storage)
  }
}

fn decode_message(value: BitArray) -> Result(#(Value, BitArray), Nil) {
  case value {
    <<>> -> Error(Nil)

    <<"_\r\n":utf8, rest:bits>> -> Ok(#(Null, rest))
    <<",nan\r\n":utf8, rest:bits>> -> Ok(#(Nan, rest))
    <<",inf\r\n":utf8, rest:bits>> -> Ok(#(Infinity, rest))
    <<"#t\r\n":utf8, rest:bits>> -> Ok(#(Boolean(True), rest))
    <<"#f\r\n":utf8, rest:bits>> -> Ok(#(Boolean(False), rest))
    <<",-inf\r\n":utf8, rest:bits>> -> Ok(#(NegativeInfinity, rest))

    <<":":utf8, rest:bits>> -> {
      use #(value, rest) <- result.then(consume_till_crlf(rest, <<>>))
      use value <- result.then(bit_array.to_string(value))

      value
      |> int.parse
      |> result.map(Integer)
      |> result.map(fn(value) { #(value, rest) })
    }

    <<",":utf8, rest:bits>> -> {
      use #(value, rest) <- result.then(consume_till_crlf(rest, <<>>))
      use value <- result.then(bit_array.to_string(value))

      case int.parse(value) {
        Ok(value) ->
          #(
            value
              |> int.to_float
              |> Double,
            rest,
          )
          |> Ok

        Error(Nil) ->
          value
          |> float.parse
          |> result.map(Double)
          |> result.map(fn(value) { #(value, rest) })
      }
    }

    <<"+":utf8, rest:bits>> -> {
      use #(value, rest) <- result.then(consume_till_crlf(rest, <<>>))
      use value <- result.then(bit_array.to_string(value))

      #(SimpleString(value), rest)
      |> Ok
    }

    <<"-":utf8, rest:bits>> -> {
      use #(value, rest) <- result.then(consume_till_crlf(rest, <<>>))
      use value <- result.then(bit_array.to_string(value))

      #(SimpleError(value), rest)
      |> Ok
    }

    <<"(":utf8, rest:bits>> -> {
      use #(value, rest) <- result.then(consume_till_crlf(rest, <<>>))
      use value <- result.then(bit_array.to_string(value))

      value
      |> int.parse
      |> result.map(BigNumber)
      |> result.map(fn(value) { #(value, rest) })
    }

    <<"$":utf8, rest:bits>> -> {
      use #(length, rest) <- result.then(consume_till_crlf(rest, <<>>))
      use length <- result.then(bit_array.to_string(length))
      use length <- result.then(int.parse(length))

      use #(value, rest) <- result.then(
        consume_by_length(rest, length - 1, <<>>),
      )
      use value <- result.then(bit_array.to_string(value))

      let assert <<"\r\n":utf8, rest:bits>> = rest
      #(BulkString(value), rest)
      |> Ok
    }

    <<"!":utf8, rest:bits>> -> {
      use #(length, rest) <- result.then(consume_till_crlf(rest, <<>>))
      use length <- result.then(bit_array.to_string(length))
      use length <- result.then(int.parse(length))

      use #(value, rest) <- result.then(
        consume_by_length(rest, length - 1, <<>>),
      )
      use value <- result.then(bit_array.to_string(value))

      let assert <<"\r\n":utf8, rest:bits>> = rest
      #(BulkError(value), rest)
      |> Ok
    }

    <<"*":utf8, rest:bits>> -> {
      use #(length, rest) <- result.then(consume_till_crlf(rest, <<>>))
      use length <- result.then(bit_array.to_string(length))
      use length <- result.then(int.parse(length))

      use #(value, rest) <- result.then(decode_array(rest, length, []))
      #(
        value
          |> Array,
        rest,
      )
      |> Ok
    }

    <<">":utf8, rest:bits>> -> {
      use #(length, rest) <- result.then(consume_till_crlf(rest, <<>>))
      use length <- result.then(bit_array.to_string(length))
      use length <- result.then(int.parse(length))

      use #(value, rest) <- result.then(decode_array(rest, length, []))
      #(
        value
          |> Push,
        rest,
      )
      |> Ok
    }

    <<"~":utf8, rest:bits>> -> {
      use #(length, rest) <- result.then(consume_till_crlf(rest, <<>>))
      use length <- result.then(bit_array.to_string(length))
      use length <- result.then(int.parse(length))

      use #(value, rest) <- result.then(decode_array(rest, length, []))
      #(
        value
          |> set.from_list
          |> Set,
        rest,
      )
      |> Ok
    }

    <<"%":utf8, rest:bits>> -> {
      use #(length, rest) <- result.then(consume_till_crlf(rest, <<>>))
      use length <- result.then(bit_array.to_string(length))
      use length <- result.then(int.parse(length))

      use #(value, rest) <- result.then(decode_map(rest, length, []))
      #(
        value
          |> Map,
        rest,
      )
      |> Ok
    }

    _ -> Error(Nil)
  }
}

fn consume_till_crlf(
  data: BitArray,
  storage: BitArray,
) -> Result(#(BitArray, BitArray), Nil) {
  case data {
    <<"\r\n":utf8, rest:bits>> -> Ok(#(storage, rest))
    <<ch:8, rest:bits>> ->
      consume_till_crlf(rest, bit_array.append(storage, <<ch>>))
    _ -> Error(Nil)
  }
}

fn consume_by_length(
  data: BitArray,
  length: Int,
  storage: BitArray,
) -> Result(#(BitArray, BitArray), Nil) {
  case bit_array.byte_size(data) {
    0 -> Error(Nil)
    _ -> {
      let assert <<ch:8, rest:bits>> = data
      case bit_array.byte_size(storage) == length {
        True -> Ok(#(bit_array.append(storage, <<ch>>), rest))
        False ->
          consume_by_length(rest, length, bit_array.append(storage, <<ch>>))
      }
    }
  }
}

fn decode_array(data: BitArray, length: Int, storage: List(Value)) {
  case list.length(storage) == length {
    True -> Ok(#(storage, data))
    False -> {
      use #(item, rest) <- result.then(decode_message(data))
      decode_array(rest, length, list.append(storage, [item]))
    }
  }
}

fn decode_map(data: BitArray, length: Int, storage: List(#(Value, Value))) {
  case list.length(storage) == length {
    True -> Ok(#(dict.from_list(storage), data))
    False -> {
      use #(key, rest) <- result.then(decode_message(data))
      use #(value, rest) <- result.then(decode_message(rest))
      decode_map(rest, length, list.append(storage, [#(key, value)]))
    }
  }
}

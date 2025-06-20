import gleam/bit_array
import gleam/erlang/process
import gleam/otp/actor
import gleam/result

import valkyrie/error
import valkyrie/internal/command
import valkyrie/internal/protocol.{type Value}
import valkyrie/internal/tcp

import lifeguard
import mug

const protocol_version = 3

pub type Message {
  Command(BitArray, process.Subject(Result(List(Value), error.Error)), Int)
  BlockingCommand(
    BitArray,
    process.Subject(Result(List(Value), error.Error)),
    Int,
  )
  ReceiveForever(process.Subject(Result(List(Value), error.Error)), Int)
}

pub opaque type Client {
  Client(pool: lifeguard.Pool(Message))
}

pub fn start(
  host: String,
  port: Int,
  timeout: Int,
  pool_size: Int,
  auth_options: List(String),
) -> Result(Client, actor.StartError) {
  worker_spec(host, port, timeout, auth_options)
  |> lifeguard.size(pool_size)
  |> lifeguard.start(timeout)
  |> result.map(Client)
}

fn worker_spec(
  host: String,
  port: Int,
  timeout: Int,
  auth_options: List(String),
) -> lifeguard.Builder(mug.Socket, Message) {
  lifeguard.new_with_initialiser(timeout, fn(_) {
    use socket <- result.try(
      tcp.connect(host, port, timeout)
      |> result.replace_error("Unable to establish TCP connection"),
    )

    use _ <- result.try(
      do_command(
        socket,
        command.hello(protocol_version, auth_options) |> protocol.encode_command,
        timeout,
      )
      |> result.replace_error("Unable to send HELLO command"),
    )

    lifeguard.initialised(socket)
    |> Ok
  })
  |> lifeguard.on_message(handle_message)
}

fn do_command(socket, cmd, timeout) -> Result(List(Value), error.Error) {
  use _ <- result.try(tcp.send(socket, cmd) |> result.map_error(error.TCPError))
  let selector = tcp.new_selector()
  socket_receive(socket, selector, <<>>, now(), timeout)
}

fn handle_message(socket: mug.Socket, msg: Message) {
  case msg {
    Command(cmd, reply_with, timeout) -> {
      let response = do_command(socket, cmd, timeout)
      case response {
        Ok(reply) -> {
          actor.send(reply_with, Ok(reply))
          actor.continue(socket)
        }
        Error(error) -> {
          let _ = mug.shutdown(socket)
          actor.send(reply_with, Error(error))
          actor.stop_abnormal("TCP Error")
        }
      }
    }

    BlockingCommand(cmd, reply_with, timeout) -> {
      case tcp.send(socket, cmd) {
        Ok(Nil) -> {
          let selector = tcp.new_selector()

          case socket_receive_forever(socket, selector, <<>>, now(), timeout) {
            Ok(reply) -> {
              actor.send(reply_with, Ok(reply))
              actor.continue(socket)
            }

            Error(error) -> {
              let _ = mug.shutdown(socket)
              actor.send(reply_with, Error(error))
              actor.stop_abnormal("TCP Error")
            }
          }
        }

        Error(error) -> {
          let _ = mug.shutdown(socket)
          actor.send(reply_with, Error(error.TCPError(error)))
          actor.stop_abnormal("TCP Error")
        }
      }
    }

    ReceiveForever(reply_with, timeout) -> {
      let selector = tcp.new_selector()

      case socket_receive_forever(socket, selector, <<>>, now(), timeout) {
        Ok(reply) -> {
          actor.send(reply_with, Ok(reply))
          actor.continue(socket)
        }

        Error(error) -> {
          let _ = mug.shutdown(socket)
          actor.send(reply_with, Error(error))
          actor.stop_abnormal("TCP Error")
        }
      }
    }
  }
}

fn socket_receive(
  socket: mug.Socket,
  selector: process.Selector(Result(BitArray, mug.Error)),
  storage: BitArray,
  start_time: Int,
  timeout: Int,
) -> Result(List(Value), error.Error) {
  case protocol.decode_value(storage) {
    Ok(value) -> Ok(value)
    Error(_) -> {
      case now() - start_time >= timeout * 1_000_000 {
        True -> Error(error.RESPError)
        False ->
          case tcp.receive(socket, selector, timeout) {
            Error(tcp_error) -> Error(error.TCPError(tcp_error))
            Ok(packet) ->
              socket_receive(
                socket,
                selector,
                bit_array.append(storage, packet),
                start_time,
                timeout,
              )
          }
      }
    }
  }
}

fn socket_receive_forever(
  socket: mug.Socket,
  selector: process.Selector(Result(BitArray, mug.Error)),
  storage: BitArray,
  start_time: Int,
  timeout: Int,
) {
  case protocol.decode_value(storage) {
    Ok(value) -> Ok(value)

    Error(_) if timeout != 0 -> {
      case now() - start_time >= timeout * 1_000_000 {
        True -> Error(error.RESPError)
        False ->
          case tcp.receive_forever(socket, selector) {
            Error(tcp_error) -> Error(error.TCPError(tcp_error))
            Ok(packet) ->
              socket_receive_forever(
                socket,
                selector,
                bit_array.append(storage, packet),
                start_time,
                timeout,
              )
          }
      }
    }

    Error(_) -> {
      case tcp.receive_forever(socket, selector) {
        Error(tcp_error) -> Error(error.TCPError(tcp_error))
        Ok(packet) ->
          socket_receive_forever(
            socket,
            selector,
            bit_array.append(storage, packet),
            start_time,
            timeout,
          )
      }
    }
  }
}

@external(erlang, "erlang", "monotonic_time")
fn now() -> Int

// ----- Caller functions ----- //

pub fn shutdown(client: Client) {
  lifeguard.shutdown(client.pool)
}

pub fn receive_forever(client: Client, timeout: Int, rest) {
  let my_subject = process.new_subject()
  let _ =
    lifeguard.send(client.pool, ReceiveForever(my_subject, timeout), timeout)

  process.new_selector()
  |> process.select(my_subject)
  |> process.selector_receive_forever
  |> fn(reply) {
    case reply {
      Ok([protocol.SimpleError(error)]) | Ok([protocol.BulkError(error)]) -> {
        case rest(Error(error.ServerError(error))) {
          True -> receive_forever(client, timeout, rest)
          False -> Nil
        }
      }
      other -> {
        case rest(other) {
          True -> receive_forever(client, timeout, rest)
          False -> Nil
        }
      }
    }
  }
}

pub fn execute(client: Client, cmd: BitArray, timeout: Int) {
  use reply <- result.then(
    lifeguard.call(client.pool, Command(cmd, _, timeout), timeout, timeout)
    |> result.replace_error(error.ActorError),
  )

  use reply <- result.then(reply)
  case reply {
    [protocol.SimpleError(error)] | [protocol.BulkError(error)] ->
      Error(error.ServerError(error))
    value -> Ok(value)
  }
}

pub fn execute_blocking(client: Client, cmd: BitArray, timeout: Int) {
  let my_subject = process.new_subject()
  use _ <- result.then(
    lifeguard.send(
      client.pool,
      BlockingCommand(cmd, my_subject, timeout),
      timeout,
    )
    |> result.replace_error(error.ActorError),
  )

  process.new_selector()
  |> process.select(my_subject)
  |> process.selector_receive_forever
  |> fn(reply) {
    case reply {
      Ok([protocol.SimpleError(error)]) | Ok([protocol.BulkError(error)]) ->
        Error(error.ServerError(error))
      Ok(value) -> Ok(value)
      Error(error) -> Error(error)
    }
  }
}

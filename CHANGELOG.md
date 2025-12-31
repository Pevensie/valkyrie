# Changelog

## 4.1.0 - 2025-12-31

- Added the new `valkyrie/pipeline` API

## 4.0.0 - 2025-08-15

- Update `lpop` and `rpop` to return string arrays. Previously they would error if a
  count higher than 1 was provided.

## 3.0.0 - 2025-06-29

- Update `mug` and add support for IPv6 connections.

## 2.0.0 - 2025-06-22

- Add named pool support.

## 1.0.1 - 2025-06-22

- Remove extraneous dependency on [Lifeguard](https://github.com/Pevensie/lifeguard)

## 1.0.0 - 2025-06-22

Initial release of Valkyrie, the lightweight, performant Redis-compatible client for
the Gleam programming language. See the [README](https://github.com/Pevensie/valkyrie)
for more information.

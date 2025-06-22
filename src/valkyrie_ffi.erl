-module(valkyrie_ffi).

-export([monotonic_now/0]).

monotonic_now() ->
    StartTime = erlang:system_info(start_time),
    CurrentTime = erlang:monotonic_time(),
    Difference = CurrentTime - StartTime,
    erlang:convert_time_unit(Difference, native, nanosecond).

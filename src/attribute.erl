-module(attribute).

-include("ddb_types.hrl").

-export([
    string/1,
    string_set/1,
    binary/1,
    binary_set/1,
    boolean/1,
    null/0,
    list/1,
    number/1,
    number_set/1,
    map/1,
    fields/1
]).
-export([
    encode/1,
    decode/1
]).

-spec string(string()) -> #attribute{type :: string, value :: string()}.
string(Value) when is_list(Value) ->
    #attribute{type=string, value=Value}.

-spec string_set(nonempty_list(string())) -> #attribute{type :: string_set, value :: nonempty_list(string())}.
string_set(Value) ->
    #attribute{type=string_set, value=Value}.

-spec binary(binary()) -> #attribute{type :: binary, value :: binary()}.
binary(Value) ->
    #attribute{type=binary, value=Value}.

-spec binary_set(nonempty_list(binary())) ->  #attribute{type :: binary_set, value :: nonempty_list(binary())}.
binary_set(Value) ->
    #attribute{type=binary_set, value=Value}.

-spec boolean(boolean()) -> #attribute{type :: boolean, value :: boolean()}.
boolean(Value) ->
    #attribute{type=boolean, value=Value}.

-spec null() -> #attribute{type :: null, value :: true}.
null() ->
    #attribute{type=null, value=true}.

-spec number(number()) -> #attribute{type :: number, value :: number()}.
number(Value) ->
    #attribute{type=number, value=Value}.

-spec number_set(nonempty_list(number())) -> #attribute{type :: number_set, value :: nonempty_list(number())}.
number_set(Value) ->
    #attribute{type=number_set, value=Value}.

-spec list(nonempty_list(#attribute{})) -> #attribute{type :: list, value :: nonempty_list(#attribute{})}.
list(Value) ->
    #attribute{type=list, value=Value}.

-spec map(nonempty_list(#field{})) -> #attribute{type :: map, value :: #{string() := #attribute{}}}.
map(Fields) ->
    Map = lists:foldl(
        fun(#field{name=Name, attribute=Attribute}, Acc) ->
            maps:put(Name, Attribute, Acc)
        end,
        maps:new(),
        Fields
    ),
    #attribute{type=map, value=Map}.

-spec fields(nonempty_list({string(), #attribute{}})) -> nonempty_list(#field{}).
fields(Attributes) ->
    lists:map(
        fun({Name, Attribute}) ->
            #field{name = Name, attribute = Attribute}
        end,
        Attributes
    ).

-spec encode(#attribute{} | #field{}) -> #{binary() => binary()}.
encode(#attribute{type=string, value=Value}) ->
    #{<<"S">> => list_to_binary(Value)};
encode(#attribute{type=string_set, value=Value}) ->
    #{<<"SS">> => lists:map(fun erlang:list_to_binary/1, Value)};
encode(#attribute{type=binary, value=Value}) ->
    #{<<"B">> => base64:encode(Value)};
encode(#attribute{type=binary_set, value=Value}) ->
    #{<<"BS">> => lists:map(fun base64:encode/1, Value)};
encode(#attribute{type=boolean, value=Value}) ->
    #{<<"BOOL">> => Value};
encode(#attribute{type=null, value=Value}) ->
    #{<<"NULL">> => Value};
encode(#attribute{type=number, value=Value}) ->
    #{<<"N">> => number_to_binary(Value)};
encode(#attribute{type=number_set, value=Value}) ->
    #{<<"NS">> => lists:map(fun number_to_binary/1, Value)};
encode(#attribute{type=list, value=Value}) ->
    #{<<"L">> => lists:map(fun encode/1, Value)};
encode(#attribute{type=map, value=Value}) ->
    Map = maps:fold(
        fun(Key, Attribute, Acc) ->
            maps:put(list_to_binary(Key), encode(Attribute), Acc)
        end,
        maps:new(),
        Value
    ),
    #{<<"M">> => Map};
encode(#field{name = Name, attribute = Attribute}) ->
    #{list_to_binary(Name) => encode(Attribute)};
encode(Fields) ->
    lists:foldl(
        fun(Field, Acc) ->
            maps:merge(encode(Field), Acc)
        end,
        maps:new(),
        Fields
    ).

-type decode_result() :: string() | nonempty_list(string()) | binary() | nonempty_list(binary()) | boolean() | nil | number() | nonempty_list(number()) | nonempty_list(decode_result()) | #{string() := decode_result()}.
-spec decode(#{binary() => binary()}) -> decode_result().
decode(#{<<"S">> := Value}) ->
    binary_to_list(Value);
decode(#{<<"SS">> := Value}) ->
    lists:map(fun erlang:binary_to_list/1, Value);
decode(#{<<"B">> := Value}) ->
    base64:decode(Value);
decode(#{<<"BS">> := Value}) ->
    lists:map(fun base64:decode/1, Value);
decode(#{<<"BOOL">> := Value}) ->
    Value;
decode(#{<<"NULL">> := _}) ->
    nil;
decode(#{<<"N">> := Value}) ->
    binary_to_number(Value);
decode(#{<<"NS">> := Value}) ->
    lists:map(fun binary_to_number/1, Value);
decode(#{<<"L">> := Value}) ->
    lists:map(fun decode/1, Value);
decode(#{<<"M">> := Value}) ->
    maps:fold(
        fun(Key, ItemValue, Acc) ->
            maps:put(binary_to_list(Key), decode(ItemValue), Acc)
        end,
        maps:new(),
        Value
    );
decode(Fields) when is_list(Fields) =:= false ->
    maps:fold(
        fun(Key, ItemValue, Acc) ->
            maps:put(binary_to_list(Key), decode(ItemValue), Acc)
        end,
        maps:new(),
        Fields
    );
decode(Fields) ->
    lists:map(fun decode/1, Fields).

%
% Private functions
%

-spec number_to_binary(number()) -> binary().
number_to_binary(Value) when is_float(Value) ->
    float_to_binary(Value);
number_to_binary(Value) when is_integer(Value) ->
    integer_to_binary(Value).

-spec binary_to_number(binary()) -> number().
binary_to_number(Value) ->
    try binary_to_float(Value)
    catch
        error:badarg -> binary_to_integer(Value)
    end.

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
    map/1
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

-spec map(nonempty_list({string(), #attribute{}})) -> #attribute{type :: map, value :: nonempty_list({string(), #attribute{}})}.
map(Value) ->
    #attribute{type=map, value=Value}.

encode(Value) when is_list(Value) =:= false ->
    private_encode(Value);
encode(Value) ->
    lists:map(
        fun({Name, Item}) ->
            {list_to_binary(Name), private_encode(Item)}
        end,
        Value
    ).

decode(Value) ->
    private_decode(Value, []).

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

private_encode(#attribute{type=string, value=Value}) ->
    [{<<"S">>, list_to_binary(Value)}];
private_encode(#attribute{type=string_set, value=Value}) ->
    [{<<"SS">>, lists:map(fun erlang:list_to_binary/1, Value)}];
private_encode(#attribute{type=binary, value=Value}) ->
    [{<<"B">>, base64:encode(Value)}];
private_encode(#attribute{type=binary_set, value=Value}) ->
    [{<<"BS">>, lists:map(fun base64:encode/1, Value)}];
private_encode(#attribute{type=boolean, value=Value}) ->
    [{<<"BOOL">>, Value}];
private_encode(#attribute{type=null, value=Value}) ->
    [{<<"NULL">>, Value}];
private_encode(#attribute{type=number, value=Value}) ->
    [{<<"N">>, number_to_binary(Value)}];
private_encode(#attribute{type=number_set, value=Value}) ->
    [{<<"NS">>, lists:map(fun number_to_binary/1, Value)}];
private_encode(#attribute{type=list, value=Value}) ->
    [{<<"L">>, lists:map(fun private_encode/1, Value)}];
private_encode(#attribute{type=map, value=Value}) ->
    Items = lists:map(
        fun({Name, Item}) ->
            {list_to_binary(Name), private_encode(Item)}
        end,
        Value
    ),
    [{<<"M">>, Items}].

private_decode([], Acc) ->
    Acc;
private_decode([{<<"S">>, Value}], _Acc) ->
    binary_to_list(Value);
private_decode([{<<"SS">>, Value}], _Acc) ->
    lists:map(fun erlang:binary_to_list/1, Value);
private_decode([{<<"B">>, Value}], _Acc) ->
    base64:decode(Value);
private_decode([{<<"BS">>, Value}], _Acc) ->
    lists:map(fun base64:decode/1, Value);
private_decode([{<<"BOOL">>, Value}], _Acc) ->
    Value;
private_decode([{<<"NULL">>, _}], _Acc) ->
    nil;
private_decode([{<<"N">>, Value}], _Acc) ->
    binary_to_number(Value);
private_decode([{<<"NS">>, Value}], _Acc) ->
    lists:map(fun binary_to_number/1, Value);
private_decode([{<<"L">>, Value}], Acc) ->
    lists:map(fun(Item) -> private_decode(Item, Acc) end, Value);
private_decode([{<<"M">>, Value}], Acc) ->
    private_decode(Value, Acc);
private_decode([{FieldName, Attribute} | Rest], Acc) ->
    Next = private_decode(Attribute, []),
    private_decode(Rest, [{binary_to_list(FieldName), Next} | Acc]).

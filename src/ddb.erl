-module(ddb).

% -record(get_item_request, {table_name, key}).
% -record(get_item_response, {}).

-include("ddb_types.hrl").

-export([
    get_item/3,
    get_item/4,
    query/3,
    query/4,
    scan/2,
    scan/3,
    put_item/3,
    put_item/4
]).

-type return_consumed_capacity() :: indexes | total | none.

get_item(Region, TableName, Keys) ->
    get_item(Region, TableName, Keys, []).

get_item(Region, TableName, Keys, Options) ->
    Req = #{
        <<"TableName">> => list_to_binary(TableName),
        <<"Key">> => attribute:encode(Keys)
    },
    private_get_item(Region, Options, Req).

query(Region, TableName, KeyCondition) ->
    query(Region, TableName, KeyCondition, []).

query(Region, TableName, KeyCondition, Options) ->
    Req = #{
        <<"TableName">> => list_to_binary(TableName),
        <<"KeyConditionExpression">> => list_to_binary(KeyCondition)
    },
    private_query(Region, Options, Req).

scan(Region, TableName) ->
    scan(Region, TableName, []).

scan(Region, TableName, Options) ->
    Req = #{
        <<"TableName">> => list_to_binary(TableName)
    },
    private_scan(Region, Options, Req).

put_item(Region, TableName, Item) ->
    put_item(Region, TableName, Item, []).

put_item(Region, TableName, Item, Options) ->
    Req = #{
        <<"TableName">> => list_to_binary(TableName),
        <<"Item">> => attribute:encode(Item)
    },
    private_put_item(Region, Options, Req).

decode_response(Map) when is_map_key(<<"Item">>, Map) ->
    {ok, attribute:decode(maps:get(<<"Item">>, Map)), maps:remove(<<"Item">>, Map)};
decode_response(Map) when is_map_key(<<"Items">>, Map) ->
    {ok, attribute:decode(maps:get(<<"Items">>, Map)), maps:remove(<<"Items">>, Map)};
decode_response(Map) ->
    {ok, Map}.

%
% Private functions
%

return_consumed_capacity(indexes) -> <<"INDEXES">>;
return_consumed_capacity(total) -> <<"TOTAL">>;
return_consumed_capacity(none) -> <<"NONE">>.

select(all_attributes) -> <<"ALL_ATTRIBUTES">>;
select(all_projected_attributes) -> <<"ALL_PROJECTED_ATTRIBUTES">>;
select(specific_attributes) -> <<"SPECIFIC_ATTRIBUTES">>;
select(count) -> <<"COUNT">>.

return_item_collection_metrics(size) -> <<"SIZE">>;
return_item_collection_metrics(none) -> <<"NONE">>.

return_values(none) -> <<"NONE">>;
return_values(all_old) -> <<"ALL_OLD">>;
return_values(updated_old) -> <<"UPDATED_OLD">>;
return_values(all_new) -> <<"ALL_NEW">>;
return_values(updated_new) -> <<"UPDATED_NEW">>.

private_get_item(Region, [], Acc) ->
    io:format("Req(~p)~n", [Acc]),
    api(Region, "DynamoDB_20120810.GetItem", Acc);
private_get_item(Region, [{consistent_read, _} = Option | Rest], Acc) ->
    private_get_item(Region, Rest, option(Option, Acc));
private_get_item(Region, [{return_consumed_capacity, _} = Option | Rest], Acc) ->
    private_get_item(Region, Rest, option(Option, Acc));
private_get_item(Region, [{projection_expression, _} = Option | Rest], Acc) ->
    private_get_item(Region, Rest, option(Option, Acc));
private_get_item(Region, [{expression_attribute_names, _} = Option | Rest], Acc) ->
    private_get_item(Region, Rest, option(Option, Acc)).

private_query(Region, [], Acc) ->
    io:format("Req(~p)~n", [Acc]),
    api(Region, "DynamoDB_20120810.Query", Acc);
private_query(Region, [{consistent_read, _} = Option | Rest], Acc) ->
    private_query(Region, Rest, option(Option, Acc));
private_query(Region, [{exclusive_start_key, _} = Option | Rest], Acc) ->
    private_query(Region, Rest, option(Option, Acc));
private_query(Region, [{expression_attribute_names, _} = Option | Rest], Acc) ->
    private_query(Region, Rest, option(Option, Acc));
private_query(Region, [{expression_attribute_values, _} = Option | Rest], Acc) ->
    private_query(Region, Rest, option(Option, Acc));
private_query(Region, [{filter_expression, _} = Option | Rest], Acc) ->
    private_query(Region, Rest, option(Option, Acc));
private_query(Region, [{index_name, _} = Option | Rest], Acc) ->
    private_query(Region, Rest, option(Option, Acc));
private_query(Region, [{key_condition_expression, _} = Option | Rest], Acc) ->
    private_query(Region, Rest, option(Option, Acc));
private_query(Region, [{limit, _} = Option | Rest], Acc) ->
    private_query(Region, Rest, option(Option, Acc));
private_query(Region, [{projection_expression, _} = Option | Rest], Acc) ->
    private_query(Region, Rest, option(Option, Acc));
private_query(Region, [{return_consumed_capacity, _} = Option | Rest], Acc) ->
    private_query(Region, Rest, option(Option, Acc));
private_query(Region, [{scan_index_forward, _} = Option | Rest], Acc) ->
    private_query(Region, Rest, option(Option, Acc));
private_query(Region, [{select, _} = Option | Rest], Acc) ->
    private_query(Region, Rest, option(Option, Acc)).

private_scan(Region, [], Acc) ->
    io:format("Req(~p)~n", [Acc]),
    api(Region, "DynamoDB_20120810.Scan", Acc);
private_scan(Region, [{consistent_read, _} = Option | Rest], Acc) ->
    private_scan(Region, Rest, option(Option, Acc));
private_scan(Region, [{exclusive_start_key, _} = Option | Rest], Acc) ->
    private_scan(Region, Rest, option(Option, Acc));
private_scan(Region, [{expression_attribute_names, _} = Option | Rest], Acc) ->
    private_scan(Region, Rest, option(Option, Acc));
private_scan(Region, [{expression_attribute_values, _} = Option | Rest], Acc) ->
    private_scan(Region, Rest, option(Option, Acc));
private_scan(Region, [{filter_expression, _} = Option | Rest], Acc) ->
    private_scan(Region, Rest, option(Option, Acc));
private_scan(Region, [{index_name, _} = Option | Rest], Acc) ->
    private_scan(Region, Rest, option(Option, Acc));
private_scan(Region, [{limit, _} = Option | Rest], Acc) ->
    private_scan(Region, Rest, option(Option, Acc));
private_scan(Region, [{projection_expression, _} = Option | Rest], Acc) ->
    private_scan(Region, Rest, option(Option, Acc));
private_scan(Region, [{return_consumed_capacity, _} = Option | Rest], Acc) ->
    private_scan(Region, Rest, option(Option, Acc));
private_scan(Region, [{segment, _} = Option | Rest], Acc) ->
    private_scan(Region, Rest, option(Option, Acc));
private_scan(Region, [{total_segments, _} = Option | Rest], Acc) ->
    private_scan(Region, Rest, option(Option, Acc)).

private_put_item(Region, [], Acc) ->
    io:format("Req(~p)~n", [Acc]),
    api(Region, "DynamoDB_20120810.PutItem", Acc);
private_put_item(Region, [{condition_expression, _} = Option | Rest], Acc) ->
    private_put_item(Region, Rest, option(Option, Acc));
private_put_item(Region, [{expression_attribute_names, _} = Option | Rest], Acc) ->
    private_put_item(Region, Rest, option(Option, Acc));
private_put_item(Region, [{expression_attribute_values, _} = Option | Rest], Acc) ->
    private_put_item(Region, Rest, option(Option, Acc));
private_put_item(Region, [{return_consumed_capacity, _} = Option | Rest], Acc) ->
    private_put_item(Region, Rest, option(Option, Acc));
private_put_item(Region, [{return_item_collection_metrics, _} = Option | Rest], Acc) ->
    private_put_item(Region, Rest, option(Option, Acc));
private_put_item(Region, [{return_values, _} = Option | Rest], Acc) ->
    private_put_item(Region, Rest, option(Option, Acc)).

option({consistent_read, Value}, Acc) ->
    maps:put(<<"ConsistentRead">>, Value, Acc);
option({return_consumed_capacity, Value}, Acc) ->
    maps:put(<<"ReturnConsumedCapacity">>, return_consumed_capacity(Value), Acc);
option({projection_expression, Value}, Acc) ->
    maps:put(<<"ProjectionExpression">>, list_to_binary(Value), Acc);
option({exclusive_start_key, Value}, Acc) ->
    maps:put(<<"ExclusiveStartKey">>, attribute:encode(Value), Acc);
option({expression_attribute_names, Value}, Acc) ->
        ExpressionAttributeNames = lists:foldl(
        fun({Key, Item}, NewAcc) ->
            maps:put(list_to_binary(Key), list_to_binary(Item), NewAcc)
        end,
        maps:new(),
        Value
    ),
    maps:put(<<"ExpressionAttributeNames">>, ExpressionAttributeNames, Acc);
option({expression_attribute_values, Value}, Acc) ->
        ExpressionAttributeValues = lists:foldl(
        fun({Key, Item}, NewAcc) ->
            maps:put(list_to_binary(Key), attribute:encode(Item), NewAcc)
        end,
        maps:new(),
        Value
    ),
    maps:put(<<"ExpressionAttributeValues">>, ExpressionAttributeValues, Acc);
option({filter_expression, Value}, Acc) ->
    maps:put(<<"FilterExpression">>, list_to_binary(Value), Acc);
option({index_name, Value}, Acc) ->
    maps:put(<<"IndexName">>, list_to_binary(Value), Acc);
option({key_condition_expression, Value}, Acc) ->
    maps:put(<<"KeyConditionExpression">>, list_to_binary(Value), Acc);
option({limit, Value}, Acc) ->
    maps:put(<<"Limit">>, Value, Acc);
option({scan_index_forward, Value}, Acc) ->
    maps:put(<<"ScanIndexForward">>, Value, Acc);
option({select, Value}, Acc) ->
    maps:put(<<"Select">>, select(Value), Acc);
option({segment, Value}, Acc) ->
    maps:put(<<"Segment">>, Value, Acc);
option({total_segments, Value}, Acc) ->
    maps:put(<<"TotalSegments">>, Value, Acc);
option({condition_expression, Value}, Acc) ->
    maps:put(<<"ConditionExpression">>, list_to_binary(Value), Acc);
option({return_item_collection_metrics, Value}, Acc) ->
    maps:put(<<"ReturnItemCollectionMetrics">>, return_item_collection_metrics(Value), Acc);
option({return_values, Value}, Acc) ->
    maps:put(<<"ReturnValues">>, return_values(Value), Acc).

% TODO: implement a connection pooling, we shouldn't have to re-open a new connection for every request.
api(Region, Method, Request) ->
    SignedHeaders = #{"content-type" => "application/x-amz-json-1.0"},
    Payload = jiffy:encode(Request),
    Host = "dynamodb." ++ Region ++ ".amazonaws.com",
    Headers = awsv4:headers(
        erliam:credentials(),
        #{
            service => "dynamodb",
            region => Region,
            method => "POST",
            host => Host,
            target_api => Method,
            signed_headers => SignedHeaders
        },
        Payload
    ),
    {ok, ConnPid} = gun:open(Host, 443),
    StreamRef = gun:post(ConnPid, "/", Headers, Payload),
    {response, nofin, _Status, _} = gun:await(ConnPid, StreamRef),
    {ok, RespBody} = gun:await_body(ConnPid, StreamRef),
    gun:close(ConnPid),
    Response = jiffy:decode(RespBody, [return_maps]),
    decode_response(Response).

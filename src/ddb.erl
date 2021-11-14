-module(ddb).

-include("ddb_types.hrl").

-export([
    get_item/3,
    get_item/4,
    query/3,
    query/4,
    scan/2,
    scan/3,
    put_item/3,
    put_item/4,
    create_table/4,
    create_table/5
]).

create_table(Region, TableName, AttributeDefinitions, KeySchema) ->
    create_table(Region, TableName, AttributeDefinitions, KeySchema, []).

create_table(Region, TableName, AttributeDefinitions, KeySchema, Options) ->
    Req = [
        {<<"TableName">>, list_to_binary(TableName)}
    ],
    Req2 = option({attribute_definitions, AttributeDefinitions}, Req),
    Req3 = option({key_schema, KeySchema}, Req2),
    private_create_table(Region, Options, Req3).

get_item(Region, TableName, Keys) ->
    get_item(Region, TableName, Keys, []).

get_item(Region, TableName, Keys, Options) ->
    Req = [
        {<<"TableName">>, list_to_binary(TableName)},
        {<<"Key">>, attribute:encode(Keys)}
    ],
    private_get_item(Region, Options, Req).

query(Region, TableName, KeyCondition) ->
    query(Region, TableName, KeyCondition, []).

query(Region, TableName, KeyCondition, Options) ->
    Req = [
        {<<"TableName">>, list_to_binary(TableName)},
        {<<"KeyConditionExpression">>, list_to_binary(KeyCondition)}
    ],
    private_query(Region, Options, Req).

scan(Region, TableName) ->
    scan(Region, TableName, []).

scan(Region, TableName, Options) ->
    Req = [
        {<<"TableName">>, list_to_binary(TableName)}
    ],
    private_scan(Region, Options, Req).

put_item(Region, TableName, Item) ->
    put_item(Region, TableName, Item, []).

put_item(Region, TableName, Item, Options) ->
    Req = [
        {<<"TableName">>, list_to_binary(TableName)},
        {<<"Item">>, attribute:encode(Item)}
    ],
    private_put_item(Region, Options, Req).

decode_response([], Acc) ->
    Acc;
decode_response([{<<"Item">>, Data} | Rest], Acc) ->
    decode_response(Rest, [attribute:decode(Data) | Acc]);
decode_response([{<<"Items">>, Data} | Rest], Acc) ->
    Items = lists:map(fun attribute:decode/1, Data),
    decode_response(Rest, [Items | Acc]);
decode_response([Item | Rest], Acc) ->
    decode_response(Rest, [Item | Acc]).

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

attribute_definition({Name, string}) ->
    [
        {<<"AttributeName">>, list_to_binary(Name)},
        {<<"AttributeType">>, <<"S">>}
    ];
attribute_definition({Name, number}) ->
    [
        {<<"AttributeName">>, list_to_binary(Name)},
        {<<"AttributeType">>, <<"N">>}
    ];
attribute_definition({Name, binary}) ->
    [
        {<<"AttributeName">>, list_to_binary(Name)},
        {<<"AttributeType">>, <<"B">>}
    ].

key_schema({Name, hash}) ->
    [
        {<<"AttributeName">>, list_to_binary(Name)},
        {<<"KeyType">>, <<"HASH">>}
    ];
key_schema({Name, partition}) ->
    [
        {<<"AttributeName">>, list_to_binary(Name)},
        {<<"KeyType">>, <<"HASH">>}
    ];
key_schema({Name, range}) ->
    [
        {<<"AttributeName">>, list_to_binary(Name)},
        {<<"KeyType">>, <<"RANGE">>}
    ];
key_schema({Name, sort}) ->
    [
        {<<"AttributeName">>, list_to_binary(Name)},
        {<<"KeyType">>, <<"RANGE">>}
    ].

provisioned_throughput([], Acc) ->
    Acc;
provisioned_throughput([{read_capacity_units, Value} | Rest], Acc) ->
    provisioned_throughput(Rest, [{<<"ReadCapacityUnits">>, integer_to_binary(Value)} | Acc]);
provisioned_throughput([{write_capacity_units, Value} | Rest], Acc) ->
    provisioned_throughput(Rest, [{<<"WriteCapacityUnits">>, integer_to_binary(Value)} | Acc]).

billing_mode(provisioned) -> <<"PROVISIONED">>;
billing_mode(pay_per_request) -> <<"PAY_PER_REQUEST">>.

private_create_table(Region, [], Acc) ->
    io:format("Req(~p)~n", [Acc]),
    api(Region, "DynamoDB_20120810.CreateTable", Acc);
private_create_table(Region, [{billing_mode, _} = Option | Rest], Acc) ->
    private_create_table(Region, Rest, option(Option, Acc)).

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
    [{<<"ConsistentRead">>, Value} | Acc];
option({return_consumed_capacity, Value}, Acc) ->
    [{<<"ReturnConsumedCapacity">>, return_consumed_capacity(Value)} | Acc];
option({projection_expression, Value}, Acc) ->
    [{<<"ProjectionExpression">>, list_to_binary(Value)} | Acc];
option({exclusive_start_key, Value}, Acc) ->
    [{<<"ExclusiveStartKey">>, attribute:encode(Value)} | Acc];
option({expression_attribute_names, Value}, Acc) ->
    ExpressionAttributeNames = lists:map(
        fun({Key, Item}) ->
            {list_to_binary(Key), list_to_binary(Item)}
        end,
        Value
    ),
   [{<<"ExpressionAttributeNames">>, ExpressionAttributeNames} | Acc];
option({expression_attribute_values, Value}, Acc) ->
    ExpressionAttributeValues = lists:map(
        fun({Key, Item}) ->
            {list_to_binary(Key), attribute:encode(Item)}
        end,
        Value
    ),
    [{<<"ExpressionAttributeValues">>, ExpressionAttributeValues} | Acc];
option({filter_expression, Value}, Acc) ->
    [{<<"FilterExpression">>, list_to_binary(Value)} | Acc];
option({index_name, Value}, Acc) ->
    [{<<"IndexName">>, list_to_binary(Value)} | Acc];
option({key_condition_expression, Value}, Acc) ->
    [{<<"KeyConditionExpression">>, list_to_binary(Value)} | Acc];
option({limit, Value}, Acc) ->
    [{<<"Limit">>, Value} | Acc];
option({scan_index_forward, Value}, Acc) ->
    [{<<"ScanIndexForward">>, Value} | Acc];
option({select, Value}, Acc) ->
    [{<<"Select">>, select(Value)} | Acc];
option({segment, Value}, Acc) ->
    [{<<"Segment">>, Value} | Acc];
option({total_segments, Value}, Acc) ->
    [{<<"TotalSegments">>, Value} | Acc];
option({condition_expression, Value}, Acc) ->
    [{<<"ConditionExpression">>, list_to_binary(Value)} | Acc];
option({return_item_collection_metrics, Value}, Acc) ->
    [{<<"ReturnItemCollectionMetrics">>, return_item_collection_metrics(Value)} | Acc];
option({return_values, Value}, Acc) ->
    [{<<"ReturnValues">>, return_values(Value)} | Acc];
option({attribute_definitions, Value}, Acc) ->
    Items = lists:map(fun attribute_definition/1, Value),
    [{<<"AttributeDefinitions">>, Items} | Acc];
option({key_schema, Value}, Acc) ->
    Items = lists:map(fun key_schema/1, Value),
    [{<<"KeySchema">>, Items} | Acc];
option({billing_mode, Value}, Acc) ->
    [{<<"BillingMode">>, billing_mode(Value)} | Acc];
option({provisioned_throughput, Value}, Acc) ->
    [{<<"ProvisionedThroughput">>, provisioned_throughput(Value, [])} | Acc].

api(Region, Method, Request) ->
    Payload = jsx:encode(Request),
    io:format("Payload(~p)~n", [Payload]),
    Host = "localhost:8000",
    % Host = "dynamodb." ++ Region ++ ".amazonaws.com",
    Headers = awsv4:headers(
        erliam:credentials(),
        #{
            service => "dynamodb",
            region => Region,
            method => "POST",
            host => Host,
            target_api => Method
        },
        Payload
    ),
    URL = list_to_binary("http://" ++ Host),
    Tmp = [{<<"Content-Type">>, <<"application/x-amz-json-1.0">>} | Headers],
    {ok, _StatusCode, _RespHeaders, ClientRef} =
        hackney:request(post, URL, Tmp, Payload, []),
    {ok, RespBody} = hackney:body(ClientRef),
    io:format("~s~n", [RespBody]),
    Response = jsx:decode(RespBody, [{return_maps, false}]),
    decode_response(Response, []).

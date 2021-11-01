%%%-------------------------------------------------------------------
%% @doc ddb public API
%% @end
%%%-------------------------------------------------------------------

-module(ddb_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    % Fields = attribute:fields([{"Age", attribute:number(42)}]),
    % attribute:map(Fields),
    ddb_sup:start_link().

stop(_State) ->
    ok.

%% internal functions

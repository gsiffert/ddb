-module(table_server).

-behaviour(gen_server).

%% API
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-record(state, {tableName, maxConn, amountConn}).
% start(Name) ->
%     _sup:start_child(Name).

% stop(Name) ->
%     gen_server:call(Name, stop).

% start_link(Name) ->
%     gen_server:start_link({local, Name}, ?MODULE, [], []).

init(_Args) ->
    {ok, #state{}}.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-module(httpconn).

-behaviour(gen_server).

%% API
-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-record(state, {connPid, parentPid}).

post(Pid, Path, Headers, Body) ->
    gen_server:call(Pid, {post, Path, Headers, Body}).

start_link(Name, Host, Port) ->
    gen_server:start_link({local, Name}, ?MODULE, [Host, Port], []).

init([Host, Port]) ->
    {ok, ConnPid} = gun:open(Host, Port, #{protocols => [http2]}),
    {ok, #state{connPid=ConnPid}}.

handle_call({post, Path, Headers, Body}, _From, #state{connPid=Connection, parentPid=Parent} = State) ->
    StreamRef = gun:post(Connection, Path, Headers, Body),
    Response = case gun:await(Connection, StreamRef) of
        {response, fin, Status, _} ->
            {Status, nobody};
        {response, nofin, Status, _} ->
            {ok, RespBody} = gun:await_body(Connection, StreamRef),
            {Status, RespBody}
    end,
    gen_server:cast(Parent, {available, self()}),
    {reply, Response, State};
handle_call(_Request, _From, State) ->
    {noreply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({gun_down, _, _, _, _}, State) ->
    io:format("disconnected~n"),
    {stop, normal, State};
handle_info({gun_up, _, Protocol}, State) ->
    io:format("connected: ~p~n", [Protocol]),
    {noreply, State};
handle_info(Info, State) ->
    io:format("[info]: ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, #state{connPid=Connection}) ->
    gun:close(Connection).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

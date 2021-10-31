-module(httpconn_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    SupervisorSpecification = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 60
    },

    ChildSpecifications = [
        #{
            id => httpconn,
            start => {httpconn, start_link, []},
            restart => permanent,
            shutdown => 2000,
            type => worker,
            modules => [httpconn]
        }
    ],

    {ok, {SupervisorSpecification, ChildSpecifications}}.

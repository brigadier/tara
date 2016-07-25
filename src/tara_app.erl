%%%-------------------------------------------------------------------
%% @doc tara public API
%% @end
%%%-------------------------------------------------------------------

-module(tara_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
	start_pools(),
	tara_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
	ok.

%%====================================================================
%% Internal functions
%%====================================================================

start_pools() ->
	Pools = application:get_env(tara, pools, []),
	lists:foreach(
		fun({PoolName, [PoolOptions, Args]}) ->
			Size = proplists:get_value(size, PoolOptions, 10),
			SupFlags = proplists:get_value(sup_flags, PoolOptions, {one_for_one, 1, 5}),
			ok = simplepool:start_pool(
				PoolName,
				Size,
				tara_worker,
				Args,
				SupFlags
			)
		end,
		Pools
	).
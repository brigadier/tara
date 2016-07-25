-module(tara_util).

%% API
-export([option/3]).


option(Key, M, Default) when is_map(M) ->
	maps:get(Key, M, Default);

option(Key, M, Default) when is_list(M) ->
	proplists:get_value(Key, M, Default).
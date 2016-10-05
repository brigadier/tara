-module(tara_SUITE).
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("tara.hrl").
-include_lib("tara_prot.hrl").

%% API
-compile(export_all).

groups() -> [
	{test, [], [
		testct,
		test_sync,
		test_start,
		test_call_noreply,
		test_async,
		test_call
	]},
	{load, [], [
		load999,
		load_read_write
	]}
].

suite() ->
     [{require, tarantool}, {require, terminal}, {require, listen}, {require, start}].

all() -> [
	{group, test}
%%	 , {group, load}
].

testct(_Config) ->
	{error, not_connected} = tara:select(pool1, 512, [1]),
	waitconnect(pool1),

	#tara_response{data = [{?IPROTO_DATA, []}]} = tara:select(pool2, 512, [1]),
	#tara_response{data = [{?IPROTO_DATA, [[1, <<"abc">>]]}]} = tara:insert(pool2, 512, [1, <<"abc">>]),
	#tara_error{code = 32771} = tara:insert(pool2, 512, [1, <<"abc">>]),

	process_flag(trap_exit, true),
	lists:foreach(
		fun(I) ->
			spawn_link(
				fun() ->
					lists:foreach(
						fun(J) ->
							N = I*10000 + J,
							#tara_response{data = [{?IPROTO_DATA, [[N, <<"abc">>]]}]} = tara:insert(pool2, 512, [N, <<"abc">>])
						end,
						lists:seq(1, 200)
					)
				end
			)
		end,
		lists:seq(1, 6)
	),
	lists:foreach(
		fun(_) ->
			receive
				{'EXIT', _, normal} -> ok;
				X -> exit(X)
			end
		end,
		lists:seq(1, 6)
	).


load_read_write(_Config) ->
	L = lists:seq(1, 20000),

	waitconnect(pool2),

	%%20000 tuples with the same first part of index
	lists:foreach(
		fun(I) ->
			Tuple = [0, term_to_binary({I, <<"abcdefgh">>, abc})],
			#tara_response{data = [{?IPROTO_DATA, [Tuple]}]} = tara:insert(pool2, 512, Tuple)
		end,
		L
	),

	process_flag(trap_exit, true),

	%%read by 1000 tuples by 20 processes
	lists:foreach(
		fun(_) ->
			spawn_link(
				fun() ->
					lists:foreach(
						fun(_) ->
							#tara_response{data = [{?IPROTO_DATA, [_|_]}]} = tara:select(pool2, 512, [0], [{limit, 1000}]),
							#tara_response{data = [{?IPROTO_DATA, [_]}]} = tara:replace(pool2, 512, [0, <<"a">>, <<"b">>])
						end,
						lists:seq(1, 1000)
					)
				end
			)
		end,
		lists:seq(1, 20)
	),
	lists:foreach(
		fun(_) ->
			receive
				{'EXIT', _, normal} -> ok;
				X -> exit(X)
			end
		end,
		lists:seq(1, 20)
	),


	ok.
test_async(_Config) ->
	waitconnect(pool2),
	Tuple1 = [555, <<"a">>, 1, <<"b">>, 1.1],
	ok = tara:async_insert(pool2, 512, Tuple1, self(), first_insert),
	receive
		{tara, first_insert, #tara_response{data = [{?IPROTO_DATA, [Tuple1]}]}} -> ok;
		_ -> exit(invalid_response)
	after 1000 -> exit(timeout)
	end,

	ok = tara:async_insert(pool2, 512, Tuple1, self(), first_insert),
	receive
		{tara, first_insert, #tara_error{}} -> ok;
		_ -> exit(invalid_response)
	after 1000 -> exit(timeout)
	end,

	Tuple2 = [444, <<"a">>, 1, <<"b">>, 1.1],
	ok = tara:async_insert(pool2, 512, Tuple2, undefined, 123),
	receive
		R -> exit({unexpected_response, R})
	after 1000 -> ok
	end,

	ok.

test_sync(_Config) ->
	waitconnect(pool2),
	Tuple1 = [100, <<"a">>, 1, <<"b">>, 1.1],
	#tara_response{data = [{?IPROTO_DATA, [Tuple1]}]} = tara:insert(pool2, 512, Tuple1),
	#tara_response{data = [{?IPROTO_DATA, [Tuple1]}]} = tara:select(pool2, 512, [100]),

	{ok, Tuple1} = tara:get(pool2, 512, [100]),


	#tara_response{data = [{?IPROTO_DATA, []}]} = tara:select(pool2, 512, [99]),

	Tuple2 = [100, <<"b">>, 2, <<"c">>, 2.1],
	tara:insert(pool2, 512, Tuple2),
	#tara_response{data = [{?IPROTO_DATA, [_, _]}]} = tara:select(pool2, 512, [100]), %% two tuples
	#tara_response{data = [{?IPROTO_DATA, [Tuple2]}]} = tara:select(pool2, 512, [100, <<"b">>]), %% one tuple

	#tara_error{} = tara:insert(pool2, 512, Tuple2),

	Tuple3 = [100, <<"b">>, 5],
	#tara_response{data = [{?IPROTO_DATA, [Tuple3]}]} = tara:replace(pool2, 512, Tuple3),
	#tara_response{data = [{?IPROTO_DATA, [Tuple3]}]} = tara:select(pool2, 512, [100, <<"b">>]),

	Tuple4 = [101, <<"a">>, 5],
	#tara_response{data = [{?IPROTO_DATA, [Tuple4]}]} = tara:replace(pool2, 512, Tuple4),
	#tara_response{data = [{?IPROTO_DATA, [Tuple4]}]} = tara:select(pool2, 512, [101, <<"a">>]),


	#tara_response{data = [{?IPROTO_DATA, [Tuple4]}]} = tara:delete(pool2, 512, [101, <<"a">>]),
	#tara_response{data = [{?IPROTO_DATA, []}]} = tara:select(pool2, 512, [101, <<"a">>]),


	#tara_response{data = [{?IPROTO_DATA, [[100, <<"b">>, 16]]}]} = tara:update(pool2, 512, [100, <<"b">>], [?OP_ADD(2, 11)]),
	#tara_error{} = tara:update(pool2, 512, [100, <<"b">>], [?OP_ADD(4, 11)]),

	#tara_response{data = [{?IPROTO_DATA, []}]} = tara:upsert(pool2, 512, [100, <<"b">>, 1000], [?OP_ADD(2, 11)]),
	#tara_response{data = [{?IPROTO_DATA, [[100, <<"b">>, 27]]}]} =tara:select(pool2, 512, [100, <<"b">>]),


	#tara_response{data = [{?IPROTO_DATA, []}]} = tara:upsert(pool2, 512, [111, <<"b">>, 1000], [?OP_ADD(2, 11)]),
	#tara_response{data = [{?IPROTO_DATA, [[111, <<"b">>, 1000]]}]} = tara:select(pool2, 512, [111, <<"b">>]),


	ok.


-ifdef(TARANTOOL_V172CALL).
-define(CALLRESULT(X), X).
-define(CALLMSG, "== call 0x10 for tarantool >= 1.7.2 ==").
-else.
-define(CALLRESULT(X), [X]).
-define(CALLMSG, "== call 0x06 for tarantool < 1.7.2 ==").
-endif.

test_call(_Config) ->
	?debugMsg(?CALLMSG),
	waitconnect(pool2),
	#tara_response{data = [{?IPROTO_DATA, [?CALLRESULT(6)]}]} = tara:call(pool2, <<"testfunc">>, [1,2,3]),
	#tara_response{data = [{?IPROTO_DATA, [?CALLRESULT(6)]}]} = tara:call(pool2, <<"testfunc">>, [1,2,3,4,5,6,7,8,9,0]),
	#tara_error{} = tara:call(pool2, <<"testfunc1">>, [1,2,3]),
	#tara_error{} = tara:call(pool2, <<"testfunc">>, [1,2]),
	ok.



test_call_noreply(_Config) ->
	waitconnect(pool2),
	{_, Workers, undefined} = simplepool:pool(pool2),

	%%ensure same worker, slow function is called first, fast one is next. Response should be first fast, next slow
	%%as the worker process is not blocked, only caller is blocked ({noreply, State} response for gen_server:call)
	Worker = element(1, Workers),
	process_flag(trap_exit, true),
	Pid1 = spawn_link(
		fun() ->
			#tara_response{data = [{?IPROTO_DATA, [?CALLRESULT(12)]}]} = tara:call({proc, Worker}, <<"slowfunc">>, [1, 2, 3])
		end
	),

	Pid2 = spawn_link(
		fun() ->
			#tara_response{data = [{?IPROTO_DATA, [?CALLRESULT(6)]}]} = tara:call({proc, Worker}, <<"testfunc">>, [1, 2, 3])
		end
	),

	receive
		{'EXIT', Pid2, normal} ->
			receive
				{'EXIT', Pid1, normal} ->
					ok;
				X ->
					exit(X)
			end;
		X ->
			exit(X)
	end,
	ok.

test_start(_Config) ->
	TArgs = [{addr, "localhost"},
		{port, 3301},
		{username, <<"manager">>},
		{password, <<"abcdef">>}
	],
	ok = tara:start_pool(localpool, [{size, 5}, {sup_flags, {one_for_one, 1, 5}}], TArgs),
	waitconnect(localpool),
	#tara_response{} = tara:select(localpool, 512, [777,<<"zzz">>]),

	ok = tara:start_pool(global, globalpool, [{size, 5}, {sup_flags, {one_for_one, 1, 5}}], TArgs),
	waitconnect(globalpool),
	#tara_response{} = tara:select(globalpool, 512, [777,<<"zzz">>]),

	tara:stop_pool(localpool),
	not_found = simplepool:pool(localpool),
	case catch tara:select(localpool, 512, [777,<<"zzz">>]) of
		{'EXIT', {noproc, _}} -> ok
	end,


	ok.

load999(_Config) ->
	waitconnect(pool2),
	?debugVal(calendar:local_time()),

	process_flag(trap_exit, true),
	lists:foreach(
		fun(I) ->
			spawn_link(
				fun() ->
					lists:foreach(
						fun(J) ->
							N = I*10000 + J,
							T1 = term_to_binary({N, abcdef}),
							T2 = term_to_binary({abcdef, N, abcdef}),
							Tuple = [N, T1, T2],
							#tara_response{data = [{_, [Tuple]}]} = tara:insert(pool2, 512, Tuple)
						end,
						lists:seq(1, 999)
					)
				end
			)
		end,
		lists:seq(1, 1000)
	),
	lists:foreach(
		fun(_) ->
			receive
				{'EXIT', _, normal} -> ok;
				X -> exit(X)
			end
		end,
		lists:seq(1, 1000)
	),

	?debugVal(calendar:local_time()),
	ok.



init_per_suite(Config) ->
	DataDir =  ?config(data_dir, Config),
	maybe_stop(DataDir),
	Tarantool = ct:get_config(tarantool),
	Terminal = ct:get_config(terminal),
	Listen = ct:get_config(listen),

	maybe_start(DataDir, Listen, Terminal, Tarantool),


	application:load(tara),
	application:set_env(
		tara,
		pools,
		[
			{
				pool1, [
				[
					{size, 2},
					{sup_flags, {one_for_one, 1, 5}}
				],
				[
					{addr, "localhost"},
					{port, 3301},
					{username, <<"manager">>},
					{password, <<"wrong">>}
				]]
			},
			{
				pool2, [
				[
					{size, 5},
					{sup_flags, {one_for_one, 1, 5}}
				],
				[
					{addr, "localhost"},
					{port, 3301},
					{username, <<"manager">>},
					{password, <<"abcdef">>}
				]]
			}
		]
	),

	tara:start(),
	Config.

end_per_suite(Config) ->
	application:stop(tara),
	application:stop(simplepool),
	DataDir =  ?config(data_dir, Config),
	maybe_stop(DataDir),
	ok.



init_per_group(_GroupName, Config) ->
	Config.


end_per_group(_GroupName, _Config) ->
	ok.


init_per_testcase(_TestCase, Config) ->
	Config.

end_per_testcase(_TestCase, _Config) ->
	ok.


pid_file(DataDir) -> filename:join(DataDir, "tarapid.pid").

make_script_lua(DataDir, Listen) ->
	WorkDir = filename:join(DataDir, "db") ++ "/",
	filelib:ensure_dir(WorkDir),
	Pid = pid_file(DataDir),
	ScriptLua = filename:join(DataDir, "script.lua"),
	{ok, Data} = file:read_file(ScriptLua),
	file:write_file(ScriptLua, io_lib:format(Data, [Listen, Pid, WorkDir])),
	ScriptLua.


tarakill(DataDir) ->
	Pid = pid_file(DataDir),
	os:cmd("pkill -F " ++ Pid),
	os:cmd("rm -rf " ++ filename:join(DataDir, "db")).


maybe_start(DataDir, Listen, Terminal, Tarantool) ->
	Start = ct:get_config(start),
	if
		Start ->
			tarakill(DataDir),
			WorkDir = filename:join(DataDir, "db") ++ "/",
			filelib:ensure_dir(WorkDir),
			Pid = pid_file(DataDir),
			ScriptLua = filename:join(DataDir, "script.lua"),
			{ok, Data} = file:read_file(ScriptLua),
			file:write_file(ScriptLua, io_lib:format(Data, [Listen, Pid, WorkDir])),
			os:cmd(Terminal ++ " -e '" ++ Tarantool ++ " " ++ ScriptLua ++ "'");
		true -> ok
	end.

maybe_stop(DataDir) ->
	Start = ct:get_config(start),
	if
		Start -> tarakill(DataDir);
		true -> ok
	end.



waitconnect(Pool) ->
	waitconnect(Pool, 30).

waitconnect(Pool, N) when N > 0 ->
	Answer = tara:state(pool2),
	case lists:all(
		fun(#{connected := Connected}) ->
			Connected
		end,
		Answer
	) of
		true -> ok;
		false ->
			timer:sleep(100),
			waitconnect(Pool, N-1)
	end.
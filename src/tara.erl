-module(tara).
-include_lib("eunit/include/eunit.hrl").
-include("tara_prot.hrl").
-include("tara.hrl").
%% API
-export([start/0]).
-export([select/4, select/3, insert/3, replace/3, update/4, update/5, delete/4, delete/3, call/3, eval/3, upsert/4]).
-export([async_select/5, async_select/6, async_insert/5, async_replace/5, async_delete/5, async_delete/6, async_update/6, async_update/7, async_upsert/6, async_call/5, async_eval/5]).
-export([get/3, get/4, state/1]).
-export([stop_pool/1, start_pool/3, start_pool/4]).
-type server_ref() :: atom() | {atom() | node()} | {global, atom()} | {via, atom(), term()}.

start() ->
	true = ensure_started(tara).


start_pool(Name, PoolArgs, TarantoolArgs) ->
	start_pool(local, Name, PoolArgs, TarantoolArgs).

start_pool(Visibility, Name, PoolArgs, TarantoolArgs) ->
	Size = tara_util:option(size, PoolArgs, 10),
	SupFlags = tara_util:option(sup_flags, PoolArgs, {one_for_one, 1, 5}),
	simplepool:start_pool(Visibility, Name, Size, tara_worker, TarantoolArgs, SupFlags).



stop_pool(PoolName) ->
	simplepool:stop_pool(PoolName).



state({proc, Worker}) ->
	tara_worker:state(Worker);

state(Pool) ->
	{_, Workers} = simplepool:pool(Pool),
	[tara_worker:state(Worker) || Worker <- tuple_to_list(Workers)].


get(Pool, SpaceID, Key) ->
	tara:get(Pool, SpaceID, Key, #{limit => 1, offset => 0}).

get(Pool, SpaceID, Key, Options) ->
	case select(Pool, SpaceID, Key, merge_for_get(Options)) of
		#tara_response{data = [{?IPROTO_DATA, Data}]} ->
			case Data of
				[] -> not_found;
				[T|_] -> {ok, T}
			end;
		Result ->
			Result
	end.


-spec select(atom()|{proc, server_ref()}, integer(), list()) -> {error, term()} | #tara_response{} | #tara_error{}.
select(Pool, SpaceID, Key) ->
	select(Pool, SpaceID, Key, []).

-spec select(atom()|{proc, server_ref()}, integer(), list(), list()|map()) -> {error, term()} | #tara_response{} | #tara_error{}.
select(Pool, SpaceID, Key, Options) when is_integer(SpaceID), is_list(Key) ->
	Body = tara_prot:request_select(SpaceID, Key, Options),
	sync_request(maybe_worker(Pool), ?REQUEST_TYPE_SELECT, Body).


-spec insert(atom()|{proc, server_ref()}, integer(), list()) -> {error, term()} | #tara_response{} | #tara_error{}.
insert(Pool, SpaceID, Tuple) when is_integer(SpaceID) ->
	Body = tara_prot:request_insert_replace(SpaceID, Tuple),
	sync_request(maybe_worker(Pool), ?REQUEST_TYPE_INSERT, Body).


-spec replace(atom()|{proc, server_ref()}, integer(), list()) -> {error, term()} | #tara_response{} | #tara_error{}.
replace(Pool, SpaceID, Tuple) when is_integer(SpaceID) ->
	Body = tara_prot:request_insert_replace(SpaceID, Tuple),
	sync_request(maybe_worker(Pool), ?REQUEST_TYPE_REPLACE, Body).


-spec delete(atom()|{proc, server_ref()}, integer(), list()) -> {error, term()} | #tara_response{} | #tara_error{}.
delete(Pool, SpaceID, Key) ->
	delete(Pool, SpaceID, Key, 0).

-spec delete(atom()|{proc, server_ref()}, integer(), list(), integer()) -> {error, term()} | #tara_response{} | #tara_error{}.
delete(Pool, SpaceID, Key, Index) when is_integer(SpaceID), is_integer(Index), is_list(Key)  ->
	Body = tara_prot:request_delete(SpaceID, Key, Index),
	sync_request(maybe_worker(Pool), ?REQUEST_TYPE_DELETE, Body).


%%see  tara.hrl for ops macroses
-spec update(atom()|{proc, server_ref()}, integer(), list(), list()) -> {error, term()} | #tara_response{} | #tara_error{}.
update(Pool, SpaceID, Key, Ops) ->
	update(Pool, SpaceID, Key, Ops, 0).

-spec update(atom()|{proc, server_ref()}, integer(), list(), list(), integer()) -> {error, term()} | #tara_response{} | #tara_error{}.
update(Pool, SpaceID, Key, Ops, Index) when is_integer(SpaceID), is_integer(Index), is_list(Key), is_list(Ops) ->
	Body = tara_prot:request_update(SpaceID, Key, Ops, Index),
	sync_request(maybe_worker(Pool), ?REQUEST_TYPE_UPDATE, Body).


-spec upsert(atom()|{proc, server_ref()}, integer(), list(), list()) -> {error, term()} | #tara_response{} | #tara_error{}.
upsert(Pool, SpaceID, Tuple, Ops) when is_integer(SpaceID), is_list(Tuple), is_list(Ops) ->
	Body = tara_prot:request_upsert(SpaceID, Tuple, Ops),
	sync_request(maybe_worker(Pool), ?REQUEST_TYPE_UPSERT, Body).


-spec call(atom()|{proc, server_ref()}, binary(), list()) -> {error, term()} | #tara_response{} | #tara_error{}.
call(Pool, Function, Args) when is_binary(Function), is_list(Args) ->
	Body = tara_prot:request_call(Function, Args),
	sync_request(maybe_worker(Pool), ?REQUEST_TYPE_CALL, Body).


-spec eval(atom()|{proc, server_ref()}, binary(), list()) -> {error, term()} | #tara_response{} | #tara_error{}.
eval(Pool, Expr, Args) when is_binary(Expr), is_list(Args) ->
	Body = tara_prot:request_eval(Expr, Args),
	sync_request(maybe_worker(Pool), ?REQUEST_TYPE_EVAL, Body).

%%====================================================================
%% Async
%%====================================================================

-spec async_select(atom()|{proc, server_ref()}, integer(), list(), undefined|pid(), term()) -> {error, term()} | #tara_response{} | #tara_error{}.
async_select(Pool, SpaceID, Key, ReplyTo, Tag) ->
	async_select(Pool, SpaceID, Key, [], ReplyTo, Tag).

-spec async_select(atom()|{proc, server_ref()}, integer(), list(), list()|map(), undefined|pid(), term()) -> {error, term()} | #tara_response{} | #tara_error{}.
async_select(Pool, SpaceID, Key, Options, ReplyTo, Tag) when is_integer(SpaceID), is_list(Key) ->
	Body = tara_prot:request_select(SpaceID, Key, Options),
	async_request(maybe_worker(Pool), ?REQUEST_TYPE_SELECT, Body, ReplyTo, Tag).


-spec async_insert(atom()|{proc, server_ref()}, integer(), list(), undefined|pid(), term()) -> {error, term()} | #tara_response{} | #tara_error{}.
async_insert(Pool, SpaceID, Tuple, ReplyTo, Tag) when is_integer(SpaceID) ->
	Body = tara_prot:request_insert_replace(SpaceID, Tuple),
	async_request(maybe_worker(Pool), ?REQUEST_TYPE_INSERT, Body, ReplyTo, Tag).


-spec async_replace(atom()|{proc, server_ref()}, integer(), list(), undefined|pid(), term()) -> {error, term()} | #tara_response{} | #tara_error{}.
async_replace(Pool, SpaceID, Tuple, ReplyTo, Tag) when is_integer(SpaceID) ->
	Body = tara_prot:request_insert_replace(SpaceID, Tuple),
	async_request(maybe_worker(Pool), ?REQUEST_TYPE_REPLACE, Body, ReplyTo, Tag).


-spec async_delete(atom()|{proc, server_ref()}, integer(), list(), undefined|pid(), term()) -> {error, term()} | #tara_response{} | #tara_error{}.
async_delete(Pool, SpaceID, Key, ReplyTo, Tag) ->
	async_delete(Pool, SpaceID, Key, 0, ReplyTo, Tag).

-spec async_delete(atom()|{proc, server_ref()}, integer(), list(), integer(), undefined|pid(), term()) -> {error, term()} | #tara_response{} | #tara_error{}.
async_delete(Pool, SpaceID, Key, Index, ReplyTo, Tag) when is_integer(SpaceID), is_integer(Index), is_list(Key)  ->
	Body = tara_prot:request_delete(SpaceID, Key, Index),
	async_request(maybe_worker(Pool), ?REQUEST_TYPE_DELETE, Body, ReplyTo, Tag).


%%see  tara.hrl for ops macroses
-spec async_update(atom()|{proc, server_ref()}, integer(), list(), list(), undefined|pid(), term()) -> {error, term()} | #tara_response{} | #tara_error{}.
async_update(Pool, SpaceID, Key, Ops, ReplyTo, Tag) ->
	async_update(Pool, SpaceID, Key, Ops, 0, ReplyTo, Tag).

-spec async_update(atom()|{proc, server_ref()}, integer(), list(), list(), integer(), undefined|pid(), term()) -> {error, term()} | #tara_response{} | #tara_error{}.
async_update(Pool, SpaceID, Key, Ops, Index, ReplyTo, Tag) when is_integer(SpaceID), is_integer(Index), is_list(Key), is_list(Ops) ->
	Body = tara_prot:request_update(SpaceID, Key, Ops, Index),
	async_request(maybe_worker(Pool), ?REQUEST_TYPE_UPDATE, Body, ReplyTo, Tag).


-spec async_upsert(atom()|{proc, server_ref()}, integer(), list(), list(), undefined|pid(), term()) -> {error, term()} | #tara_response{} | #tara_error{}.
async_upsert(Pool, SpaceID, Tuple, Ops, ReplyTo, Tag) when is_integer(SpaceID), is_list(Tuple), is_list(Ops) ->
	Body = tara_prot:request_upsert(SpaceID, Tuple, Ops),
	async_request(maybe_worker(Pool), ?REQUEST_TYPE_UPSERT, Body, ReplyTo, Tag).


-spec async_call(atom()|{proc, server_ref()}, binary(), list(), undefined|pid(), term()) -> {error, term()} | #tara_response{} | #tara_error{}.
async_call(Pool, Function, Args, ReplyTo, Tag) when is_binary(Function), is_list(Args) ->
	Body = tara_prot:request_call(Function, Args),
	async_request(maybe_worker(Pool), ?REQUEST_TYPE_CALL, Body, ReplyTo, Tag).


-spec async_eval(atom()|{proc, server_ref()}, binary(), list(), undefined|pid(), term()) -> {error, term()} | #tara_response{} | #tara_error{}.
async_eval(Pool, Expr, Args, ReplyTo, Tag) when is_binary(Expr), is_list(Args) ->
	Body = tara_prot:request_eval(Expr, Args),
	async_request(maybe_worker(Pool), ?REQUEST_TYPE_EVAL, Body, ReplyTo, Tag).


%%====================================================================
%% Internal functions
%%====================================================================
merge_for_get(Options) when is_map(Options) -> Options#{limit => 1, offset => 0};
merge_for_get(Options) when is_list(Options) -> [{limit, 1}, {offset, 0} | Options].


sync_request(Worker, RequestType, Body) when is_binary(Body) ->
	tara_worker:sync_request(Worker, RequestType, Body);
sync_request(_Worker, _RequestType, {error, Body}) ->
	{error, Body}.


async_request(Worker, RequestType, Body, ReplyTo, Tag) when is_binary(Body) ->
	tara_worker:async_request(Worker, RequestType, Body, ReplyTo, Tag);
async_request(_Worker, _RequestType, {error, Body}, _ReplyTo, _Tag) ->
	{error, Body}.


maybe_worker({proc, Worker}) -> Worker;
maybe_worker(Pool) -> simplepool:rand_worker(Pool).




ensure_deps_started(App) ->
	Deps = case application:get_key(App, applications) of
			   undefined -> [];
			   {_, V} -> V
		   end,
	lists:all(fun ensure_started/1,Deps).

ensure_started(App) ->
	application:load(App),
	ensure_deps_started(App)
		andalso case application:start(App) of
					ok ->
						true;
					{error, {already_started, App}} ->
						true;
					Else ->
						error_logger:error_msg("Couldn't start ~p: ~p", [App, Else]),
						false
				end.
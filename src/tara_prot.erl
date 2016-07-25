-module(tara_prot).
-include_lib("eunit/include/eunit.hrl").
-include("tara_prot.hrl").
-include("tara.hrl").
-define(TARA_LIMIT, 16#FFFF).
%% API
-export([request_auth/3, unpack/1, header/3, request_select/3, request_insert_replace/2, request_update/4, request_delete/3, request_call/2, request_eval/2, request_upsert/3]).

-define(MPACKOPTS, [{spec, old}, {map_format, jsx}]).
-define(CHAP, <<"chap-sha1">>).

unpack(<<TotalSZ:5/binary, Rest/binary>> = _Data) ->
	case msgpack:unpack(TotalSZ, ?MPACKOPTS) of
		{ok, N} when is_integer(N), N =< byte_size(Rest) ->
			case msgpack:unpack_stream(Rest, ?MPACKOPTS) of
				{[{?IPROTO_CODE, Code}, {?IPROTO_SYNC, Sync}, {?IPROTO_SCHEMA_ID, SchemaID}] = _Head, BinBody} ->
					case msgpack:unpack_stream(BinBody, ?MPACKOPTS) of
						{Body, Tail} ->
							{Sync, Tail, prettify(Code, SchemaID, Body)};
						_ ->
							trash
					end;
				_ ->
					trash
			end;
		{ok, N} when is_integer(N) ->
			incomplete;
		_ ->
			trash
	end;

unpack(_Data) ->
	incomplete.


header(ReqType, Sync, Body) ->
	SzBody = byte_size(Body),

	Head = msgpack:pack(
		[
			{?IPROTO_CODE, ReqType},
			{?IPROTO_SYNC, Sync}
		],
		?MPACKOPTS
	),

	SzHead = byte_size(Head),
	HeadSZMap = msgpack:pack(SzBody + SzHead),
	<<HeadSZMap/binary, Head/binary, Body/binary>>.


request_auth(Username, Password, Salt) ->
	Hash1 = crypto:hash(sha, Password),
	Hash2 = crypto:hash(sha, Hash1),
	Scramble = scramble(Salt, Hash1, Hash2),
	msgpack:pack(
		[
			{?IPROTO_TUPLE, [?CHAP, Scramble]},
			{?IPROTO_USER_NAME, Username}

		],
		?MPACKOPTS
	).

request_select(SpaceID, Key, Params) ->
	Index = tara_util:option(index, Params, 0),
	Limit =  tara_util:option(limit, Params, ?TARA_LIMIT),
	Offset = tara_util:option(offset, Params, 0),
	Iterator = tara_util:option(iterator, Params, 0),

	true = is_integer(Index),
	true = is_integer(Limit),
	true = is_integer(Offset),
	true = is_integer(Iterator),

	do_request_select(SpaceID, Key, Index, Limit, Offset, Iterator).

request_insert_replace(SpaceID, Tuple) ->
	msgpack:pack(
		[
			{?IPROTO_SPACE_ID, SpaceID},
			{?IPROTO_TUPLE, Tuple}
		],
		?MPACKOPTS
	).

request_update(SpaceID, Key, Ops, Index) ->
	msgpack:pack(
		[
			{?IPROTO_SPACE_ID, SpaceID},
			{?IPROTO_INDEX_ID, Index},
			{?IPROTO_KEY, Key},
			{?IPROTO_TUPLE, Ops}
		],
		?MPACKOPTS
	).

request_delete(SpaceID, Key, Index) ->
	msgpack:pack(
		[
			{?IPROTO_SPACE_ID, SpaceID},
			{?IPROTO_INDEX_ID, Index},
			{?IPROTO_KEY, Key}
		],
		?MPACKOPTS
	).

request_call(Function, Args) ->
	msgpack:pack(
		[
			{?IPROTO_FUNCTION_NAME, Function},
			{?IPROTO_TUPLE, Args}
		],
		?MPACKOPTS
	).

request_eval(Expr, Args) ->
	msgpack:pack(
		[
			{?IPROTO_EXPR, Expr},
			{?IPROTO_TUPLE, Args}
		],
		?MPACKOPTS
	).

request_upsert(SpaceID, Tuple, Ops) ->
	msgpack:pack(
		[
			{?IPROTO_SPACE_ID, SpaceID},
			{?IPROTO_TUPLE, Tuple},
			{?IPROTO_OPS, Ops}
		],
		?MPACKOPTS
	).

%%====================================================================
%% Internal functions
%%====================================================================

do_request_select(SpaceID, Key, Index, Limit, Offset, Iterator) when is_list(Key) ->
	msgpack:pack(
		[
			{?IPROTO_SPACE_ID, SpaceID},
			{?IPROTO_INDEX_ID, Index},
			{?IPROTO_LIMIT, Limit},
			{?IPROTO_OFFSET, Offset},
			{?IPROTO_ITERATOR, Iterator},
			{?IPROTO_KEY, Key}
		],
		?MPACKOPTS
	).


scramble(Salt, Hash1, Hash2) ->
	HashFinal = crypto:hash(sha, <<Salt/binary, Hash2/binary>>),
	crypto:exor(Hash1, HashFinal).


prettify(Code, SchemaID, Body) when Code >= ?REQUEST_TYPE_ERROR ->
	#tara_error{
		code = Code,
		schema = SchemaID,
		message = case Body of
					  [{_, Msg}] -> Msg;
					  _ -> <<"Unknown error">>
				  end
	};

prettify(Code, SchemaID, Body)->
	#tara_response{code = Code, schema = SchemaID, data = Body}.




scramble_test_() ->
	[
		{"scramble",
			?_assertEqual(<<73, 6, 160, 99, 80, 168, 156, 30, 158, 59, 1, 27, 22, 226, 126, 13, 25, 111, 139, 31>>,
				scramble(
					<<64, 87, 165, 65, 237, 4, 84, 171, 111, 218, 0, 228, 98, 235, 13, 202, 176, 136, 133, 155>>,
					crypto:hash(sha, <<"abcdef">>),
					crypto:hash(sha, crypto:hash(sha, <<"abcdef">>))
				)
			)
		}

	].
-module(tara_worker).

-behaviour(gen_server).
-behaviour(gen_simplepool_worker).

-include_lib("eunit/include/eunit.hrl").
-include("../include/tara_prot.hrl").
-include("tara.hrl").
%% API
-export([start_link/2, simplepool_start_link/4, start_link/1]).
%% gen_server callbacks
-export([init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3, state/1]).

-export([sync_request/3, async_request/5]).

-define(RECONNECT_AFTER, 500).
-define(MAX_RECONNECT_AFTER, 10000).
-define(TIMEOUT, 5000).
-define(SERVER, ?MODULE).


-record(state, {
	sock = undefined,
	reason = undefined,
	addr,
	port,
	username,
	password,
	salt,
	greeting = undefined,
	sync = 1,
	reconnect_after = ?RECONNECT_AFTER,
	buffer = <<>>,
	timer_id = 1
}).

%%%===================================================================
%%% API
%%%===================================================================
simplepool_start_link(Visibility, Name, _, Args) ->
	gen_server:start_link({Visibility, Name}, ?MODULE, Args, []).

start_link(Name, Args) ->
	gen_server:start_link(Name, ?MODULE, Args, []).

start_link(Args) ->
	gen_server:start_link(?MODULE, Args, []).

sync_request(Worker, RequestType, Body) when is_binary(Body), is_integer(RequestType) ->
	case gen_server:call(Worker, {request, RequestType, Body}, ?TIMEOUT) of
		{tara, {reply, Response}} ->
			Response;
		Result ->
			Result
	end.

async_request(Worker, RequestType, Body, ReplyTo, Tag) when is_binary(Body) andalso is_integer(RequestType) andalso (is_pid(ReplyTo) orelse ReplyTo == undefined) ->
	gen_server:cast(Worker, {request, RequestType, Body, ReplyTo, Tag}).



state(Worker) ->
	gen_server:call(Worker, state).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


init(Args) ->
	Addr = proplists:get_value(addr, Args, "localhost"),
	Port = proplists:get_value(port, Args, 3301),

	Username = proplists:get_value(username, Args, <<"guest">>),
	Password = proplists:get_value(password, Args, <<>>),
	process_flag(trap_exit, true),
	self() ! {reconnect, 1},
	{ok, #state{addr = Addr, port = Port, username = Username, password = Password, timer_id = 1}}.

handle_call(state, _From, #state{sock = Sock, reason = Reason} = State) ->
	Answer = #{
		connected => Sock =/= undefined,
		last_error => Reason
	},
	{reply, Answer, State};

handle_call({request, _, _}, _From, #state{sock = undefined} = State) ->
	{reply, {error, not_connected}, State};


handle_call({request, RequestType, Body}, From, #state{sock = Socket} = State) ->
	#state{sync = Sync} = State2 = next(State),
	inet:setopts(Socket, [{active, false}]),
	Request = tara_prot:header(RequestType, Sync, Body),
	case transaction(Request, 0, 0, Socket) of
		{ok, Response} ->
			put({tara, Sync}, From),
			self() ! {tcp, Socket, Response},
			inet:setopts(Socket, [{active, true}]),
			{noreply, State2};
		{error, timeout} ->
			put({tara, Sync}, From),
			inet:setopts(Socket, [{active, true}]),
			{noreply, State2};
		{error, Reason} ->
			{reply, {error, Reason}, next_reconnect(State, {transaction, Reason})}
	end;


handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast({request, _RequestType, _Body, ReplyTo, Tag}, #state{sock = undefined} = State) ->
	reply({async, ReplyTo, Tag}, {error, not_connected}),
	{noreply, State};

handle_cast({request, RequestType, Body, ReplyTo, Tag}, #state{sock = Socket} = State) ->
	#state{sync = Sync} = State2 = next(State),
	inet:setopts(Socket, [{active, false}]),
	Request = tara_prot:header(RequestType, Sync, Body),


	State3 = case transaction(Request, 0, 0, Socket) of
				 {ok, Response} ->
					 maybe_async_put(ReplyTo, Tag, Sync),
					 self() ! {tcp, Socket, Response},
					 inet:setopts(Socket, [{active, true}]),
					 State2;
				 {error, timeout} ->
					 maybe_async_put(ReplyTo, Tag, Sync),
					 inet:setopts(Socket, [{active, true}]),
					 State2;
				 {error, Reason} ->
					 reply({async, ReplyTo, Tag}, {error, Reason}),
					 next_reconnect(State, {transaction, Reason})
			 end,
	{noreply, State3};

handle_cast(_Request, State) ->
	{noreply, State}.

handle_info({tcp, Socket, Packet}, #state{sock = Socket, buffer = Buffer} = State) ->
	State2 = case fold_unpack(State#state{buffer = <<Buffer/binary, Packet/binary>>}) of
				 {ok, S} ->
					 inet:setopts(Socket, [{active, once}]),
					 S;
				 trash ->
					 next_reconnect(State#state{reconnect_after = 5000}, trash)
			 end,
	{noreply, State2};


handle_info({tcp_closed, Socket}, #state{sock = Socket} = State) ->
	State2 = next_reconnect(State#state{reconnect_after = ?RECONNECT_AFTER}, tcp_closed),
	{noreply, State2};


handle_info({reconnect, TimerID}, #state{timer_id = TimerID, addr = Addr, port = Port} = State) ->
	State2 = case gen_tcp:connect(Addr, Port,
		[{mode, binary},
			{packet, raw},
			{keepalive, true},
			{active, false},
			{exit_on_close, true},
			{send_timeout, ?TIMEOUT},
			{send_timeout_close, true},
			{nodelay, true}
		]) of
				 {ok, Sock} -> handshake(State#state{sock = Sock, buffer = <<>>});
				 {error, Reason} -> next_reconnect(State, Reason)
			 end,
	{noreply, State2};

handle_info(_Msg, State) ->
	{noreply, State}.


terminate(_Reason, _State) ->
	fan_response(),
	ok.



code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

fold_unpack(#state{buffer = Buffer} = State) ->
	case tara_prot:unpack(Buffer) of
		incomplete -> {ok, State};
		trash -> trash;
		{Sync, Tail, Msg} ->
			Key = {tara, Sync},
			case get(Key) of
				undefined ->
					ok;
				From ->
					erase(Key),
					reply(From, Msg)
			end,
			fold_unpack(State#state{buffer = Tail})
	end.

handshake(#state{sock = Sock} = State) ->
	case gen_tcp:recv(Sock, 128, ?TIMEOUT) of
		{error, Reason} -> next_reconnect(State, {greeting, Reason});
		{ok, <<Greeting:63/binary, 10, SaltB64:44/binary, _Rest/binary>>} ->
			case catch base64:decode(SaltB64) of
				{'EXIT', Error} -> next_reconnect(State, {greeting, Error});
				<<Salt:20/binary, _/binary>> ->
					auth(State#state{salt = Salt, greeting = Greeting})
			end
	end.

auth(#state{sock = Socket, salt = Salt, username = UserName, password = Password} = State) ->
	#state{sync = Sync} = State2 = next(State),
	Request = tara_prot:header(?REQUEST_TYPE_AUTHENTICATE, Sync, tara_prot:request_auth(UserName, Password, Salt)),
	case transaction(Request, 3000, 0, Socket) of
		{ok, Response} ->
			case tara_prot:unpack(Response) of
				{Sync, <<>>, #tara_response{}} ->
					inet:setopts(Socket, [{active, once}]),
					State2#state{reason = logged_on};
				{Sync, <<>>, #tara_error{message = Message}} ->
					next_reconnect(State2, {auth, Message});
				Else -> next_reconnect(State2, {auth, Else})
			end;

		{error, Reason} ->
			next_reconnect(State, {auth, Reason})
	end.

transaction(SendPacket, RecvTimeout, RecvSize,  Socket) ->
	case gen_tcp:send(Socket, SendPacket) of
		ok ->
			case gen_tcp:recv(Socket, RecvSize, RecvTimeout) of
				{ok, RecvPacket} -> {ok, RecvPacket};
				Else -> Else %%{error,timeout} is usually ok
			end;
		Else -> Else
	end.

fan_response() ->
	Keys = get(),
	lists:foreach(
		fun
			({{tara, _Sync} = Key, From}) ->
				reply(From, {error, disconnect_before_response}),
				erase(Key);
			(_) ->
				ok
		end,
		Keys
	).

next_reconnect(#state{sock = Sock, timer_id = OldTimerID, reconnect_after = ReconnectAfter} = State, Reason) ->
	fan_response(),

	if
		Sock =/= undefined -> gen_tcp:close(Sock);
		true -> ok
	end,
	TimerID = next(OldTimerID),
	erlang:send_after(ReconnectAfter, self(), {reconnect, TimerID}),
	State#state{
		sock = undefined,
		reason = Reason,
		greeting = undefined,
		salt = undefined,
		reconnect_after = min(?MAX_RECONNECT_AFTER, ReconnectAfter + ?RECONNECT_AFTER),
		buffer = <<>>,
		timer_id = TimerID
	}.

next(#state{sync = Sync} = State) ->
	State#state{sync = next(Sync)};
next(Sync) when Sync >= 16#FFFFFFFF -> 1;
next(Sync) -> Sync + 1.


%%
maybe_async_put(undefined, _Tag, _Sync) -> ok;
maybe_async_put(ReplyTo, Tag, Sync) ->
	put({tara, Sync}, {async, ReplyTo, Tag}).


reply({async, ReplyTo, Tag}, Msg) -> ReplyTo ! {tara, Tag, Msg};
reply(From, Msg) -> gen_server:reply(From, Msg).
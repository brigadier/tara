## TARA: Erlang tarantool connector
=====
Erlang connector application to [Tarantool](http://tarantool.org/) server. Tarantool on github:
https://github.com/tarantool/tarantool

Tested with Tarantool 1.7 branch, Erlang 19.0


#### Features:
* Multiple pools, each with its' own connection options.
* Start/stop pools dynamically or from the application `env`.
* Authentification.
* Autoreconnect with deamplification on connection loss or other problems. 
* Both async and sync mode. The sync mode locks the caller process with `gen_server:call`. Worker in `handle_call`
waits until writing to socket is completed, then tries to read some data from socket with `0` timeout and
returns `{noreply, State}`. The worker does not wait for response for the current request, so slow
operation won't block the worker, but only the caller. The case when you do a slow request
from one process and then a fast one from another, and both requests are sent to the same worker of the pool,
will be handled correctly - the fast one will not wait for the slow one. But 5 seconds timeout of gen_server
still exists, so think about async requests for potentially slow operations. 
* Can use unix domain sockets, if tarantool configured to listen on such socket. Use 
`{local, "/tmp/sock"}` as addr and `0` as port.
* select, delete, insert, call, update, upsert, replace, eval operations.




#### Note:
* Uses [Simplepool](https://github.com/brigadier/simplepool) pools. You might not like it as
`simplepool` uses quite unconventional thing - it compiles pool proc names and other data in a RAM beam module.


Build
-----

    $ rebar3 compile
    
    
If you don't have rebar3 in your path, download it from http://www.rebar3.org/ or build it from sources.  
    
    
Tests
-----
Download and make tarantool. Ensure it is stopped. Edit the `test/test.config` file, change the path to
tarantool and the name of your terminal app. Then run

    $ rebar3 ct    


Load test such as "insert 999000 records " are disabled (commented off in the test module).


Examples
-----
* Include tara/include/tara.hrl - you may need the records and macroses from this file
* In the Sync mode operations return either `{error, Error}`, `#tara_response{}` or `#tara_error{}`.
 - `{error, Error}` gets returned when the server is disconnected or authentication is not yet completed. Also,
if you sent request succesfully but socket disconnected before the worker got response for this
request, you will get the `{error, disconnect_before_response}` response.
 - `#tara_response{}` - successful response with some data. Always contains `[{?IPROTO_DATA, Data}]` in the
`data` field, where `Data` - list of 0 or more lists (named 'tuples' in tarantool terms,
in docs and everywhere) of the result.
 - `#tara_error{}` - tarantool error. Something went wrong - invalid parameters in request, attempt to
insert the tuple which is already exists and anything like that. The error message in the `message` field.
* The `select` operation accepts optional `Options` parameter, with `limit`, `index`, `offset`, `iterator` 
integer fields. The `Options` can be either map or proplist. By default `limit` is equal to `16#FFFF` so
specify some smaller value if your table potentially can have many records matching the query, and use
pagination.
* Response data can be different for the `vinyl` backend.

Async operations have 2 additional parameters: Pid of the process which will get the
result (or `undefined` if no process would listen for the response) and a Tag - any term, to be able to match
on if you need to associate response with the request. The message will look like this: `{tara, Tag, Response}`
where Response is either `{error, Error}`, `#tara_response{}` or `#tara_error{}` 



```erlang
rd(tara_response, {code, schema, data}).
rd(tara_error, {code, schema, message}).
TArgs = [{addr, "localhost"},
		{port, 3301},
		{username, <<"manager">>},
		{password, <<"abcdef">>}
	],
ok = tara:start_pool(pool2, [{size, 5}, {sup_flags, {one_for_one, 1, 5}}], TArgs).
%%connect is async, so right after connection it is in disconnected state. Wait for a while or poll
%%the tara:state(Pool) - see tests. Also note that connection can disconnect and reconnect again any time
timer:sleep(1000).
Tuple1 = [100, <<"a">>, 1, <<"b">>, 1.1].
#tara_response{data = [{_, [Tuple1]}]} = tara:insert(pool2, 512, Tuple1).
#tara_response{data = [{_, [Tuple1]}]} = tara:select(pool2, 512, [100]).
#tara_response{data = [{_, [Tuple1]}]} = tara:delete(pool2, 512, [100, <<"a">>]).
#tara_response{data = [{_, []}]} = tara:select(pool2, 512, [100, <<"a">>]).
ok = tara:async_delete(pool2, 512, [100, <<"a">>], self(), some_tag).
receive
    {tara, some_tag, Response} -> ok;
after 10000 -> exit(timeout)
end.
```
See the tests for more examples on other methods. `tara.hrl` contains useful macroses for the
`update` and `upsert` methods. More info is there: http://tarantool.org/doc/dev_guide/box_protocol.html





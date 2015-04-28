%%
%% tremble
%% tuple-relational mapper
%%
%% Copyright 2015 Alex Wilson <alex@uq.edu.au>, The University of Queensland
%% All rights reserved.
%%
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted provided that the following conditions
%% are met:
%% 1. Redistributions of source code must retain the above copyright
%%    notice, this list of conditions and the following disclaimer.
%% 2. Redistributions in binary form must reproduce the above copyright
%%    notice, this list of conditions and the following disclaimer in the
%%    documentation and/or other materials provided with the distribution.
%%
%% THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
%% IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
%% OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
%% IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
%% INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
%% NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
%% DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
%% THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
%% (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
%% THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
%%

-module(tremble).

-export([start/0, start/1]).
-export([config/1, config/2]).
-export([squery/1, equery/2, transaction/1]).

start() ->
    start(tremble:config(pg_options, [
        {database, "postgres"},
        {host, "localhost"},
        {username, "postgres"},
        {password, "postgres"},
        {timeout, 5000}])).

start(Opts) ->
    ok = application:ensure_started(tremble),
    case lists:keytake(size, 1, Opts) of
        {value, {size, Size}, Opts2} -> ok;
        false -> Size = tremble:config(pool_size, 10), Opts2 = Opts
    end,
    case lists:keytake(overflow, 1, Opts2) of
        {value, {overflow, Overflow}, Opts3} -> ok;
        false -> Overflow = tremble:config(pool_size, 10) div 2, Opts3 = Opts
    end,
    tremble_sup:start_pool(Opts3, Size, Overflow).

dpath_resolve(Item, []) -> Item;
dpath_resolve(List, ['_' | Rest]) ->
    [dpath_resolve(X, Rest) || X <- List];
dpath_resolve(Plist, [Key | Rest]) when is_atom(Key) or is_binary(Key) ->
    dpath_resolve(proplists:get_value(Key, Plist), Rest);
dpath_resolve(List, [Index | Rest]) when is_integer(Index) ->
    dpath_resolve(lists:nth(Index, List), Rest).

config(KeyOrDPath) -> config(KeyOrDPath, undefined).
config([ConfigKey | DPath], Default) ->
    case application:get_env(tremble, ConfigKey) of
        undefined -> Default;
        {ok, Other} -> case dpath_resolve(Other, DPath) of
            undefined -> Default;
            Other2 -> Other2
        end
    end;
config(ConfigKey, Default) ->
    case application:get_env(tremble, ConfigKey) of
        undefined -> Default;
        {ok, Other} -> Other
    end.

squery(Sql) ->
    case get(tr_pgsql_conn) of
        undefined ->
            case fuse:ask(tr_db_fuse, sync) of
                ok ->
                    poolboy:transaction(tr_db_pool, fun(Worker) ->
                        gen_server:call(Worker, {squery, Sql}, 10000)
                    end, 10000);
                blown ->
                    timer:sleep(1000), squery(Sql)
            end;
        Worker ->
            gen_server:call(Worker, {squery, Sql}, 10000)
    end.

equery(Stmt, Params) ->
    case get(tr_pgsql_conn) of
        undefined ->
            case fuse:ask(tr_db_fuse, sync) of
                ok ->
                    poolboy:transaction(tr_db_pool, fun(Worker) ->
                        gen_server:call(Worker, {equery, Stmt, Params}, 10000)
                    end, 10000);
                blown ->
                    timer:sleep(1000), equery(Stmt, Params)
            end;
        Worker ->
            gen_server:call(Worker, {equery, Stmt, Params}, 10000)
    end.

transaction(Fun) ->
    case fuse:ask(tr_db_fuse, sync) of
        ok ->
            poolboy:transaction(tr_db_pool, fun(Worker) ->
                put(tr_post_commit_hooks, []),
                put(tr_pgsql_conn, Worker),
                {ok, _, _} = gen_server:call(Worker, {squery, "begin transaction"}),
                Result = (catch Fun()),
                case Result of
                    {'EXIT', _} ->
                        {ok, _, _} = gen_server:call(Worker, {squery, "rollback"}, 30000);
                    _ ->
                        {ok, _, _} = gen_server:call(Worker, {squery, "commit"}, 30000)
                end,
                erase(tr_pgsql_conn),
                Hooks = get(tr_post_commit_hooks),
                erase(tr_post_commit_hooks),
                case Result of
                    {'EXIT', Reason} ->
                        error(Reason);
                    _ ->
                        [F() || F <- lists:reverse(Hooks)],
                        erase(tr_post_commit_hooks),
                        Result
                end
            end, 30000);
        blown ->
            timer:sleep(1000), transaction(Fun)
    end.

%%
%% tremble
%% tuple-relational mapper
%%
%% Copyright (c) 2015, Alex Wilson
%% Author: Alex Wilson <alex@cooperi.net>
%%

-module(tremble).

-export([config/1, config/2]).
-export([squery/1, equery/2, transaction/1]).

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

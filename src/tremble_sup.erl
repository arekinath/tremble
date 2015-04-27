%%
%% tremble
%% tuple-relational mapper
%%
%% Copyright (c) 2015, Alex Wilson
%% Author: Alex Wilson <alex@cooperi.net>
%%

-module(tremble_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    PgOpts = tremble:config(pg_options, [
        {database, "postgres"},
        {host, "localhost"},
        {username, "postgres"},
        {password, "postgres"},
        {timeout, 5000}]),
    Pool = poolboy:child_spec(tr_db_pool,
            [{name, {local, tr_db_pool}},
             {worker_module, tremble_pool_worker},
             {size, tremble:config(pool_size, 10)},
             {max_overflow, tremble:config(pool_size, 10) div 2}],
            [PgOpts, tr_db_fuse]),
    {ok,
        {{one_for_one, 60, 60},
        [Pool]}}.


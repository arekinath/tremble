%%
%% tremble
%% tuple-relational mapper
%%
%% Copyright (c) 2015, Alex Wilson
%% Author: Alex Wilson <alex@cooperi.net>
%%

-module(tremble_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    fuse:install(tr_db_fuse, { {standard, 10, 10}, {reset, 5000} }),
    tremble_sup:start_link().

stop(_) -> ok.

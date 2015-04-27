%%
%% tremble
%% tuple-relational mapper
%%
%% Copyright (c) 2015, Alex Wilson
%% Author: Alex Wilson <alex@cooperi.net>
%%

-include_lib("epgsql/include/epgsql.hrl").
-type jsonb() :: [{atom()|binary(),jsonb()}].
-type date() :: {Year :: integer(), Month :: integer(), Day :: integer()}.
-type time() :: {Hour :: integer(), Min :: integer(), Second :: float()}.
-type datetime() :: {date(), time()}.

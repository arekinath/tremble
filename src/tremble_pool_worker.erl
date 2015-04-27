%%
%% tremble
%% tuple-relational mapper
%%
%% Copyright (c) 2015, Alex Wilson
%% Author: Alex Wilson <alex@cooperi.net>
%%

-module(tremble_pool_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {c, fuse}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init([PgOpts, FuseName]) ->
    {ok, _} = timer:send_interval(5000, ping),
    case fuse:ask(FuseName, sync) of
        ok -> connect(PgOpts, #state{fuse = FuseName});
        blown -> {ok, #state{fuse = FuseName}}
    end.

connect(PgOpts0, InitialState) ->
    {value, {_, PgHost}, PgOpts1} = lists:keytake(host, 1, PgOpts0),
    {value, {_, PgUser}, PgOpts2} = lists:keytake(username, 1, PgOpts1),
    {value, {_, PgPass}, PgOpts3} = lists:keytake(password, 1, PgOpts2),
    {ok, C} = epgsql:connect(PgHost, PgUser, PgPass, PgOpts3),
    {ok, InitialState#state{c = C}}.

handle_call({squery, _Sql}, _From, #state{c=undefined}=State) ->
    {reply, {error, fuse_blown}, State};
handle_call({squery, Sql}, From, #state{c=Conn,fuse=F}=State) ->
    T1 = os:timestamp(),
    case epgsql:squery(Conn, Sql) of
        {error, closed} ->
            gen_server:reply(From, {error, closed}),
            fuse:melt(F),
            {stop, closed_during_query, State};
        {error, sync_required} ->
            ok = epgsql:sync(Conn),
            handle_call({squery, Sql}, From, State);
        Other ->
            T2 = os:timestamp(),
            gen_server:reply(From, Other),
            case timer:now_diff(T2,T1) of
                I when I > 20000000 ->
                    fuse:melt(F),
                    {stop, too_slow, State};
                _ ->
                    {noreply, State}
            end
    end;
handle_call({equery, _Stmt, _Params}, _From, #state{c=undefined}=State) ->
    {reply, {error, fuse_blown}, State};
handle_call({equery, Stmt, Params}, From, #state{c=Conn,fuse=F}=State) ->
    T1 = os:timestamp(),
    case epgsql:equery(Conn, Stmt, Params) of
        {error, closed} ->
            gen_server:reply(From, {error, closed}),
            fuse:melt(F),
            {stop, closed_during_query, State};
        {error, sync_required} ->
            ok = epgsql:sync(Conn),
            handle_call({equery, Stmt, Params}, From, State);
        Other ->
            T2 = os:timestamp(),
            gen_server:reply(From, Other),
            case timer:now_diff(T2,T1) of
                I when I > 20000000 ->
                    fuse:melt(F),
                    {stop, too_slow, State};
                _ ->
                    {noreply, State}
            end
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(ping, #state{c = undefined,fuse=F}=State) ->
    case fuse:ask(F, sync) of
        ok -> {stop, normal, State};
        blown -> {noreply, State}
    end;
handle_info(ping, #state{c=Conn,fuse=F}=State) ->
    T1 = os:timestamp(),
    case epgsql:squery(Conn, "select 1") of
        {ok, _, [{<<"1">>}]} ->
            T2 = os:timestamp(),
            case timer:now_diff(T2,T1) of
                I when I > 1000000 ->
                    fuse:melt(F),
                    {stop, too_slow_ping, State};
                _ -> {noreply, State}
            end;
        _ ->
            fuse:melt(F),
            {stop, ping_invalid, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{c=undefined}) ->
    ok;
terminate(_Reason, #state{c=Conn}) ->
    ok = epgsql:close(Conn).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

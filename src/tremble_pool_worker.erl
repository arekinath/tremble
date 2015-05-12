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

-module(tremble_pool_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {c, fuse, txclient, txmon}).

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

handle_call(open_tx, _From, #state{c=undefined}=State) ->
    {reply, {error, fuse_blown}, State};
handle_call(open_tx, From = {Pid,_}, #state{txclient=undefined,c=Conn,fuse=F}=State) ->
    case epgsql:squery(Conn, "begin transaction") of
        {error, closed} ->
            gen_server:reply(From, {error, closed}),
            fuse:melt(F),
            {stop, closed_during_query, State};
        Err when is_tuple(Err) and (element(1,Err) =:= error) ->
            {reply, Err, State};
        Res when is_tuple(Res) and (element(1,Res) =:= ok) ->
            gen_server:reply(From, ok),
            MonRef = monitor(process, Pid),
            {noreply, State#state{txclient=Pid,txmon=MonRef}}
    end;
handle_call(open_tx, {Pid,_}, #state{txclient=Pid}=State) ->
    {reply, {error, nested_transaction}, State};
handle_call(open_tx, From = {Pid,_}, #state{txclient=OldPid}=State) ->
    % we must have been released back to the pool and taken by a new process
    % probably best if we just panic and drop the connection
    gen_server:reply(From, {error, {transaction_still_open, OldPid}}),
    {stop, {transaction_stolen, Pid}, State};

handle_call({close_tx,_}, _From, #state{c=undefined}=State) ->
    {reply, {error, fuse_blown}, State};
handle_call({close_tx,Type}, From = {Pid,_}, #state{txclient=Pid,c=Conn,fuse=F}=State) ->
    case epgsql:squery(Conn, Type) of
        {error, closed} ->
            gen_server:reply(From, {error, closed}),
            fuse:melt(F),
            {stop, closed_during_query, State};
        Err when is_tuple(Err) and (element(1,Err) =:= error) ->
            {reply, Err, State};
        Res when is_tuple(Res) and (element(1,Res) =:= ok) ->
            gen_server:reply(From, ok),
            demonitor(State#state.txmon),
            {noreply, State#state{txclient=undefined,txmon=undefined}}
    end;
handle_call({close_tx,_}, From = {Pid, _}, #state{txclient=OldPid}=State) ->
    gen_server:reply(From, {error, {not_transaction_owner, OldPid}}),
    {stop, {transaction_stolen, Pid}, State};

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

handle_info({'DOWN', _, _, _, _}, #state{txmon = undefined}=State) ->
    {noreply, State};
handle_info({'DOWN', Mon, _, Pid, _}, #state{txmon = Mon, txclient = Pid}=State) ->
    {stop, {tx_owner_died, Pid}, State};

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

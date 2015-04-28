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

-module(tremble_transform).
-compile({parse_transform, parse_trans_codegen}).
-export([parse_transform/2]).

-record(state, {
    mod :: atom(),
    out = [],
    recs = [],
    rectypes = [],
    tables = [],
    hooks = [] :: [{atom(), [atom()]}]
    }).

parse_transform(Forms, _Options) ->
    SOut = lists:foldl(fun xform/2, #state{}, Forms),
    SOut2 = gen_functions(SOut),
    lists:reverse(SOut2#state.out).

xform(R = {attribute,_L,module,N}, S = #state{out = OF}) ->
    Exports = [
        eval_form("-export([erquery/3, all/1, find/1, find/2, merge/2, match/2, create/1, update/1, update/2, delete/1, delete/2, is_table_record/1])."),
        eval_form("-export([tp_identity/1, tp_integer/1, tp_jsonb/1, tp_string/1, tp_binary/1, tp_float/1, tp_boolean/1, tp_date/1, tp_time/1, tp_datetime/1, tp_big_integer/1]).")
    ],
    S#state{out = Exports ++ [R | OF], mod = N};
xform(R = {attribute,_L,record,{N, Fs}}, S = #state{out = OF, recs = Recs}) ->
    case N of
        column -> S#state{out = [R | OF]};
        statement -> S#state{out = [R | OF]};
        error -> S#state{out = [R | OF]};
        _ ->
            S#state{recs = [{N,Fs} | Recs], out = [R | OF]}
    end;
xform(R = {attribute,_L,type,{{record,N}, Fs, []}}, S = #state{out = OF, rectypes = RecTypes}) ->
    S#state{out = [R | OF], rectypes = [{N, Fs} | RecTypes]};
xform({attribute,_L,table,{N, RecN, Key}}, S = #state{tables = Ts}) ->
    S#state{tables = [{N,RecN,Key} | Ts]};
xform({attribute,_L,hook,{N, FunN}}, S = #state{hooks = Hs}) ->
    NewHooks = case lists:keyfind(N, 1, Hs) of
        {N, L} -> [FunN | L];
        false -> [FunN]
    end,
    S#state{hooks = lists:keystore(N, 1, Hs, {N, NewHooks})};
xform(F, S = #state{out = OF}) ->
    S#state{out = [F | OF]}.

gen_functions(S = #state{out = OF, recs = Recs, rectypes = RecTypes, tables = Tables, mod = M, hooks = Hs}) ->
    Funcs = lists:flatten([
        tremble_gen:gen_type_parsers(),
        tremble_gen:gen_run_hooks(),
        tremble_gen:gen_map_recs(Recs, RecTypes),
        tremble_gen:gen_make_params(Recs, RecTypes),
        tremble_gen:gen_erquery(M),
        tremble_gen:gen_match(),
        tremble_gen:gen_merge(),
        tremble_gen:gen_all(Tables),
        tremble_gen:gen_is_table_record(Tables),
        tremble_gen:gen_find1(Tables, Recs),
        tremble_gen:gen_find2(Tables),
        tremble_gen:gen_create(Tables, Recs, proplists:get_value(post_create, Hs, [])),
        tremble_gen:gen_update1(Tables),
        tremble_gen:gen_update2(Tables, Recs, proplists:get_value(post_update, Hs, [])),
        tremble_gen:gen_delete1(Tables),
        tremble_gen:gen_delete2(Tables, proplists:get_value(post_delete, Hs, []))
    ]),
    S#state{out = Funcs ++ OF}.

eval_form(Str) ->
    {ok, Tokens, _} = erl_scan:string(Str),
    {ok, AbsForm} = erl_parse:parse_form(Tokens),
    AbsForm.


%%
%% tremble
%% tuple-relational mapper
%%
%% Copyright (c) 2015, Alex Wilson
%% Author: Alex Wilson <alex@cooperi.net>
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
        eval_form("-export([erquery/3, all/1, find/1, find/2, merge/2, match/2, create/1, update/1, update/2, delete/1, delete/2])."),
        eval_form("-export([tp_identity/1, tp_integer/1, tp_jsonb/1, tp_string/1, tp_binary/1, tp_float/1, tp_boolean/1, tp_date/1, tp_time/1, tp_datetime/1]).")
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

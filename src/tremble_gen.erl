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

-module(tremble_gen).
-compile({parse_transform, parse_trans_codegen}).

-export([
    gen_type_parsers/0,
    gen_erquery/1,
    gen_all/1, gen_is_table_record/1,
    gen_find1/2, gen_find2/1,
    gen_create/3,
    gen_update1/1, gen_update2/3,
    gen_delete1/1, gen_delete2/2,
    gen_merge/0, gen_match/0,
    gen_make_params/2, gen_map_recs/2,
    gen_run_hooks/0
    ]).

gen_type_parsers() ->
    [
        codegen:gen_function(tp_identity, fun(V) -> V end),
        codegen:gen_function(tp_jsonb, fun(null) -> []; (B) when is_binary(B) -> jsx:decode(B) end),
        codegen:gen_function(tp_integer, fun(null) -> null; (V) when is_integer(V) -> V end),
        codegen:gen_function(tp_big_integer, fun(null) -> null; (V) when is_integer(V) -> V end),
        codegen:gen_function(tp_string, fun(null) -> null; (V = [I | _]) when is_list(V) and is_integer(I) -> V; (V) when is_binary(V) -> binary_to_list(V) end),
        codegen:gen_function(tp_binary, fun(null) -> null; (V) when is_binary(V) -> V end),
        codegen:gen_function(tp_float, fun(null) -> null; (V) when is_float(V) -> V; (V) when is_integer(V) -> float(V) end),
        codegen:gen_function(tp_boolean, fun(null) -> null; (true) -> true; (false) -> false end),
        codegen:gen_function(tp_datetime, fun(null) -> null; ({D, T}) -> {tp_date(D), tp_time(T)} end),
        codegen:gen_function(tp_date, fun(null) -> null; ({Y,M,D}) when is_integer(Y) and is_integer(M) and is_integer(D) -> {Y,M,D} end),
        codegen:gen_function(tp_time, fun(null) -> null; ({H,M,S}) when is_integer(H) and is_integer(M) and is_float(S) -> {H,M,S}; ({H,M,S}) when is_integer(H) and is_integer(M) and is_integer(S) -> {H,M,float(S)} end)
    ].

gen_run_hooks() ->
    codegen:gen_function(run_hooks,
        fun(Fun) ->
            case get(tr_post_commit_hooks) of
                L when is_list(L) ->
                    put(post_commit_hooks, [Fun | L]);
                _ ->
                    Fun()
            end
        end).

gen_erquery(_M) ->
    codegen:gen_function(erquery,
        fun(Rec, Stmt, Params) ->
            case tremble:equery(Stmt, Params) of
                {ok, Cols, Rows} ->
                    {ok, map_recs(Cols, Rows, Rec)};
                Err -> Err
            end
        end).

gen_is_table_record(Tables) ->
    DefaultClause = {clause,1,[{var,1,'_'}],[],[{atom,1,'false'}]},
    TableClauses = [{clause,1,[{atom,1,Rec}],[],[{atom,1,'true'}]} || {_Table, Rec, _Key} <- Tables],
    Clauses = lists:reverse([DefaultClause | TableClauses]),
    {function,1,is_table_record,1,Clauses}.

gen_all(Tables) ->
    codegen:gen_function(all, [
        fun(R = {'$var',Rec}) ->
            erquery(R, "select * from " ++ {'$var',Table}, [])
        end
        || {Table, Rec, _Key} <- Tables]).

gen_find1(Tables, Recs) ->
    TableCols = lists:map(fun({Table, Rec, _Key}) ->
        {Rec, Fields} = lists:keyfind(Rec, 1, Recs),
        FieldNames = [Field || {record_field,_,{atom,_,Field}} <- Fields],
        {Table, Rec, FieldNames}
    end, Tables),
    codegen:gen_function(find, [
        fun(R) when is_tuple(R) and (element(1,R) =:= {'$var',Rec}) ->
            find(R, {'$var', FieldNames})
        end
        || {_Table, Rec, FieldNames} <- TableCols]).

gen_find2(Tables) ->
    codegen:gen_function(find, [
        fun(R, WhereAttrs) when is_tuple(R) and (element(1,R) =:= {'$var',Rec}) ->
            {Parts, Params, _N} = make_params(R, WhereAttrs, 1,
                fun(F, T, N) ->
                    case T of
                        null -> [F, " is null"];
                        jsonb -> [F, " @> ", N];
                        _ -> [F, " = ", N]
                    end
                end),
            Q = lists:flatten(["select * from ", {'$var', Table}, " where ",
                string:join(Parts, " and ")]),
            erquery({'$var',Rec}, Q, Params)
        end
        || {Table, Rec, _Key} <- Tables]).

gen_create(Tables, Recs, PostHooks) ->
    TableCols = lists:map(fun({Table, Rec, Key}) ->
        {Rec, Fields} = lists:keyfind(Rec, 1, Recs),
        FieldNames = [{Field, I} || {I, {record_field,_,{atom,_,Field}}} <- lists:zip(lists:seq(2,length(Fields)+1), Fields)],
        KeyFields = string:join([atom_to_list(A) || A <- Key], ", "),
        {Table, Rec, KeyFields, FieldNames}
    end, Tables),
    Hook = {block, 1, [{var,1,'T'} | [
            {call,1,{atom,1,Func},[{var,1,'R'},{var,1,'T'}]}
        || Func <- PostHooks]]},
    codegen:gen_function(create, [
        fun(R) when is_tuple(R) and (element(1,R) =:= {'$var',Rec}) ->
            Fields = [FN || {FN,I} <- {'$var',FieldNames}, element(I,R) =/= undefined],
            {ComboParts, Params, _N} = make_params(R, Fields, 1, fun(F,_T,N) -> {F,N} end),
            ColParts = [C || {C,_} <- ComboParts],
            ValParts = [V || {_,V} <- ComboParts],
            Q = lists:flatten(["insert into ", {'$var',Table},
                " (", string:join(ColParts, ", "), ")",
                " values (", string:join(ValParts, ", "), ")",
                " returning ", {'$var',KeyFields}]),
            case tremble:equery(Q, Params) of
                {ok, _, _, [T]} when is_tuple(T) ->
                    run_hooks(fun() -> {'$form', Hook} end),
                    case T of
                        {Id} -> {ok, Id};
                        _ -> {ok, T}
                    end;
                {ok, _, [T]} when is_tuple(T) ->
                    run_hooks(fun() -> {'$form', Hook} end),
                    case T of
                        {Id} -> {ok, Id};
                        _ -> {ok, T}
                    end;
                ok ->
                    T = {},
                    run_hooks(fun() -> {'$form', Hook} end),
                    ok;
                T when is_tuple(T) and (element(1,T) =:= ok) ->
                    run_hooks(fun() -> {'$form', Hook} end),
                    ok;
                Err -> Err
            end
        end
        || {Table, Rec, KeyFields, FieldNames} <- TableCols]).

gen_update1(Tables) ->
    codegen:gen_function(update, [
        fun(R) when is_tuple(R) and (element(1,R) =:= {'$var',Rec}) ->
            update(R, {'$var', Key})
        end
        || {_Table, Rec, Key} <- Tables]).

gen_update2(Tables, Recs, PostHooks) ->
    TableCols = lists:map(fun({Table, Rec, _Key}) ->
        {Rec, Fields} = lists:keyfind(Rec, 1, Recs),
        FieldNames = [{Field, I} || {I, {record_field,_,{atom,_,Field}}} <- lists:zip(lists:seq(2,length(Fields)+1), Fields)],
        {Table, Rec, FieldNames}
    end, Tables),
    Hook = {block, 1, [{var,1,'T'} | [
            {call,1,{atom,1,Func},[{var,1,'R'},{var,1,'T'},{var,1,'SetFields'}]}
        || Func <- PostHooks]]},
    codegen:gen_function(update, [
        fun(R, WhereAttrs) when is_tuple(R) and (element(1,R) =:= {'$var',Rec}) ->
            SetFields = [FN || {FN,I} <- {'$var',FieldNames}, element(I,R) =/= undefined] -- WhereAttrs,
            {SetParts, SetParams, N1} = make_params(R, SetFields, 1,
                fun(F, _T, N) -> [F, " = ", N] end),
            {WhereParts, WhereParams, _N2} = make_params(R, WhereAttrs, N1,
                fun(F, T, N) -> case T of
                    null -> [F, " is null"];
                    jsonb -> [F, " @> ", N];
                    _ -> [F, " = ", N]
                end end),
            Q = lists:flatten(["update ", {'$var',Table}, " set ",
                string:join(SetParts, ", "), " where ",
                string:join(WhereParts, " and ")]),
            case tremble:equery(Q, SetParams ++ WhereParams) of
                T when is_tuple(T) and (element(1,T) =:= ok) ->
                    run_hooks(fun() -> {'$form', Hook} end), T;
                Err -> Err
            end
        end
        || {Table, Rec, FieldNames} <- TableCols]).

gen_delete1(Tables) ->
    codegen:gen_function(delete, [
        fun(R) when is_tuple(R) and (element(1,R) =:= {'$var',Rec}) ->
            delete(R, {'$var',Key})
        end
        || {_Table, Rec, Key} <- Tables]).

gen_delete2(Tables, PostHooks) ->
    Hook = {block, 1, [{var,1,'T'} | [
            {call,1,{atom,1,Func},[{var,1,'R'},{var,1,'T'}]}
        || Func <- PostHooks]]},
    codegen:gen_function(delete, [
        fun(R, WhereAttrs) when is_tuple(R) and (element(1,R) =:= {'$var',Rec}) ->
            {WhereParts, WhereParams, _N} = make_params(R, WhereAttrs, 1,
                fun(F, T, N) -> case T of
                    null -> [F, " is null"];
                    jsonb -> [F, " @> ", N];
                    _ -> [F, " = ", N]
                end end),
            Q = lists:flatten(["delete from ", {'$var',Table},
                " where ", string:join(WhereParts, " and ")]),
            case tremble:equery(Q, WhereParams) of
                T when is_tuple(T) and (element(1,T) =:= ok) ->
                    run_hooks(fun() -> {'$form', Hook} end), T;
                Err -> Err
            end
        end
        || {Table, Rec, _Key} <- Tables]).

gen_merge() ->
    codegen:gen_function(merge, fun
        (Rec1, Rec2) when is_tuple(Rec1) and is_tuple(Rec2) and is_atom(element(1, Rec1)) and
                (element(1,Rec1) =:= element(1,Rec2)) ->
            Size = tuple_size(Rec1),
            lists:foldl(fun(Idx, Acc) ->
                case {element(Idx, Rec1), element(Idx, Rec2)} of
                    {undefined, _V} -> Acc;
                    {V, undefined} -> setelement(Idx, Acc, V);
                    {_V1, _V2} -> Acc
                end
            end, Rec2, lists:seq(2, Size));
        (_, _) -> error(bad_record)
    end).

gen_match() ->
    codegen:gen_function(match, fun
        (Rec1, Rec2) when is_tuple(Rec1) and is_tuple(Rec2) and is_atom(element(1, Rec1)) and
                (element(1,Rec1) =:= element(1,Rec2)) ->
            S1 = tuple_size(Rec1),
            lists:all(fun(Idx) ->
                case {element(Idx, Rec1), element(Idx, Rec2)} of
                    {A, A} -> true;
                    {undefined, _} -> true;
                    {_, undefined} -> true;
                    _ -> false
                end
            end, lists:seq(2, S1));
        (_, _) -> false
    end).

gen_make_params(Recs, RecTypes) ->
    RecFields = lists:flatmap(fun({RecN, Fields}) ->
        FieldTypes = [{Field, Type} ||
            {typed_record_field,{record_field,_,{atom,_,Field}},Type} <-
                proplists:get_value(RecN, RecTypes, [])],
        FieldsN = lists:zip(Fields, lists:seq(2,length(Fields)+1)),
        lists:map(fun({{record_field,_,{atom,_,Field}}, N}) ->
            TypeForm = proplists:get_value(Field, FieldTypes),
            TypeAtom = case TypeForm of
                {type,_,union,[{atom,_,undefined},{type,_,list,[{type,_,_Type,[]}]}]} ->
                    array;
                {type,_,union,[{atom,_,undefined},{type,_,Type,[]}]} ->
                    Type;
                {type,_,list,[{type,_,_Type,[]}]} ->
                    array;
                {type,_,Type,[]} ->
                    Type;
                _ ->
                    other
            end,
            [Parser] = case TypeAtom of
                array -> codegen:exprs(fun(V0) -> {array, V0} end);
                integer ->
                    codegen:exprs(fun(V0) -> case V0 of
                        null -> {null, null};
                        I when is_integer(I) -> {int, I} end end);
                big_integer ->
                    codegen:exprs(fun(V0) -> case V0 of
                        null -> {null, null};
                        I when is_integer(I) -> {bigint, I} end end);
                float ->
                    codegen:exprs(fun(V0) -> case V0 of
                        null -> {null, null};
                        I when is_integer(I); is_float(I) -> {numeric, I} end end);
                string ->
                    codegen:exprs(fun(V0) -> case V0 of
                        null -> {null, null};
                        S when is_list(S); is_binary(S) -> {varchar, S} end end);
                binary ->
                    codegen:exprs(fun(V0) -> case V0 of
                        null -> {null, null};
                        B when is_binary(B) -> {bytea, B} end end);
                boolean ->
                    codegen:exprs(fun(V0) -> case V0 of
                        null -> {null, null};
                        true -> {bool, true};
                        false -> {bool, false} end end);
                date ->
                    codegen:exprs(fun(V0) -> case V0 of
                        null -> {null, null};
                        {Y,M,D} when is_integer(Y) and is_integer(M) and is_integer(D) -> {date, {Y,M,D}} end end);
                time ->
                    codegen:exprs(fun(V0) -> case V0 of
                        null -> {null, null};
                        {H,M,S} when is_integer(H) and is_integer(M) -> {time, {H,M,float(S)}} end end);
                datetime ->
                    codegen:exprs(fun(V0) -> case V0 of
                        null -> {null, null};
                        {{Y,M,D},{H,Min,S}} when is_integer(Y) and is_integer(M) and is_integer(D) and is_integer(H) and is_integer(Min) -> {timestamp, {{Y,M,D},{H,Min,float(S)}}};
                        Now = {MS, S, US} when is_integer(MS) and is_integer(S) and is_integer(US) -> {timestamp, calendar:now_to_datetime(Now)} end end);
                jsonb ->
                    codegen:exprs(fun(V0) -> case V0 of
                        null -> {null, null};
                        Pl when is_list(Pl) -> {jsonb, jsx:encode(Pl)} end end)
            end,
            {RecN, Field, atom_to_list(Field), N, Parser}
        end, FieldsN)
    end, Recs),
    codegen:gen_function(make_params, [
        fun(R, [{'$var', Field} | Rest], N, Fun) when
                is_tuple(R) and (element(1,R) =:= {'$var',RecN}) ->
            case element({'$var',M}, R) of
                undefined ->
                    case Rest of
                        [] -> {[], [], N};
                        _ -> make_params(R, Rest, N, Fun)
                    end;
                V0 ->
                    {T, V} = {'$form',Parser},
                    NPart = case T of
                        null -> "null";
                        other -> [$$ | integer_to_list(N)];
                        array -> [$$ | integer_to_list(N)];
                        _ -> [$$ | integer_to_list(N)] ++ [$:,$: | atom_to_list(T)]
                    end,
                    Part = Fun({'$var',FieldStr}, T, NPart),
                    case T of
                        null ->
                            {RestParts, RestParams, FinalN} = case Rest of
                                [] -> {[], [], N};
                                _ -> make_params(R, Rest, N, Fun)
                            end,
                            {[Part | RestParts], RestParams, FinalN};
                        _ ->
                            {RestParts, RestParams, FinalN} = case Rest of
                                [] -> {[], [], N + 1};
                                _ -> make_params(R, Rest, N + 1, Fun)
                            end,
                            {[Part | RestParts], [V | RestParams], FinalN}
                    end
            end
        end
        || {RecN, Field, FieldStr, M, Parser} <- RecFields]).

gen_map_recs(Recs, RecTypes) ->
    RecsWithCMs = lists:map(fun({RecN, Fields}) ->
        FieldTypes = [{Field, Type} ||
            {typed_record_field,{record_field,_,{atom,_,Field}},Type} <-
                proplists:get_value(RecN, RecTypes, [])],
        FieldClauses = lists:map(fun({N, {record_field,_,{atom,_,Field}}}) ->
            TypeForm = proplists:get_value(Field, FieldTypes),
            ParserName = case TypeForm of
                {type,_,union,[{atom,_,undefined},{type,_,list,[{type,_,_Type,[]}]}]} ->
                    %% TODO: better array validation
                    tp_identity;
                {type,_,union,[{atom,_,undefined},{type,_,Type,[]}]} ->
                    list_to_existing_atom("tp_" ++ atom_to_list(Type));
                {type,_,list,[{type,_,_Type,[]}]} ->
                    %% TODO: better array validation
                    tp_identity;
                {type,_,Type,[]} ->
                    list_to_existing_atom("tp_" ++ atom_to_list(Type));
                _ ->
                    tp_identity
            end,
            Parser = {'fun',1,{function, ParserName, 1}},
            {clause,1, [{atom,1,Field}], [],
                [{cons,1,{tuple,1,[{integer,1,N}, {var,1,'M'}, Parser]},{var,1,'Acc'}}]
                }
        end, lists:zip(lists:seq(2, length(Fields)+1), Fields)),
        DefaultClause = {clause, 1, [{var,1,'_'}], [], [{var,1,'Acc'}]},
        ColMapper = {'case',1,{var,1,'A'},FieldClauses ++ [DefaultClause]},
        Blank = {record,1,RecN,[]},
        {RecN, Fields, Blank, ColMapper}
    end, Recs),
    codegen:gen_function(map_recs, [
        fun(Cols, Rows, {'$var', RecN}) ->
            Blank = {'$form', BlankForm},
            ColOffsets = lists:zip(Cols, lists:seq(1, length(Cols))),
            ColMappings = lists:foldl(fun({Col,M}, Acc) ->
                A = binary_to_existing_atom(Col#column.name, utf8),
                {'$form', ColMapper}
            end, [], ColOffsets),
            lists:map(fun(Row) ->
                lists:foldl(fun({N, M, Parser}, Acc) ->
                    setelement(N, Acc, Parser(element(M, Row)))
                end, Blank, ColMappings)
            end, Rows)
        end
        || {RecN, _Fields, BlankForm, ColMapper} <- RecsWithCMs]).

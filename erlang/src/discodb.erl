-module(discodb).

%% DiscoDBCons

-export([cons/0,
         cons/1,
         add/2,
         add/3,
         finalize/1,
         finalize/2]).

%% DiscoDB

-export([new/1,
         new/2,
         load/1,
         loads/1,
         dump/2,
         dumps/1,
         get/2,
         keys/1,
         values/1,
         unique_values/1,
         q/2,
         query/2]).

%% DiscoDBIter

-export([fold/3,
         next/1,
         size/1,
         count/1,
         to_list/1]).

%% Convenience
-export([each/3,
         list/1,
         list/3,
         peek/1,
         peek/2,
         peek/3,
         peek/4]).

str(Bin) when is_binary(Bin) ->
    binary_to_list(Bin);
str(Str) when is_list(Str) ->
    Str.

wait(DDB) ->
    receive
        {discodb, ok} ->
            DDB;
        {discodb, Reply} ->
            Reply;
        Else ->
            self() ! Else,
            wait(DDB)
    end.

init(Type, Func, Args) ->
    case discodb_nif:init(Type, Func, Args) of
        DDB when is_binary(DDB) ->
            wait(DDB);
        Error ->
            Error
    end.

call(DDB, Method, Args) ->
    case discodb_nif:call(DDB, Method, Args) of
        DDB when is_binary(DDB) ->
            wait(DDB);
        Error ->
            Error
    end.

%% DiscoDBCons

cons() ->
    init(discodb_cons, new, []).

cons(DB) ->
    init(discodb_cons, ddb, DB).

add(Cons, [Key, Val]) ->
    add(Cons, {Key, Val});
add(Cons, Item) ->
    call(Cons, add, Item).

add(Cons, Key, Val) ->
    add(Cons, {Key, Val}).

finalize(Cons) ->
    finalize(Cons, []).

finalize(Cons, Flags) ->
    call(Cons, finalize, Flags).

%% DiscoDB

new(Items) ->
    new(Items, []).

new(Items, Flags) ->
    finalize(lists:foldl(fun (Item, Cons) ->
                                 add(Cons, Item)
                         end, cons(), Items), Flags).

load(Filename) ->
    init(discodb, load, str(Filename)).

loads(Data) ->
    init(discodb, loads, str(Data)).

dump(DB, Filename) ->
    call(DB, dump, str(Filename)).

dumps(DB) ->
    call(DB, dumps, []).

get(DB, Key) ->
    call(DB, get, Key).

keys(DB) ->
    call(DB, iter, keys).

values(DB) ->
    call(DB, iter, values).

unique_values(DB) ->
    call(DB, iter, unique_values).

q(DB, Q) ->
    query(DB, Q).

query(DB, Q) ->
    call(DB, query, Q).

%% DiscoDBIter

fold(Iter, Fun, Acc) ->
    case discodb:next(Iter) of
        null ->
            Acc;
        <<Entry/binary>> ->
            fold(Iter, Fun, Fun(Entry, Acc))
    end.

next(Iter) ->
    discodb_nif:next(Iter).

size(Iter) ->
    discodb_nif:size(Iter).

count(Iter) ->
    discodb_nif:count(Iter).

to_list(Iter) ->
    lists:reverse(fold(Iter, fun (E, Acc) -> [E|Acc] end, [])).

%% Convenience

each(DB, Fun, Acc) ->
    fold(keys(DB),
         fun (K, A) ->
                 fold(get(DB, K),
                      fun (V, B) ->
                              Fun({K, V}, B)
                      end, A)
         end, Acc).

list(Iter) ->
    to_list(Iter).

list(DB, Fun, Arg) ->
    to_list(call(DB, Fun, Arg)).

peek(Iter) ->
    peek(Iter, null).

peek(Iter, Default) ->
    case next(Iter) of
        null ->
            Default;
        <<Entry/binary>> ->
            Entry
    end.

peek(DB, Fun, Arg) ->
    peek(DB, Fun, Arg, null).

peek(DB, Fun, Arg, Default) ->
    peek(call(DB, Fun, Arg), Default).

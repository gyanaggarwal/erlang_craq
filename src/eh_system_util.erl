%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Copyright (c) 2016 Gyanendra Aggarwal.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-module(eh_system_util).

-export([get_node_name/1,
         get_node_atom/1,
         get_file_name/3,
         make_list_to_string/2,
         new_set/0,
         size_set/1,
         add_set/2,
         remove_set/2,
         merge_set/2,
         subtract_set/2,
         fold_set/3,
         is_key_set/2,
         is_empty_set/1,
         to_list_set/1,
         filter_set/2,
         new_map/0,
         size_map/1,
         add_map/3,
         remove_map/2,
         find_map/2,
         fold_map/3,
         is_key_map/2,
         is_empty_map/1,
         to_list_map/1,
         partition_map/2,
         filter_map/2,
         display_atom_to_list/1]).

-spec get_node_name(Node :: atom()) -> string().
get_node_name(Node) ->
  lists:takewhile(fun(X) -> X =/= $@ end, atom_to_list(Node)).

-spec get_node_atom(Node :: atom()) -> atom().
get_node_atom(Node) ->
  list_to_atom(get_node_name(Node)).

-spec get_file_name(NodeName :: string(), DataDir :: string(), FileName :: string() | atom()) -> string() | atom().
get_file_name(_, _, standard_io) ->
  standard_io;
get_file_name(NodeName, DataDir, FileName) ->
  DataDir ++ NodeName ++ FileName.

make_list_to_string(Fun, List) ->
  lists:foldl(fun(N, Acc) -> case length(Acc) of
                               0 -> Acc ++ Fun(N);
                               _ -> Acc ++ "," ++ Fun(N)
                             end end, [], List).

new_set() ->
  sets:new().

size_set(Set) ->
  sets:size(Set).

add_set(Key, Set) ->
  sets:add_element(Key, Set).

remove_set(Key, Set) ->
  sets:del_element(Key, Set).

merge_set(Set1, Set2) ->
  sets:union(Set1, Set2).

subtract_set(Set1, Set2) ->
  sets:subtract(Set1, Set2).

is_key_set(Key, Set) ->
  sets:is_element(Key, Set).

is_empty_set(undefined) ->
  true;
is_empty_set(Set) ->
  sets:size(Set) =:= 0.

fold_set(Fun, Acc, Set) ->
  sets:fold(Fun, Acc, Set).

to_list_set(Set) ->
  sets:to_list(Set).

filter_set(Fun, Set) ->
  sets:filter(Fun, Set).

new_map() ->
  maps:new().

size_map(Map) ->
  maps:size(Map).

add_map(Key, Value, Map) ->
  maps:put(Key, Value, Map).

remove_map(Key, Map) ->
  maps:remove(Key, Map).

find_map(Key, Map) ->
  maps:find(Key, Map).

is_key_map(Key, Map) ->
  maps:is_key(Key, Map).

is_empty_map(undefined) ->
  true;
is_empty_map(Map) ->
  maps:size(Map) =:= 0.

fold_map(Fun, Acc, Map) ->
  maps:fold(Fun, Acc, Map).

to_list_map(Map) ->
  maps:to_list(Map).

partition_map(Fun, Map) ->
  maps:fold(fun(K, V, {TMapX, FMapX}) -> case Fun(K, V) of
                                           true  ->
                                             {add_map(K, V, TMapX), FMapX};
                                           false ->
                                             {TMapX, add_map(K, V, FMapX)}
                                         end end, {new_map(), new_map()}, Map).

filter_map(Fun, Map) ->
  maps:filter(Fun, Map).

display_atom_to_list(Atom) ->
  ListAtom = atom_to_list(Atom),
  lists:sublist(ListAtom, 4, length(ListAtom)). 











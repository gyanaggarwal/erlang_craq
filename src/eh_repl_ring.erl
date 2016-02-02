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

-module(eh_repl_ring).

-export([drop/4,
         add/4,
         predecessor/4,
         successor/4,
         get_ordered_list/3,
         get_ordered_list_pred_succ/4,
         effective_head_node_id/4,
         effective_tail_node_id/4]).

-include("erlang_craq.hrl").

make_ordered_list(NodeList, NodeOrderList) ->
  lists:filter(fun(X) -> lists:member(X, NodeList) end, NodeOrderList).

get_ordered_list(NodeList, NodeOrderList, ?EH_SORTED) ->
  get_ordered_list(NodeList, lists:sort(NodeOrderList), ?EH_USER_DEFINED);
get_ordered_list(NodeList, NodeOrderList, ?EH_USER_DEFINED) ->
  {make_ordered_list(NodeList, NodeOrderList), NodeOrderList}.

drop(Node, NodeList, NodeOrderList, NodeOrder) ->
  get_ordered_list(lists:delete(Node, NodeList), NodeOrderList, NodeOrder).

add(Node, NodeList, NodeOrderList, NodeOrder) ->
  get_ordered_list([Node | lists:delete(Node, NodeList)], NodeOrderList, NodeOrder).

predecessor(Node, NodeList, NodeOrderList, NodeOrder) ->
  {NewNodeList, _} = add(Node, NodeList, NodeOrderList, NodeOrder),
  {L1, L2} = lists:splitwith(fun(N) -> N =/= Node end, NewNodeList),
  case {length(L1), length(L2)} of
    {0, 0} -> 
      undefined;
    {0, 1} ->
      undefined;
    {0, _} ->
      lists:last(L2);
    {_, _} ->
      lists:last(L1)
  end.

successor(Node, NodeList, NodeOrderList, NodeOrder) ->
  {NewNodeList, _} = add(Node, NodeList, NodeOrderList, NodeOrder),
  {L1, L2} = lists:splitwith(fun(N) -> N =/= Node end, NewNodeList),
  case {length(L1), length(L2)} of
    {0, 0} ->
      undefined;
    {0, 1} ->
      undefined;
    {_, 1} ->
      hd(L1);
    {_, _} ->
      [Node, Succ | _] = L2,
      Succ
  end.

get_ordered_list_pred_succ(Node, NodeList, NodeOrderList, NodeOrder) ->
  {NodeList1, NodeOrderList1} = get_ordered_list(NodeList, NodeOrderList, NodeOrder),
  Pred = predecessor(Node, NodeList, NodeOrderList, NodeOrder),
  Succ = successor(Node, NodeList, NodeOrderList, NodeOrder),
  {NodeList1, NodeOrderList1, Pred, Succ}.

effective_head_node_id(MsgNodeId, NodeList, NodeOrderList, NodeOrder) ->
  case lists:member(MsgNodeId, NodeList) of
    true  ->
      MsgNodeId;
    false ->
      successor(MsgNodeId, NodeList, NodeOrderList, NodeOrder)
  end.

effective_tail_node_id(MsgNodeId, NodeList, NodeOrderList, NodeOrder) ->
  predecessor(MsgNodeId, NodeList, NodeOrderList, NodeOrder).



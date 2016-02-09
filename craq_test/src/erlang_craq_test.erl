%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Copyright (c) 2015 Gyanendra Aggarwal.  All Rights Reserved.
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

-module(erlang_craq_test).

-export([data_entries/1,
         node_change/1]).

-include("erlang_craq_test.hrl").

update(0, _State) ->
  ok;
update(DataEntries, #eh_run_state{active_nodes=ActiveNodes}=State) ->
  timer:sleep(?ENTRY_SLEEP_TIME),
  {Node, ObjectType, ObjectId, Columns} = eh_test_util:get_update_param(ActiveNodes),
  erlang_craq:update(Node, ObjectType, ObjectId, Columns),
  update(DataEntries-1, State).

make_node_change(?NODE_DOWN, Node, #eh_run_state{active_nodes=ActiveNodes, down_nodes=DownNodes}=State) ->
  erlang_craq:stop(Node),
  State#eh_run_state{active_nodes=lists:delete(Node, ActiveNodes), down_nodes=[Node | DownNodes]};
make_node_change(?NODE_UP, Node, #eh_run_state{initial_nodes=InitialNodes, active_nodes=ActiveNodes, down_nodes=DownNodes}=State) ->
  ActiveNodes1 = [Node | ActiveNodes],
  erlang_craq:add_node(Node, ActiveNodes1, InitialNodes),
  State#eh_run_state{active_nodes=ActiveNodes1, down_nodes=lists:delete(Node, DownNodes)};
make_node_change(_, _, State) ->
  State.

data_entries(NodeList) ->
  random:seed(erlang:phash2([node()]), erlang:monotonic_time(), erlang:unique_integer()),
  State = #eh_run_state{initial_nodes=NodeList, active_nodes=NodeList},
  DataEntries = eh_test_util:get_random(?BULK_DATA_ENTRIES),
  update(DataEntries, State).

node_change(NodeList) ->
  random:seed(erlang:phash2([node()]), erlang:monotonic_time(), erlang:unique_integer()),
  State = #eh_run_state{initial_nodes=NodeList, active_nodes=NodeList, test_runs=eh_test_util:get_random(?TEST_RUNS)},
  do_node_change(State).

do_node_change(#eh_run_state{test_runs=0, down_nodes=[]}) ->
  ok;
do_node_change(#eh_run_state{test_runs=TestRuns}=State) ->
  timer:sleep(?NODE_SLEEP_TIME),
  case TestRuns =:= 0 of
    true  ->
      DataEntries = eh_test_util:get_random(?DATA_ENTRIES),
      update(DataEntries, State);
    false ->
      ok
  end,
  {NodeChange, Node} = eh_test_util:get_node_change(State),
  print(NodeChange, Node, State),
  State1 = State#eh_run_state{test_runs=max(0, TestRuns-1)},
  State2 = make_node_change(NodeChange, Node, State1),
  do_node_change(State2).

print(NodeChange, Node, #eh_run_state{test_runs=TestRuns, active_nodes=ActiveNodes, down_nodes=DownNodes}) ->
  io:fwrite("node_change=~p, node=~p, test_runs=~p, active_nodes=~p, down_nodes=~p~n", 
            [NodeChange, eh_system_util:get_node_name(Node), TestRuns, length(ActiveNodes), length(DownNodes)]).
 

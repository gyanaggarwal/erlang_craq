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

-module(eh_test_util).

-export([get_random/1,
         get_random_nth/1,
         get_update_param/1,
         get_node_change/1]).

-include("erlang_craq_test.hrl").

get_random({Min, Min}) ->
  Min;
get_random({Min, Max}) ->
  random:uniform(Max-Min)+Min.

get_random_nth(List) ->
  N = get_random({0, length(List)}),
  lists:nth(N, List).

get_update_param(ActiveNodes) ->
  {get_random_nth(ActiveNodes), get_random_nth(?OBJECT_TYPE), get_random_nth(?OBJECT_ID), [{get_random_nth(?COLUMNS), get_random_nth(?VALUES)}]}.

get_node_change(#eh_run_state{test_runs=0, down_nodes=[]}=State) ->
  get_node_change(?NODE_NOCHANGE, State);
get_node_change(#eh_run_state{test_runs=0}=State) ->
  get_node_change(?NODE_UP, State);
get_node_change(#eh_run_state{initial_nodes=InitialNodes, active_nodes=ActiveNodes}=State) when length(ActiveNodes) =:= length(InitialNodes) ->
  get_node_change(?NODE_DOWN, State);
get_node_change(#eh_run_state{active_nodes=ActiveNodes}=State) when length(ActiveNodes) =:= 1 ->
  get_node_change(?NODE_UP, State);
get_node_change(State) ->
  get_node_change(get_random_nth(?NODE_CHANGE), State).

get_node_change(?NODE_NOCHANGE, _) ->
  {?NODE_NOCHANGE, undefined};
get_node_change(?NODE_DOWN, #eh_run_state{active_nodes=ActiveNodes}) ->
  {?NODE_DOWN, get_random_nth(ActiveNodes)};
get_node_change(?NODE_UP, #eh_run_state{down_nodes=DownNodes}) ->
  {?NODE_UP, get_random_nth(DownNodes)}.



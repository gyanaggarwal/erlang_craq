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

-module(eh_aq_query_handler_api).

-behavior(eh_query_handler).

-export([process/6]).

-include("erlang_craq.hrl").

process(ObjectType, ObjectId, NodeId, From, Ref, #eh_system_state{repl_ring_order=ReplRingOrder, repl_ring=ReplRing, app_config=AppConfig}=State) ->
  NodeOrder = eh_system_config:get_node_order(AppConfig),
  Tail = eh_repl_ring:effective_tail_node_id(NodeId, ReplRing, ReplRingOrder, NodeOrder),
  eh_query_handler:process_tail(ObjectType, ObjectId, Tail, From, Ref, State).




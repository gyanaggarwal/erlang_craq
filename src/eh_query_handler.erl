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

-module(eh_query_handler).

-export([reply/3,
         error_node_unavailable/1,
         error_node_down/1,
         error_being_updated/1,
         updated/1,
         query/6,
         process_pending/2,
         process_tail/6]).

-include("erlang_craq.hrl").

-callback process(ObjectType :: atom(), ObjectId :: term(), NodeId :: atom(), From :: pid(), Ref :: term(), State :: #eh_system_state{}) -> #eh_system_state{}.

reply(From, Ref, Reply) ->
  From ! {reply, Ref, Reply}.

error_node_down(NodeId) ->
  error_node(NodeId, ?EH_NODEDOWN).

error_node_unavailable(NodeId) ->
  error_node(NodeId, ?EH_NODE_UNAVAILABLE).

error_being_updated(DataList) -> 
  object(error, DataList, ?EH_BEING_UPDATED).

updated(DataList) ->
  object(ok, DataList, ?EH_UPDATED).
 
object_tuple(DataList, Msg) ->
  {eh_update_msg:get_object_list(DataList), Msg}.

node_tuple(NodeId, Msg) ->
  {NodeId, Msg}.

error_node(NodeId, Msg) ->
  {error, node_tuple(NodeId, Msg)}.

object(Tag, DataList, Msg) ->
  {Tag, object_tuple(DataList, Msg)}.

process_pending(ObjectType, ObjectId, #eh_system_state{query_data=QueryData, app_config=AppConfig}=State) ->
  case eh_system_util:find_map({ObjectType, ObjectId}, QueryData) of
    error      ->
      ok;
    {ok, []}   ->
      ok;
    {ok, List} ->
      QueryReply = query_reply(ObjectType, ObjectId, AppConfig),
      lists:foreach(fun({From, Ref}) -> reply(From, Ref, {ok, QueryReply}) end, List)
  end,
  eh_node_timestamp:update_state_remove_query_data(ObjectType, ObjectId, State).

process_pending(UMsgList, State) ->
  lists:foldl(fun({ObjectType, ObjectId, _}, StateAcc) -> process_pending(ObjectType, ObjectId, StateAcc) end, State, UMsgList).

query_reply(ObjectType, ObjectId, AppConfig) ->
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  ReplDataManager:query({ObjectType, ObjectId}).

query(Tag, ObjectType, ObjectId, From, Ref, #eh_system_state{app_config=AppConfig}=State) ->
  reply(From, Ref, {Tag, query_reply(ObjectType, ObjectId, AppConfig)}),
  State.

process_tail(ObjectType, ObjectId, Tail, From, Ref, State) ->
  gen_server:cast({?EH_SYSTEM_SERVER, Tail}, {?EH_QUERY_AQ, {ObjectType, ObjectId, From, Ref}}),
  State.







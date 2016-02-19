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

-module(eh_node_timestamp).

-export([update_state_client_reply/2,
         update_state_completed_set/2,
         update_state_new_msg/3,
         update_state_timestamp/2,
         update_state_msg_data/2,
         update_state_add_query_data/5,
         update_state_remove_query_data/3,
         valid_pre_update_msg/2,
         valid_update_msg/2,
         valid_pending_pre_msg_data/2,
         valid_add_node_msg/2,
         msg_status/2]).

-include("erlang_craq.hrl").

update_state_add_query_data(ObjectType, ObjectId, From, Ref, #eh_system_state{query_data=QueryData}=State) ->
  Key = {ObjectType, ObjectId},
  Value = {From, Ref},
  List1 = case eh_system_util:find_map(Key, QueryData) of
            error      ->
              [Value];
            {ok, List} ->
              [Value | List]
          end,
  State#eh_system_state{query_data=eh_system_util:add_map(Key, List1, QueryData)}.

update_state_remove_query_data(ObjectType, ObjectId, #eh_system_state{query_data=QueryData}=State) ->
  State#eh_system_state{query_data=eh_system_util:remove_map({ObjectType, ObjectId}, QueryData)}.

update_state_client_reply(UMsgList, 
                          #eh_system_state{successor=Succ, completed_set=CompletedSet, pre_msg_data=PreMsgData}=State) ->
  {PreMsgData1, CompletedSet1} = lists:foldl(fun({UMsgKeyX, _}, {MsgDataX, CompletedSetX}) -> 
                                             {eh_system_util:remove_map(UMsgKeyX, MsgDataX), eh_system_util:add_set(UMsgKeyX, CompletedSetX)} end,
                                             {PreMsgData, CompletedSet},
                                             UMsgList),                              
 CompletedSet2 = case Succ of
                    undefined ->
                       CompletedSet;
                    _         ->
                       CompletedSet1
                 end,
  State#eh_system_state{completed_set=CompletedSet2, pre_msg_data=PreMsgData1}.

update_state_completed_set(CompletedSet, 
                           #eh_system_state{completed_set=NewCompletedSet}=State) ->
  State#eh_system_state{completed_set=eh_system_util:subtract_set(NewCompletedSet, CompletedSet)}.

update_state_msg_data(CompletedSet,
                      #eh_system_state{msg_data=MsgData}=State) ->
  MsgData1 = eh_system_util:fold_set(fun(X, Acc) -> eh_system_util:remove_map(X, Acc) end, MsgData, CompletedSet),
  State#eh_system_state{msg_data=MsgData1}.

update_state_new_msg(?EH_PRED_PRE_UPDATE, 
                     UMsgList, 
                     #eh_system_state{pre_msg_data=PreMsgData}=State) ->
  PreMsgData1 = lists:foldl(fun({UMsgKey, UMsgData}, PreMsgDataX) -> eh_system_util:add_map(UMsgKey, UMsgData, PreMsgDataX) end, PreMsgData, UMsgList),
  State#eh_system_state{pre_msg_data=PreMsgData1};
update_state_new_msg(?EH_SUCC_UPDATE, 
                     UMsgList, 
                     #eh_system_state{pre_msg_data=PreMsgData, msg_data=MsgData}=State) ->
  {PreMsgData1, MsgData1} = lists:foldl(fun({UMsgKey, UMsgData}, {PreMsgDataX, MsgDataX}) -> 
                                        {eh_system_util:remove_map(UMsgKey, PreMsgDataX), eh_system_util:add_map(UMsgKey, UMsgData, MsgDataX)} end,
                                        {PreMsgData, MsgData}, UMsgList),
  State#eh_system_state{pre_msg_data=PreMsgData1, msg_data=MsgData1}.

update_state_timestamp(MsgTimestamp, 
                       #eh_system_state{timestamp=Timestamp}=State) ->
  eh_node_state:update_state_msg(State#eh_system_state{timestamp=max(MsgTimestamp, Timestamp)}).

valid_add_node_msg(Node, 
                   #eh_system_state{repl_ring=ReplRing, app_config=AppConfig}=State) ->
  case {eh_node_state:msg_state(State), Node =:= eh_system_config:get_node_id(AppConfig), lists:member(Node, ReplRing)} of
    {?EH_NOT_READY, true, false} ->
      ?EH_VALID_FOR_NEW;
    {?EH_READY, false, false}    ->
      ?EH_VALID_FOR_EXISTING;
    {_, _, _}                    ->
      ?EH_INVALID_MSG
  end.

valid_pending_pre_msg_data(PendingPreMsgData, #eh_system_state{pre_msg_data=PreMsgData, msg_data=MsgData}) ->
  Flag = eh_system_util:fold_map(fun(UMsgKey, _, FlagX) -> FlagX andalso (not eh_system_util:is_key_map(UMsgKey, PreMsgData)) end, true, PendingPreMsgData),
  eh_system_util:fold_map(fun(UMsgKey, _, FlagX) -> FlagX andalso (not eh_system_util:is_key_map(UMsgKey, MsgData)) end, Flag, PendingPreMsgData). 

msg_status(UMsgList, 
          #eh_system_state{repl_ring_order=ReplRingOrder, repl_ring=ReplRing, app_config=AppConfig}) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  NodeOrder = eh_system_config:get_node_order(AppConfig),
  {_, _, MsgNodeId, _} = eh_update_msg:get_msg_param(UMsgList),
  TailNodeId = eh_repl_ring:effective_tail_node_id(MsgNodeId, ReplRing, ReplRingOrder, NodeOrder),
  HeadNodeId = eh_repl_ring:effective_head_node_id(MsgNodeId, ReplRing, ReplRingOrder, NodeOrder),
  case {NodeId =:= HeadNodeId, NodeId =:= TailNodeId} of
    {true, false}  ->
      ?EH_HEAD_MSG;
    {false, true}  ->
      ?EH_TAIL_MSG;
    {false, false} ->
      ?EH_RING_MSG
  end.

process_pending_pre_msg_data(UMsgList, #eh_system_state{pending_pre_msg_data=PendingPreMsgData}=State) ->
  case eh_system_util:size_map(PendingPreMsgData) > 0 of
    true  ->
      {MsgTimestamp, _, MsgNodeId, _} = eh_update_msg:get_msg_param(UMsgList),
      {_, MsgMap} = eh_update_msg:partition_on_timestamp_node_id(MsgTimestamp, MsgNodeId, PendingPreMsgData),
      State1 = case eh_system_util:size_map(MsgMap) > 0 of
                 true  ->
                   eh_persist_data:persist_data(eh_system_util:to_list_map(MsgMap), State);
                 false ->
                   State
               end,
      State1#eh_system_state{pending_pre_msg_data=eh_system_util:new_map()};
    false ->
      State
  end.

valid_msg(CheckFun,
          ConflictResolveFun, 
          UMsgList,
          MsgData,
          CheckData,
          State) ->
  State1 = process_pending_pre_msg_data(UMsgList, State),
  Flag1 = check_map(UMsgList, MsgData),
  Flag2 = Flag1 orelse CheckFun(UMsgList, CheckData),
  Flag3 = Flag2 orelse check_data(UMsgList, State1),
  case Flag3 of
    false ->
      {Flag4, State2} = ConflictResolveFun(UMsgList, State1),
      case Flag4 of
        true  ->
          {true, msg_status(UMsgList, State2), State2};
        false ->
          {false, undefined, State2}
      end;
    true  ->
      {false, undefined, State1}
  end.

valid_update_msg(UMsgList,
                 #eh_system_state{msg_data=MsgData, completed_set=CompletedSet}=State) ->
  valid_msg(fun check_set/2,
            fun update_conflict_resolver/2,
            UMsgList,
            MsgData,
            CompletedSet, 
            State).

valid_pre_update_msg(UMsgList,
                     #eh_system_state{pre_msg_data=PreMsgData, msg_data=MsgData}=State) ->
  valid_msg(fun check_map/2,
            fun pre_update_conflict_resolver/2, 
            UMsgList, 
            PreMsgData, 
            MsgData, 
            State).

check_data(UMsgList,
           #eh_system_state{timestamp=Timestamp, app_config=AppConfig}) ->
  {MsgTimestamp, _, _, _, DataList} = eh_update_msg:get_data_list(UMsgList),
  case Timestamp >= MsgTimestamp of
    true  -> 
      ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
      ReplDataManager:check_data({MsgTimestamp, DataList});
    false ->
      false
  end.

check_map(UMsgList, MsgMap) ->
  {Timestamp, _, MsgNodeId, _} = eh_update_msg:get_msg_param(UMsgList),
  {TMap, _} = eh_update_msg:partition_on_timestamp_node_id(Timestamp, MsgNodeId, MsgMap),
  eh_system_util:size_map(TMap) > 0.

check_set(UMsgList, CompletedSet) ->
  lists:any(fun({UMsgKey, _}) -> eh_system_util:is_key_set(UMsgKey, CompletedSet) end, UMsgList).

resolve_conflict(MsgListTag, MsgMapTag, UMsgList, MsgMap, #eh_system_state{app_config=AppConfig}) ->
  {MsgTimestamp, _, MsgNodeId, _} = eh_update_msg:get_msg_param(UMsgList),
  #eh_update_msg_data{node_id=EMsgNodeId, client_id=EClientId, msg_ref=EMsgRef} = eh_update_msg:exist_msg_list(UMsgList, MsgMap),
  case {EMsgNodeId, EMsgNodeId =:= MsgNodeId} of
    {undefined, _} ->
      {true, MsgMap};
    {_, true}      ->
      {true, MsgMap};
    {_, _}         ->
      ConflictResolver = eh_system_config:get_write_conflict_resolver(AppConfig),
      ResolvedNodeId = ConflictResolver:resolve({MsgListTag, MsgNodeId}, {MsgMapTag, EMsgNodeId}),
      case ResolvedNodeId =:= MsgNodeId of
        true  ->
          {TMsgMap, FMsgMap} = eh_update_msg:partition_on_timestamp_node_id(MsgTimestamp, EMsgNodeId, MsgMap),
          NodeId = eh_system_config:get_node_id(AppConfig),
          case EMsgNodeId =:= NodeId of
            true  ->
              [{_, EUMsgList}] = eh_update_msg:get_map_msg_list(TMsgMap),
              {_, _, _, _, DataList} = eh_update_msg:get_data_list(EUMsgList),
              eh_query_handler:reply(EClientId, EMsgRef, eh_query_handler:error_being_updated(DataList));
            false ->
              ok
          end,
          {true, FMsgMap};
        false ->
          {false, MsgMap}
      end
  end.

pre_update_conflict_resolver(UMsgList, #eh_system_state{pre_msg_data=PreMsgData, msg_data=MsgData}=State) ->
  {Flag1, PreMsgData1} = resolve_conflict(?EH_PRED_PRE_UPDATE, ?EH_PRED_PRE_UPDATE, UMsgList, PreMsgData, State),
  Flag3 = case Flag1 of
            true  ->
              {Flag2, _} = resolve_conflict(?EH_PRED_PRE_UPDATE, ?EH_SUCC_UPDATE, UMsgList, MsgData, State),
              Flag2;
            false ->
              false
          end,
  {Flag3, State#eh_system_state{pre_msg_data=PreMsgData1}}.

update_conflict_resolver(UMsgList, #eh_system_state{pre_msg_data=PreMsgData}=State) ->
  {Flag1, PreMsgData1} = resolve_conflict(?EH_SUCC_UPDATE, ?EH_PRED_PRE_UPDATE, UMsgList, PreMsgData, State),
  {Flag1, State#eh_system_state{pre_msg_data=PreMsgData1}}.










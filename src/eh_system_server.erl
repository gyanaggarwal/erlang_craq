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

-module(eh_system_server).

-behavior(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-include("erlang_craq.hrl").

-define(SERVER, ?EH_SYSTEM_SERVER).

start_link(AppConfig) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [AppConfig], []).


init([AppConfig]) ->
  State = #eh_system_state{app_config=AppConfig},
  {ok, State}.


handle_call(?EH_DATA_VIEW, 
            _From, 
            #eh_system_state{app_config=AppConfig}=State) ->
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  Reply = ReplDataManager:data_view(),
  {reply, Reply, State};
  
handle_call(_Msg, _From, State) ->
  {reply, ok, State}.


handle_cast({?EH_SETUP_REPL, ReplRing}, 
            #eh_system_state{app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  NodeOrder = eh_system_config:get_node_order(AppConfig),
  {ReplRing1, ReplRingOrder1, Pred, Succ} = eh_repl_ring:get_ordered_list_pred_succ(NodeId, ReplRing, ReplRing, NodeOrder),
  FailureDetector = eh_system_config:get_failure_detector(AppConfig),
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  FailureDetector:set(NodeId, ReplRing),
  {Timestamp, _} = ReplDataManager:timestamp(),
  NewState1 = eh_node_state:update_state_ready(State),
  NewState2 = NewState1#eh_system_state{repl_ring_order=ReplRingOrder1, repl_ring=ReplRing1, predecessor=Pred, successor=Succ, timestamp=Timestamp},
  event_state("setup_repl.99", NewState2),
  {noreply, NewState2};

handle_cast({?EH_ADD_NODE, {Node, NodeList, NodeOrderList}}, 
            #eh_system_state{app_config=AppConfig}=State) ->
  FailureDetector = eh_system_config:get_failure_detector(AppConfig),
  NodeOrder = eh_system_config:get_node_order(AppConfig),
  NodeId = eh_system_config:get_node_id(AppConfig),
  {NodeList1, NodeOrderList1, Pred1, Succ1} = eh_repl_ring:get_ordered_list(NodeId, NodeList, NodeOrderList, NodeOrder),
  NewState2 = case eh_node_timestamp:valid_add_node_msg(Node, State) of
                ?EH_VALID_FOR_NEW      ->
                  UniqueIdGenerator = eh_system_config:get_unique_id_generator(AppConfig),
                  Ref = UniqueIdGenerator:unique_id(),
                  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
                  FailureDetector:set(Node, NodeList1),
                  {Timestamp, Snapshot} = ReplDataManager:timestamp(),
                  gen_server:cast({?EH_SYSTEM_SERVER, Succ1}, {?EH_SNAPSHOT, {Node, NodeList, NodeOrderList, Ref, {Timestamp, Snapshot}}}),
                  NewState1 = eh_node_state:update_state_transient(State),
                  NewState1#eh_system_state{repl_ring_order=NodeOrderList1, repl_ring=NodeList1, predecessor=Pred1, successor=Succ1, snapshot_ref=Ref};
                ?EH_VALID_FOR_EXISTING -> 
                  FailureDetector:set(Node),
                  State#eh_system_state{repl_ring_order=NodeOrderList1, repl_ring=NodeList1, predecessor=Pred1, successor=Succ1};
                _                      ->
                  State
              end,
  event_state("add_node.99", NewState2),
  {noreply, NewState2};

handle_cast({?EH_SNAPSHOT, {Node, NodeList, NodeOrderList,  Ref, {Timestamp, Snapshot}}}, 
            #eh_system_state{pre_msg_data=PreMsgData, app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  NodeOrder = eh_system_config:get_node_order(AppConfig),
  {NodeList1, NodeOrderList1, Pred1, Succ1} = eh_repl_ring:get_ordered_list_pred_succ(NodeId, NodeList, NodeOrderList, NodeOrder),
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  Q0 = ReplDataManager:snapshot(Timestamp, Snapshot),
  PendingPreMsgMap = eh_update_msg:filter_node_id(NodeId, PreMsgData),
  gen_server:cast({?EH_SYSTEM_SERVER, Node}, {?EH_UPDATE_SNAPSHOT, {Ref, Q0, PendingPreMsgMap}}),
  NewState2 = State#eh_system_state{repl_ring_order=NodeOrderList1, repl_ring=NodeList1, predecessor=Pred1, successor=Succ1},
  event_state("snapshot.99", NewState2),
  {noreply, NewState2};

handle_cast({?EH_UPDATE_SNAPSHOT, {Ref, Q0, PendingPreMsgData}}, 
            #eh_system_state{snapshot_ref=Ref, app_config=AppConfig}=State) ->
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  ReplDataManager:update_snapshot(Q0),
  PendingPreMsgData1 = case eh_node_timestamp:valid_pending_pre_msg_data(PendingPreMsgData, State) of
                         true  ->
                           PendingPreMsgData;
                         false ->
                           eh_system_util:new_map()
                       end,
  NewState2 = eh_node_state:update_state_snapshot(State),
  NewState3 = NewState2#eh_system_state{pending_pre_msg_data=PendingPreMsgData1},
  event_state("update_snapshot.99", NewState3),
  {noreply, NewState3};

handle_cast({?EH_UPDATE_SNAPSHOT, _}, State) ->
  {noreply, State};

handle_cast({?EH_QUERY, {From, Ref, {ObjectType, ObjectId}}}, 
            #eh_system_state{pre_msg_data=PreMsgData, app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  State1 = case eh_node_state:data_state(State) of
             ?EH_NOT_READY ->
               eh_query_handler:reply(From, Ref, eh_query_handler:error_node_unavailable(NodeId)),
               State;
             _             ->  
               case eh_update_msg:exist_map_msg(ObjectType, ObjectId, PreMsgData) of
                 undefined ->
                    eh_query_handler:query(ok, ObjectType, ObjectId, From, Ref, State);
                 MsgNodeId -> 
                    QueryHandler = eh_system_config:get_query_handler(AppConfig),
                    QueryHandler:process(ObjectType, ObjectId, MsgNodeId, From, Ref, State)
               end
           end,
  {noreply, State1};

handle_cast({?EH_QUERY_AQ, {ObjectType, ObjectId, From, Ref}},
            #eh_system_state{predecessor=Pred, app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  State1 = case eh_node_state:data_state(State) of
             ?EH_NOT_READY ->
               case Pred of
                 undefined ->
                   eh_query_handler:reply(From, Ref, eh_query_handler:error_node_unavailable(NodeId)),
                   State;
                 Other     ->
                   eh_query_handler:process_tail(ObjectType, ObjectId, Other, From, Ref, State)
               end;
             _             ->
               eh_query_handler:process_aq(ObjectType, ObjectId, From, Ref, State)
           end,
  {noreply, State1};

handle_cast({?EH_UPDATE, {From, Ref, ObjectList}},
            #eh_system_state{timestamp=Timestamp, successor=Succ, completed_set=CompletedSet, app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  NewState9 = case eh_node_state:client_state(State) of
                ?EH_NOT_READY ->
                  eh_query_handler:reply(From, Ref, eh_query_handler:error_node_unavailable(NodeId)),
                  State;
                _             ->
                  Timestamp1 = Timestamp+1,
                  {NodeId, UpdateList} = lists:keyfind(NodeId, 1, ObjectList),
                  UMsgList = eh_update_msg:get_msg(UpdateList, 
                                                   Timestamp1,
                                                   From,
                                                   NodeId,
                                                   Ref),
                  NewState1 = eh_node_timestamp:update_state_timestamp(Timestamp1, State),
                  case Succ of 
                    undefined ->
                      reply_to_client(fun eh_node_timestamp:persist_data/2, UMsgList, CompletedSet, NewState1);
                    _         -> 
                      send_pre_update_msg(fun eh_node_timestamp:no_persist_data/2, UMsgList, CompletedSet, NewState1)
                  end                            
              end,
  event_state("update.99", NewState9),
  {noreply, NewState9};

handle_cast({?EH_PRED_PRE_UPDATE, {UMsgList, CompletedSet}}, State) ->
  NewState1 = process_msg(?EH_PRED_PRE_UPDATE,
                          fun eh_node_timestamp:valid_pre_update_msg/2,
                          fun send_update_msg/4,
                          fun eh_node_timestamp:persist_data/2,
                          fun send_pre_update_msg/4,
                          fun eh_node_timestamp:no_persist_data/2,
                          UMsgList,
                          CompletedSet,
                          State),
  {noreply, NewState1};

handle_cast({?EH_SUCC_UPDATE, {UMsgList, CompletedSet}}, State) ->
  NewState1 = process_msg(?EH_SUCC_UPDATE,
                          fun eh_node_timestamp:valid_update_msg/2,
                          fun reply_to_client/4,
                          fun eh_node_timestamp:persist_data/2,
                          fun send_update_msg/4,
                          fun eh_node_timestamp:persist_data/2,
                          UMsgList,
                          CompletedSet,
                          State),
  {noreply, NewState1};

handle_cast({stop, Reason}, State) ->
  event_data("stop", status, stopped),
  {stop, Reason, State};

handle_cast(_Msg, State) ->
  {noreply, State}.


handle_info(Msg, #eh_system_state{repl_ring=ReplRing, successor=Succ, app_config=AppConfig}=State) ->
  FailureDetector = eh_system_config:get_failure_detector(AppConfig),
  NewState9 = case FailureDetector:detect(Msg) of
                {?EH_NODEDOWN, DownNode} ->
                  event_data("failure", node_down, eh_system_util:get_node_name(DownNode)),
%                  NodeId = eh_system_config:get_node_id(AppConfig),
%                  NewReplRing = eh_repl_ring:drop(DownNode, ReplRing),
%                  NewSucc = eh_repl_ring:successor(NodeId, NewReplRing),
%                  NewState1 = State#eh_system_state{repl_ring=NewReplRing, successor=NewSucc},
%                  NewState3 = case Succ =:= DownNode of
%                                true  ->
%                                  NewState2 = eh_node_timestamp:update_state_merge_completed_set(DownNode, NewState1),
%                                  send_down_msg(NewState2);
%                                false ->
%                                  NewState1
%                              end,
%                  event_state("failure.99", NewState3),
%                  NewState3;
                  State; 
                _                        ->
                  State 
              end,
   {noreply, NewState9}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

event_state(Msg, State) ->
  eh_event:state(?MODULE, Msg, State).

event_message(Msg, MsgList, CompletedSet) ->
  eh_event:message(?MODULE, Msg, {MsgList, CompletedSet}).

event_data(Msg, DataMsg, Data) ->
  eh_event:data(?MODULE, Msg, DataMsg, Data).

reply_to_client(PersistFun,
                UMsgList,
                _CompletedSet,
                State) -> 
  State1 = PersistFun(UMsgList, State),
  {_, ClientId, _, Ref, DataList} = eh_update_msg:get_data_list(UMsgList),
  eh_query_handler:reply(ClientId, Ref, eh_query_handler:updated(DataList)),
  eh_node_timestamp:update_state_client_reply(UMsgList, State1).

send_msg(Tag, 
         PersistFun,
         UMsgList,
         CompletedSet,
         #eh_system_state{predecessor=Pred, successor=Succ}=State) ->
  State1 = PersistFun(UMsgList, State),
  State2 = eh_node_timestamp:update_state_new_msg(Tag, UMsgList, State1),
  Dest = case Tag of
           ?EH_PRED_PRE_UPDATE ->
             Succ;
           ?EH_SUCC_UPDATE     ->
             Pred
         end,
  gen_server:cast({?EH_SYSTEM_SERVER, Dest}, {Tag, {UMsgList, CompletedSet}}),
  State2.  

send_pre_update_msg(PersistFun,
                    UMsgList,
                    CompletedSet,
                    State) ->
  send_msg(?EH_PRED_PRE_UPDATE, PersistFun, UMsgList, CompletedSet, State).
 
send_update_msg(PersistFun,
                UMsgList,
                CompletedSet,
		State) ->
  send_msg(?EH_SUCC_UPDATE, PersistFun, UMsgList, CompletedSet, State).

process_msg(Tag,
            ValidateMsgFun,
            ReturnedMsgFun,
            ReturnedMsgPersistFun,
            ValidMsgFun,
            ValidMsgPersistFun,
            UMsgList,
            CompletedSet,
            State) ->
  DisplayTag = eh_system_util:display_atom_to_list(Tag),
  {MsgTimestamp, _, _, _} = eh_update_msg:get_msg_param(UMsgList),
  NewState8 = case eh_node_state:msg_state(State) of
                ?EH_NOT_READY ->
                  event_message(DisplayTag++".invalid_msg", UMsgList, CompletedSet),
                  State;
                _             ->
                  case ValidateMsgFun(UMsgList, State) of
                    {false, _, NewState1}           ->
                      event_message(DisplayTag++".duplicate_msg", UMsgList, CompletedSet),
                      NewState1;
                    {true, ?EH_HEAD_MSG, NewState1} ->
                      event_message(DisplayTag++".valid_head_msg", UMsgList, CompletedSet),
                      NewState2 = eh_node_timestamp:update_state_timestamp(MsgTimestamp, NewState1),
                      NewState3 = eh_node_timestamp:update_state_completed_set(CompletedSet, NewState2),
                      ReturnedMsgFun(ReturnedMsgPersistFun, UMsgList, CompletedSet, NewState3);
                    {true, ?EH_TAIL_MSG, NewState1} ->
                      event_message(DisplayTag++".valid_tail_msg", UMsgList, CompletedSet),
                      NewState2 = eh_node_timestamp:update_state_timestamp(MsgTimestamp, NewState1),
                      NewState3 = eh_node_timestamp:update_state_msg_data(CompletedSet, NewState2),
                      ReturnedMsgFun(ReturnedMsgPersistFun, UMsgList, CompletedSet, NewState3);
                    {true, _, NewState1}            ->
                      event_message(DisplayTag++".valid_ring_msg", UMsgList, CompletedSet),
                      NewState2 = eh_node_timestamp:update_state_timestamp(MsgTimestamp, NewState1),
                      NewState3 = eh_node_timestamp:update_state_msg_data(CompletedSet, NewState2),
                      ValidMsgFun(ValidMsgPersistFun, UMsgList, CompletedSet, NewState3)
                  end
              end,
  event_state(DisplayTag++".99", NewState8),
  NewState8.

%send_down_msg(#eh_system_state{pre_msg_data=PreMsgData, msg_data=MsgData, successor=undefined}=State) ->
%  eh_system_util:fold_map(fun(K, V, S) -> process_down_msg(fun persist_data/3, K, V, S) end, State, PreMsgData),
%  eh_system_util:fold_map(fun(K, V, S) -> process_down_msg(fun no_persist_data/3, K, V, S) end, State, MsgData),  
%  State#eh_system_state{pre_msg_data=eh_system_util:new_map(), msg_data=eh_system_util:new_map(), ring_completed_map=eh_system_util:new_map()};
%send_down_msg(#eh_system_state{pre_msg_data=PreMsgData, msg_data=MsgData}=State) ->
%  State1 = send_down_msg(fun no_persist_data/3, fun send_pre_update_msg/4, fun persist_data/3, fun send_update_msg/4, PreMsgData, State),
%  send_down_msg(fun no_persist_data/3, fun send_update_msg/4, fun no_persist_data/3, fun reply_to_client/4, MsgData, State1).

%down_fold_fun(PersistRingFun,
%              RingFun,
%              PersistReturnedFun,
%              ReturnedFun,
%              NodeId,
%              ReplRing,
%              UMsgKey, 
%              #eh_update_msg_data{node_id=MsgNodeId}=UMsgData,
%              State) ->
%  OriginNodeId = eh_repl_ring:originating_node_id(MsgNodeId, ReplRing),
%  case NodeId =:= OriginNodeId of
%    true  ->
%      ReturnedFun(PersistReturnedFun, UMsgKey, UMsgData, State);
%    false ->
%      RingFun(PersistRingFun, UMsgKey, UMsgData, State)
%  end.

%send_down_msg(PersistRingFun,
%              RingFun,
%              PersistReturnedFun,
%              ReturnedFun,
%              MsgData,
%              #eh_system_state{repl_ring=ReplRing, app_config=AppConfig}=State) ->
%  NodeId = eh_system_config:get_node_id(AppConfig),
%  eh_system_util:fold_map(fun(K, V, S) -> down_fold_fun(PersistRingFun, RingFun, PersistReturnedFun, ReturnedFun, NodeId, ReplRing, K, V, S) end, State, MsgData).

%process_down_msg(PersistFun,
%                 UMsgKey,
%                 UMsgData,
%                 State) ->
%  reply_to_client(PersistFun, UMsgKey, UMsgData, State).




                 





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

-module(eh_update_msg).

-export([get_msg/5,
         get_msg_param/1,
         get_data_list/1,
         get_object_list/1,
         get_msg_key_list/1,
         get_map_msg_list/1,
         exist_map_msg/3,
         filter_node_id/2,
         partition_on_timestamp_node_id/3,
         add_list_to_map/2]).

-include("erlang_craq.hrl").

get_msg_key_list(UMsgList) ->
  lists:map(fun({UMsgKey, _}) -> UMsgKey end, UMsgList).

get_map_msg_list(Map) ->
  maps:fold(fun(K, V, Acc) -> [{K, V} | Acc] end, [], Map).

get_msg(MsgList, Timestamp, From, NodeId, Ref) ->
  get_msg(MsgList, Timestamp, From, NodeId, Ref, []).

get_msg([{ObjectType, ObjectId, UpdateData} | Rest], Timestamp, From, NodeId, Ref, Acc) ->
  get_msg(Rest, Timestamp, From, NodeId, Ref,
          [{#eh_update_msg_key{timestamp=Timestamp, object_type=ObjectType, object_id=ObjectId},
            #eh_update_msg_data{update_data=UpdateData, client_id=From, node_id=NodeId, msg_ref=Ref}} | Acc]);
get_msg([], _, _, _, _, Acc) ->
  Acc.

get_data_list(UMsgList) ->
  get_data_list(UMsgList, {undefined, undefined, undefined, undefined, []}).

get_data_list([{#eh_update_msg_key{timestamp=Timestamp, object_type=ObjectType, object_id=ObjectId},
                #eh_update_msg_data{update_data=UpdateData, client_id=ClientId, node_id=NodeId, msg_ref=Ref}} | Rest], 
              {_, _, _, _, Acc}) ->
  get_data_list(Rest, {Timestamp, ClientId, NodeId, Ref, [{ObjectType, ObjectId, UpdateData} | Acc]});
get_data_list([], {Timestamp, ClientId, NodeId, Ref, Acc}) ->
  {Timestamp, ClientId, NodeId, Ref, Acc}.

get_msg_param([{#eh_update_msg_key{timestamp=Timestamp},
                #eh_update_msg_data{client_id=ClientId, node_id=NodeId, msg_ref=Ref}} | _Rest]) ->
  {Timestamp, ClientId, NodeId, Ref};
get_msg_param([]) ->
  {undefined, undefined, undefined, undefined}.

get_object_list(DataList) ->
  get_object_list(DataList, []).

get_object_list([{ObjectType, ObjectId, _} | Rest], Acc) ->
  get_object_list(Rest, [{ObjectType, ObjectId} | Acc]);
get_object_list([], Acc) ->
  Acc.

exist_map_msg(ObjectType, ObjectId, Map) ->
  maps:fold(fun(#eh_update_msg_key{object_type=XOT, object_id=XOI}, #eh_update_msg_data{node_id=NodeId}, Acc) -> 
              case Acc of
                undefined ->
                  case ObjectType =:= XOT andalso ObjectId =:= XOI of
                    true  ->
                      NodeId;
                    false ->
                      undefined
                  end;
                Other     ->
                  Other
              end end, undefined, Map).
 
filter_node_id(NodeId, Map) ->
  eh_system_util:filter_map(fun(_K, #eh_update_msg_data{node_id=MsgNodeId}) -> NodeId =:= MsgNodeId end, Map).

partition_on_timestamp_node_id(Timestamp, NodeId, Map) ->
  eh_system_util:partition_map(fun(#eh_update_msg_key{timestamp=TimestampX}, #eh_update_msg_data{node_id=NodeIdX}) ->
                               Timestamp =:= TimestampX andalso NodeId =:= NodeIdX end, Map).

add_list_to_map(List, Map) ->
  lists:foldl(fun({K, V}, Acc) -> maps:put(K, V, Acc) end, Map, List).












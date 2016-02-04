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

-module(eh_node_state).

-export([update_state_msg/1,
         update_state_snapshot/1,
         update_state_ready/1,
         update_state_transient/1,
         data_state/1,
         client_state/1,
         snapshot_state/1,
         msg_state/1,
         display_state/1]).

-include("erlang_craq.hrl").

update_state(NodeStatus, State) ->
  State#eh_system_state{node_status=NodeStatus}.

update_state_msg(#eh_system_state{node_status=?EH_TRANSIENT}=State) ->
  update_state(?EH_TRANSIENT_TU, State);
update_state_msg(#eh_system_state{node_status=?EH_TRANSIENT_DU}=State) ->
  update_state(?EH_READY, State);
update_state_msg(State) ->
  State.

update_state_snapshot(#eh_system_state{node_status=?EH_TRANSIENT}=State) ->
  update_state(?EH_TRANSIENT_DU, State);
update_state_snapshot(#eh_system_state{node_status=?EH_TRANSIENT_TU}=State) ->
  update_state(?EH_READY, State);
update_state_snapshot(State) ->
  State.

update_state_ready(State) ->
  update_state(?EH_READY, State).

update_state_transient(State) ->
  update_state(?EH_TRANSIENT, State).

data_state(#eh_system_state{node_status=?EH_READY}) ->
  ?EH_READY;
data_state(#eh_system_state{node_status=?EH_TRANSIENT_DU, pending_pre_msg_data=PendingPreMsgData}) ->
  case eh_system_util:is_empty_map(PendingPreMsgData) of
    true  ->
      ?EH_READY;
    false ->
      ?EH_NOT_READY
  end;
data_state(_) ->
  ?EH_NOT_READY.

snapshot_state(#eh_system_state{node_status=?EH_READY}) ->
  ?EH_READY;
snapshot_state(#eh_system_state{node_status=?EH_TRANSIENT_DU}) ->
  ?EH_READY;
snapshot_state(_) ->
  ?EH_NOT_READY.

client_state(#eh_system_state{node_status=?EH_READY}) ->
  ?EH_READY;
client_state(_) ->
  ?EH_NOT_READY.

msg_state(#eh_system_state{node_status=?EH_NOT_READY}) ->
  ?EH_NOT_READY;
msg_state(_) ->
  ?EH_READY.

display_state(#eh_system_state{node_status=NodeStatus}) ->
  eh_system_util:display_atom_to_list(NodeStatus).




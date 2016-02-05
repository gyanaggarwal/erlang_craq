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

-define(STATUS_INACTIVE,           0).
-define(STATUS_ACTIVE,             1).

-define(READ_TIMEOUT,              500).
-define(UPDATE_TIMEOUT,            1000).

-define(EH_BAD_DATA,               eh_bad_data).

-define(EH_NODEDOWN,               nodedown).
-define(EH_BEING_UPDATED,          being_updated).
-define(EH_NODE_UNAVAILABLE,       node_unavailable).
-define(EH_UPDATED,                updated).

-define(EH_SORTED,                 sorted).
-define(EH_USER_DEFINED,           user_defined).

-define(EH_INVALID_MSG,            eh_invalid_msg).
-define(EH_VALID_FOR_EXISTING,     eh_valid_for_existing).
-define(EH_VALID_FOR_NEW,          eh_valid_for_new).
-define(EH_RING_MSG,               eh_ring_msg).
-define(EH_HEAD_MSG,               eh_head_msg).
-define(EH_TAIL_MSG,               eh_tail_msg).

-define(EH_SETUP_REPL,             eh_setup_repl).
-define(EH_ADD_NODE,               eh_add_node).
-define(EH_UPDATE,                 eh_update).
-define(EH_SUCC_UPDATE,            eh_succ_update).
-define(EH_PRED_PRE_UPDATE,        eh_pred_pre_update).
-define(EH_TIMESTAMP,              eh_timestamp).
-define(EH_QUERY,                  eh_query).
-define(EH_QUERY_AQ,               eh_query_aq).
-define(EH_SNAPSHOT,               eh_snapshot).
-define(EH_UPDATE_SNAPSHOT,        eh_update_snapshot).
-define(EH_DATA_VIEW,              eh_data_view).
-define(EH_CHECK_DATA,             eh_check_data).
-define(EH_GET_DATA,               eh_get_data).

-define(EH_NOT_READY,              eh_not_ready).
-define(EH_READY,                  eh_ready).
-define(EH_TRANSIENT,              eh_transient).
-define(EH_TRANSIENT_DU,           eh_transient_data_updated).
-define(EH_TRANSIENT_TU,           eh_transient_timestamp_updated).

-define(EH_SYSTEM_SERVER,          eh_system_server).
-define(EH_DATA_SERVER,            eh_data_server).

-record(eh_app_config,          {node_id                             :: atom(),
                                 node_order                          :: ?EH_SORTED | ?EH_USER_DEFINED,
                                 failure_detector                    :: atom(),
                                 repl_data_manager                   :: atom(),
                                 storage_data                        :: atom(),
                                 write_conflict_resolver             :: atom(),
                                 unique_id_generator                 :: atom(),
                                 query_handler                       :: atom(),
                                 query_aq_handler                    :: atom(),
                                 file_repl_data                      :: string(),
                                 file_repl_log                       :: standard_io | string(),
                                 debug_mode=false                    :: true | false,
                                 sup_restart_intensity               :: non_neg_integer(),
                                 sup_restart_period                  :: non_neg_integer(),
                                 sup_child_shutdown                  :: non_neg_integer()}).

-record(eh_storage_key,         {object_type                         :: atom(),
                                 object_id                           :: term()}).

-record(eh_storage_value,       {timestamp                           :: non_neg_integer(),
                                 data_index                          :: non_neg_integer(),
                                 status=?STATUS_ACTIVE               :: ?STATUS_ACTIVE | ?STATUS_INACTIVE,
                                 column                              :: atom(),
                                 value                               :: term()}).

-record(eh_storage_data,        {object_type                         :: atom(),
                                 object_id                           :: term(),
                                 timestamp                           :: non_neg_integer(),
                                 data_index                          :: non_neg_integer(),
                                 status=?STATUS_ACTIVE               :: ?STATUS_ACTIVE | ?STATUS_INACTIVE,
                                 column                              :: atom(),
                                 value                               :: term()}).

-record(eh_update_msg_key,      {timestamp                           :: non_neg_integer(),
                                 object_type                         :: atom(),
                                 object_id                           :: term()}).

-record(eh_update_msg_data,     {update_data                         :: term(),
                                 client_id                           :: pid(),
                                 node_id                             :: atom(),
                                 msg_ref                             :: term()}).

-record(eh_system_state,        {node_status=?EH_NOT_READY           :: ?EH_NOT_READY | 
                                                                        ?EH_TRANSIENT |
                                                                        ?EH_TRANSIENT_TU |
                                                                        ?EH_TRANSIENT_DU |
                                                                        ?EH_READY,
                                 timestamp=0                         :: non_neg_integer(),
                                 repl_ring_order=[]                  :: list(),
                                 repl_ring=[]                        :: list(),
                                 predecessor                         :: atom(),
                                 successor                           :: atom(),
                                 pre_msg_data=maps:new()             :: maps:map(),
                                 msg_data=maps:new()                 :: maps:map(),
                                 completed_set=sets:new()            :: sets:set(),
                                 pending_pre_msg_data=maps:new()     :: maps:map(),
                                 query_data=maps:new()               :: maps:map(),
                                 snapshot_ref                        :: term(),
                                 app_config                          :: #eh_app_config{}}).

-record(eh_data_state,          {timestamp=0                         :: non_neg_integer(),
                                 transient_timestamp=0               :: non_neg_integer(),
                                 data_index_list=[]                  :: list(),
                                 data=maps:new()                     :: maps:map(),
                                 transient_data=queue:new()          :: queue:queue(),
                                 file                                :: file:io_device(),
                                 app_config                          :: #eh_app_config{}}).




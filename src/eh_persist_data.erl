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

-module(eh_persist_data).

-export([persist_data/2,
         no_persist_data/2]).

-include("erlang_craq.hrl").

persist_data(UMsgList,
             #eh_system_state{app_config=AppConfig}=State) ->
  {Timestamp, _, _, _, DataList} = eh_update_msg:get_data_list(UMsgList),
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  ReplDataManager:update(eh_node_state:snapshot_state(State), Timestamp, DataList),
  eh_query_handler:process_pending(DataList, State).

no_persist_data(_, State) ->
  State.









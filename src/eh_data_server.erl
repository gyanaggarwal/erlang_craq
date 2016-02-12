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

-module(eh_data_server).

-behavior(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-include("erlang_craq.hrl").

-define(SERVER, ?EH_DATA_SERVER).

start_link(AppConfig) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [AppConfig], []).

init([AppConfig]) ->
  DataDir = eh_system_config:get_data_dir(AppConfig),
  FileName = eh_system_config:get_file_repl_data(AppConfig),
  {_, Timestamp, DataIndexList, D0} = eh_storage_data_operation_api:read_all(AppConfig, eh_file_name:get_full_versioned_file_names(DataDir, FileName)),
  FileVersionNum = eh_file_name:get_version_num(DataDir, FileName)+1,
  VersionedFileName = eh_file_name:get_full_versioned_file_name(FileVersionNum, AppConfig),
  {ok, File} = eh_storage_data_operation_api:open(VersionedFileName),
  State = #eh_data_state{timestamp=Timestamp, 
			 data_index_list=DataIndexList, 
			 data=D0, 
			 file_version_num=FileVersionNum, 
			 file=File, 
			 app_config=AppConfig},
  {ok, State}.


handle_call(?EH_TIMESTAMP, 
            _From, 
            State) ->
  {reply, {State#eh_data_state.timestamp, State#eh_data_state.data_index_list}, State};

handle_call({?EH_QUERY, {ObjectType, ObjectId}}, 
            _From, 
            #eh_data_state{data=Data}=State) ->
  Reply = eh_data_util:query_data(ObjectType, ObjectId, Data),
  {reply, {ObjectType, ObjectId, Reply}, State};

handle_call({?EH_SNAPSHOT, {Timestamp, DataIndex}}, 
            _From, 
            #eh_data_state{data=Data}=State) ->
  Reply = eh_data_util:snapshot_data(Timestamp, DataIndex, Data),
  {reply, Reply, State};

handle_call({?EH_UPDATE, {?EH_NOT_READY, Timestamp, UpdateList}}, 
            _From, 
            #eh_data_state{transient_timestamp=TTimestamp, transient_data=TData}=State) when Timestamp > TTimestamp ->
  TData1 = eh_data_util:make_transient_data(UpdateList, Timestamp, TData),
  {reply, ok, State#eh_data_state{transient_timestamp=Timestamp, transient_data=TData1}};

handle_call({?EH_UPDATE, {?EH_NOT_READY, _Timestamp, _}}, 
            _From, 
            State) ->
  {reply, ok, State};

handle_call({?EH_UPDATE, {?EH_READY, Timestamp, UpdateList}}, 
            _From, 
            #eh_data_state{data=Data, 
			   transient_data=TQ0, 
			   timestamp=StateTimestamp, 
			   data_index_list=StateDataIndexList}=State) ->
  {_, {_, DIL0}, Q0, D0} = eh_data_util:make_data(UpdateList, Timestamp, {StateTimestamp, StateDataIndexList}, TQ0, Data),
  write_data(Timestamp, DIL0, D0, Q0, State);

handle_call({?EH_UPDATE_SNAPSHOT, Qi0}, 
            _From, 
            #eh_data_state{data=Data, 
			   transient_data=TData, 
			   timestamp=StateTimestamp, 
			   data_index_list=StateDataIndexList}=State) ->
  {Timestamp, DIL0, Q0, D0} = eh_data_util:merge_data(Qi0, TData, {StateTimestamp, StateDataIndexList}, Data),
  write_data(Timestamp, DIL0, D0, Q0, State);

handle_call(?EH_DATA_VIEW, 
            _From, 
            #eh_data_state{data=Data}=State) ->
  Reply = eh_data_util:data_view(Data),
  {reply, Reply, State};

handle_call({?EH_GET_DATA, {ObjectType, ObjectId}},
            _From,
            #eh_data_state{data=Data}=State) ->
  Reply = eh_data_util:get_data(ObjectType, ObjectId, Data),
  {reply, Reply, State};
        
handle_call({?EH_CHECK_DATA, {Timestamp, DataList}},
            _From, 
            #eh_data_state{data=Data}=State) ->
  Reply = eh_data_util:check_data(Timestamp, DataList, Data),
  {reply, Reply, State}.

handle_cast({stop, Reason}, State) ->
  {stop, Reason, State};

handle_cast(_Msg, State) ->
  {noreply, State}.


handle_info(_Msg, State) ->
  {noreply, State}.


code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


terminate(_Reason, #eh_data_state{file=File}) ->
  eh_storage_data_operation_api:close(File).

write_data(Timestamp, DIL0, D0, Q0, 
            #eh_data_state{file=File,
                           file_version_num=FileVersionNum,
                           data_update_count=DataUpdateCount,
                           app_config=AppConfig}=State) ->
    {Tag, File1, FileVersionNum1, DataUpdateCount1} = eh_storage_data_operation_api:write(AppConfig, File, Q0, FileVersionNum, DataUpdateCount),
    State1 = State#eh_data_state{timestamp=Timestamp, 
                                 file=File1,
	                         file_version_num=FileVersionNum1,
                                 data_update_count=DataUpdateCount1,
                                 data_index_list=DIL0,
                                 data=D0,
			         transient_data=queue:new()},
    case Tag of
	ok ->
	    {reply, ok, State1}; 
	_  ->
	    {stop, Tag, ok, State1}
    end.
      

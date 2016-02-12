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

-module(eh_storage_data_operation_api).

-export([open/1,
         close/1,
         read/2,
	 read_all/2,
         write/5]).

-include("erlang_craq.hrl").

-spec open(FileName :: string()) -> {ok, file:io_device()} | {error, atom()}.
open(FileName) ->
  eh_persist_storage_data:open_data_file(FileName).
      
-spec close(File :: file:io_device()) -> ok | {error, atom()}.
close(File) ->
  eh_persist_storage_data:close_data_file(File).

-spec read_all(AppConfig :: #eh_app_config{}, FileNames :: list()) -> {ok | error, non_neg_integer(), list(), maps:map()}.
read_all(AppConfig, FileNames) ->
    EntryOperation = eh_system_config:get_storage_data(AppConfig),
    read_all(EntryOperation, FileNames, {ok, 0, [], maps:new()}).

-spec read_all(EntryOperation :: atom(), FileNames :: list(), Acc :: term()) -> {ok | error, non_neg_integer(), list(), maps:map()}.
read_all(EntryOperation, [FileName | RFileNames], {_, Timestamp, DataIndex, M0}) ->
    {ok, File} = open(FileName),
    Acc = read(EntryOperation, File, 0, {Timestamp, DataIndex}, M0),
    close(File),
    read_all(EntryOperation, RFileNames, Acc);
read_all(_EntryOperation, [], Acc) ->
    Acc.

-spec read(AppConfig :: #eh_app_config{}, File :: file:io_device()) ->  {ok | error, non_neg_integer(), list(), maps:map()}.
read(AppConfig, File) ->
  EntryOperation = eh_system_config:get_storage_data(AppConfig),
  read(EntryOperation, File, 0, {0, []}, maps:new()).

-spec read(EntryOperation :: atom(), File :: file:io_device(), Loc :: non_neg_integer(), {Timestamp :: non_neg_integer(), DataIndex :: list()}, M0 :: maps:map()) 
      -> {ok | error, non_neg_integer(), list(), maps:map()}.
read(EntryOperation, File, Loc, {Timestamp, DataIndex}, M0) ->
  case eh_persist_storage_data:read_data(File, Loc, EntryOperation:header_byte_size()) of
    eof               -> 
      {ok, Timestamp, DataIndex, M0};
    {error, _}        -> 
      {error, Timestamp, DataIndex, M0};
    {ok, Loc1, HData} ->
      DataSize =  EntryOperation:entry_header(HData),
      case eh_persist_storage_data:read_data(File, Loc1, DataSize) of
        eof               ->
          eh_persist_storage_data:truncate_data(File, Loc),
          {error, Timestamp, DataIndex, M0};
        {error, _}        ->
          {error, Timestamp, DataIndex, M0};
        {ok, Loc2, RData} ->
          case EntryOperation:binary_to_entry(HData, RData) of
            ?EH_BAD_DATA ->
              eh_persist_storage_data:truncate_data(File, Loc),
              {error, Timestamp, DataIndex, M0};
            {ok, Entry}  ->
              read(EntryOperation, File, Loc2, eh_data_util:update_timestamp(Entry, {Timestamp, DataIndex}), eh_data_util:add_key_value(Entry, M0))
          end
      end
  end.

-spec write(AppConfig :: #eh_app_config{}, File :: file:io_device(), Q0 :: queue:queue(), FileVersionNum :: non_neg_integer(), DataUpdateCount :: non_neg_integer()) 
            -> {ok, file:io_device(), non_neg_integer(), non_neg_integer()} | {error, atom()}.          
write(AppConfig, File, Q0, FileVersionNum, DataUpdateCount)->
  EntryOperation = eh_system_config:get_storage_data(AppConfig),
  write_entries(EntryOperation, File, Q0),
  DataCheckPoint = eh_system_config:get_data_checkpoint(AppConfig),
  case DataUpdateCount >= DataCheckPoint of
    true  ->
      case read(AppConfig, File) of
        {ok, _, _, _}    ->
          close(File),
          FileVersionNum1 = FileVersionNum+1,
          VersionedFileName = eh_file_name:get_full_versioned_file_name(FileVersionNum1, AppConfig),
          {ok, File1} = open(VersionedFileName),
          {ok, File1, FileVersionNum1, 0};
        {error, _, _, _} ->
          {error, File, FileVersionNum, DataUpdateCount+1}
      end;
    false ->
      {ok, File, FileVersionNum, DataUpdateCount+1}
  end.

-spec write_entries(EntryOperation :: atom(), File :: file:io_device(), Q0 :: queue:queue()) -> ok | {error, atom()}.
write_entries(EntryOperation, File, Q0) ->
  case queue:out(Q0) of
    {empty, _}           ->
      file:sync(File);
    {{value, Entry}, Q1} ->
      ok = write_entry(EntryOperation, File, Entry),
      write_entries(EntryOperation, File, Q1)
  end.

-spec write_entry(EntryOperation :: atom(), File :: file:io_device(), Entry :: #eh_storage_data{}) -> ok | {error, atom()}.                      
write_entry(EntryOperation, File, Entry) ->
  Bin = EntryOperation:entry_to_binary(Entry),
  eh_persist_storage_data:write_data(File, Bin).




    
  

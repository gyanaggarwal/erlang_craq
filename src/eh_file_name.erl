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

-module(eh_file_name).

-export([get_file_name/2,
         get_full_file_name/2,
	 get_full_versioned_file_name/2,
         get_full_versioned_file_name/4,
         get_version_num/2,
         get_full_versioned_file_names/2]).

-include("erlang_craq.hrl").

-spec get_file_name(NodeName :: string(), FileName :: string()) -> string() | atom().
get_file_name(_, standard_io) ->
    standard_io;
get_file_name(NodeName, FileName) ->
    NodeName ++ FileName.

-spec get_full_file_name(DataDir :: string(), FileName :: string() | atom()) -> string() | atom().
get_full_file_name(_, standard_io) ->
    standard_io;
get_full_file_name(DataDir, FileName) ->
    DataDir ++ FileName.

-spec get_full_versioned_file_name(DataDir :: string(), FileName :: string(), Suffix :: string(), Version :: non_neg_integer()) -> string().
get_full_versioned_file_name(DataDir, FileName, Suffix, Version) ->
    VersionSuffix = Suffix ++ integer_to_list(Version),
    get_full_file_name(DataDir, FileName) ++ "." ++ lists:sublist(VersionSuffix, (length(VersionSuffix)-length(Suffix)+1), length(Suffix)).

-spec get_full_versioned_file_name(Version :: non_neg_integer(), AppConfig :: #eh_app_config{}) -> string().
get_full_versioned_file_name(Version, AppConfig) -> 
    DataDir = eh_system_config:get_data_dir(AppConfig),
    FileName = eh_system_config:get_file_repl_data(AppConfig),
    FileNameSuffix = eh_system_config:get_file_repl_data_suffix(AppConfig),
    get_full_versioned_file_name(DataDir, FileName, FileNameSuffix, Version).

-spec get_file_names(DataDir :: string(), FileName :: string()) -> list().
get_file_names(DataDir, FileName) ->
    {ok, FileList} = file:list_dir(DataDir),    
    FileList1 = lists:filter(fun(FN) -> lists:prefix(FileName, FN) end, FileList),
    lists:sort(fun(FN1, FN2) -> get_version(FN1, FileName) =< get_version(FN2, FileName) end, FileList1).
		        
-spec get_version(FullFileName :: string(), FileName :: string()) -> non_neg_integer().
get_version(FullFileName, FileName) ->
    list_to_integer(lists:nthtail(length(FileName)+1, FullFileName)).

-spec get_version_num(DataDir :: string(), FileName :: string()) -> non_neg_integer().
get_version_num(DataDir, FileName) ->
    FileList = get_file_names(DataDir, FileName),
    case FileList of
	[] ->
	    0;
	_  ->
	    get_version(lists:last(FileList), FileName)
    end.
   
-spec get_full_versioned_file_names(DataDir ::string(), FileName :: string()) -> list().
get_full_versioned_file_names(DataDir, FileName) ->
    lists:map(fun(FN) -> get_full_file_name(DataDir, FN) end, get_file_names(DataDir, FileName)).


		      
    














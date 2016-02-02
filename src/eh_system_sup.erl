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

-module(eh_system_sup).

-behavior(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()} | term().
start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec init(Arg :: list()) -> {ok, {tuple(), list()}}.
init([]) ->
  AppConfig        = eh_system_config:get_env(),
  RestartIntensity = eh_system_config:get_sup_restart_intensity(AppConfig),
  RestartPeriod    = eh_system_config:get_sup_restart_period(AppConfig),
  ChildShutdown    = eh_system_config:get_sup_child_shutdown(AppConfig),

  Htas  = {eh_system_server, {eh_system_server, start_link, [AppConfig]},
           permanent, ChildShutdown, worker, [eh_system_server]},
  
  Data  = {eh_data_server, {eh_data_server, start_link, [AppConfig]},
           permanent, ChildShutdown, worker, [eh_data_server]},
 
  Childern        = [Data, Htas],
  RestartStrategy = {rest_for_one, RestartIntensity, RestartPeriod},

  {ok, {RestartStrategy, Childern}}.


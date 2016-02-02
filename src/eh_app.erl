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

-module(eh_app).

-behavior(application).

-export([start/2, stop/1]).

-spec start(StartType :: atom(), StartArgs :: term()) -> {ok, pid()} | {error, term()}.
start(_StartType, _StartArgs) ->
  {ok, Pid} = eh_sup:start_link(),
  AppConfig = eh_system_config:get_env(), 
  case eh_system_config:get_debug_mode(AppConfig) of
    true  ->
      eh_event_handler:add_handler(AppConfig);
    false ->
      ok
  end, 
  {ok, Pid}.

-spec stop(State :: term()) -> ok.
stop(_State) ->
  ok.

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

-module(eh_failure_detector_api).

-behavior(eh_failure_detector).

-export([set/2,
         set/1,
         detect/1]).

-include("erlang_craq.hrl").

set(Node, NodeList) ->
  NewNodeList = lists:delete(Node, NodeList),
  lists:foreach(fun(N) -> monitor(process, {?EH_SYSTEM_SERVER, N}) end, NewNodeList),
  ok.

set(Node) ->
  monitor(process, {?EH_SYSTEM_SERVER, Node}).

detect({'DOWN', MRef, process, {?EH_SYSTEM_SERVER, Node}, _Reason}) ->
  demonitor(MRef, [flush]),
  {?EH_NODEDOWN, Node};
detect(_) ->
  ok.

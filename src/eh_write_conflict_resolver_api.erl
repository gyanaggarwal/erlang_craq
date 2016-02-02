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

-module(eh_write_conflict_resolver_api).

-behavior(eh_write_conflict_resolver).

-export([resolve/2]).

-include("erlang_craq.hrl").

resolve({?EH_PRED_PRE_UPDATE, Value1}, {?EH_PRED_PRE_UPDATE, Value2}) ->
  max(Value1, Value2);
resolve({?EH_PRED_PRE_UPDATE, _Value1}, {?EH_SUCC_UPDATE, Value2}) ->
  Value2;
resolve({?EH_SUCC_UPDATE, Value1}, {?EH_PRED_PRE_UPDATE, _Value2}) ->
  Value1.

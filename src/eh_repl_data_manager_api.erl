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

-module(eh_repl_data_manager_api).

-behavior(eh_repl_data_manager).

-export([update/3,
         query/1,
         timestamp/0,
         snapshot/2,
         update_snapshot/1,
         data_view/0,
         check_data/1]).

-include("erlang_craq.hrl").

-spec update(NodeState :: atom(), Timestamp :: non_neg_integer(), Msg :: term()) -> ok.
update(NodeState, Timestamp, Msg) ->
  gen_server:call(?EH_DATA_SERVER, {?EH_UPDATE, {NodeState, Timestamp, Msg}}).

-spec timestamp() -> {non_neg_integer(), term()}.
timestamp() ->
  gen_server:call(?EH_DATA_SERVER, ?EH_TIMESTAMP).

-spec query(Msg :: term()) -> {atom(), term(), list()}.
query(Msg) ->
  gen_server:call(?EH_DATA_SERVER, {?EH_QUERY, Msg}).

-spec snapshot(Timestamp :: non_neg_integer(), Snapshot :: term()) -> queue:queue().
snapshot(Timestamp, Snapshot) ->
  gen_server:call(?EH_DATA_SERVER, {?EH_SNAPSHOT, {Timestamp, Snapshot}}).

-spec update_snapshot(Q0 :: queue:queue()) -> ok.
update_snapshot(Q0) ->
  gen_server:call(?EH_DATA_SERVER, {?EH_UPDATE_SNAPSHOT, Q0}).

-spec data_view() -> term().
data_view() ->
  gen_server:call(?EH_DATA_SERVER, ?EH_DATA_VIEW).

-spec check_data(Msg :: term()) -> true | false.
check_data(Msg) ->
  gen_server:call(?EH_DATA_SERVER, {?EH_CHECK_DATA, Msg}).

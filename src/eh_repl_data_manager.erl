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

-module(eh_repl_data_manager).

-callback update(NodeState :: atom(), Timestamp :: non_neg_integer(), Msg :: term()) -> ok.

-callback timestamp() -> {non_neg_integer(), term()}.

-callback query(Msg :: term()) -> {atom(), term(), list()}.

-callback snapshot(Timestamp :: non_neg_integer(), Snapshot :: term()) -> queue:queue().

-callback update_snapshot(Q0 :: queue:queue()) -> ok.

-callback data_view() -> term().

-callback get_data(Msg :: term()) -> term().

-callback check_data(Msg :: term()) -> true | false.



 
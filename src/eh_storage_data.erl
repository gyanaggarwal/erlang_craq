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

-module(eh_storage_data).

-include("erlang_craq.hrl").

-callback header_byte_size() -> non_neg_integer().

-callback entry_to_binary(Entry :: #eh_storage_data{}) -> binary().

-callback binary_to_entry(Bin1 :: binary(), Bin2 :: binary()) -> {ok, #eh_storage_data{}} | ?EH_BAD_DATA.

-callback entry_header(Bin :: binary()) -> non_neg_integer().

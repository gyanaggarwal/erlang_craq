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

-module(erlang_craq_validate).

-export([data_view/1,
         check_data/1,
         get_data/3,
         missing_data/2]).

-include("erlang_craq.hrl").

missing_data(D1, D2) ->
  missing_data(D1, D2, []).

missing_data([H1 | R1], [H2, R2]=D2, Acc) ->
  case H1 =:= H2 of
    true  ->
      missing_data(R1, R2, Acc);
    false ->
      missing_data(R1, D2, [H1 | Acc])
  end;
missing_data(D1, [], Acc) ->
  Acc ++ D1;
missing_data([], [], Acc) ->
  Acc.

compare({N1, [H1 | R1]}, {N2, [H2 | R2]}, Acc) ->
  case H1 =:= H2 of
    true  ->
      compare({N1, R1}, {N2, R2}, Acc);
    false ->
      compare({N1, R1}, {N2, R2}, [{{N1, H1}, {N2, H2}} | Acc])
  end;
compare({_N1, []}, {_N2, []}, Acc) ->
  Acc.

check_data(NodeList) ->
  [R1 | Result] = data_view(NodeList),
  Issues = lists:foldl(fun(R2, Acc) -> compare(R1, R2, Acc) end, [], Result),
  case length(Issues) of
    0 ->
      valid;
    _ ->
     Issues
  end.

data_view(NodeList) ->
  {Replies, _} = gen_server:multi_call(NodeList, ?EH_SYSTEM_SERVER, ?EH_DATA_VIEW),
  Replies.

get_data(Node, ObjectType, ObjectId) ->
  gen_server:call({?EH_SYSTEM_SERVER, Node}, {?EH_GET_DATA, {ObjectType, ObjectId}}).



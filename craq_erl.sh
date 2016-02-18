#!/bin/bash

cd /Users/gyanendraaggarwal/erlang/code/erlang_craq

erl -sname $1 -pa ./ebin -pa ./craq_test/ebin -pa ./deps/lager/ebin -pa ./deps/goldrush/ebin -config ./sys

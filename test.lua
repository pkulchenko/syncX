require "testwell"

local sync9 = require "sync9"

-- testing inserts
local dag = sync9.createnode("0", {'X', '1'})
is(dag:getvalue(), "X1", "Initial node created.")
dag:addpatchset("20", {{1, 0, {'A', 'A'}}, {2, 0, {'B', 'B'}}})
is(dag:getvalue(), "XABBA1", "Patchset processed with two patches inserted at different positions.")
dag:addpatchset("30", {{3, 0, {'C', 'C'}}})
is(dag:getvalue(), "XABCCBA1", "Patch processed with 2 elements inserted at position 3.")
dag:addpatchset("40", {{4, 0, {'D'}}})
is(dag:getvalue(), "XABCDCBA1", "Patch processed with 2 elements inserted at position 4.")
dag:addpatchset("50", {{1, 0, {}}})
is(dag:getvalue(), "XABCDCBA1", "Patch processed with no deletes and no additions.")

-- testing deletes
dag = sync9.createnode("0", {'X', '1', '2', '3'})
is(dag:getvalue(), "X123", "Initial node created.")
dag:addpatchset("20", {{1, 2, {'A'}}, {2, 0, {'B'}}})
is(dag:getvalue(), "XAB3", "Patchset processed with two patches with elements added and deleted.")
dag:addpatchset("30", {{2, 2, {}}})
is(dag:getvalue(), "XA", "Patch processed with 2 elements deleted.")
dag:addpatchset("40", {{0, 2, {'C', 'C'}}})
is(dag:getvalue(), "CC", "Patch processed with 2 elements deleted and 2 added at position 0.")

-- testing get/set methods
dag = sync9.createnode("0", {'X', '1', '2', '3'})
is(dag:getvalue(), "X123", "Initial node created.")
dag:set(0, "0")
is(dag:getvalue(), "0123", "Set processed.")
is(dag:get(0), "0", "Get processed (1/2).")
is(dag:get(3), "3", "Get processed (2/2).")

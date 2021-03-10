require "testwell"

local sync9 = require "sync9"

-- testing additions
local dag = sync9.createnode("0", {'X', 'X'})
is(dag:getvalue(), "XX", "Initial node created.")
dag:addpatchset("20", {{1, 0, {'A', 'A'}}, {2, 0, {'B', 'B'}}})
is(dag:getvalue(), "XABBAX", "Patchset processed with two patches inserted at different positions.")
dag:addpatchset("30", {{3, 0, {'C', 'C'}}})
is(dag:getvalue(), "XABCCBAX", "Patch processed with 2 elements inserted at position 3.")
dag:addpatchset("40", {{4, 0, {'D'}}})
is(dag:getvalue(), "XABCDCBAX", "Patch processed with 2 elements inserted at position 4.")

-- testing deletes
dag = sync9.createnode("0", {'X', 'X', 'X', 'X'})
is(dag:getvalue(), "XXXX", "Initial node created.")
dag:addpatchset("20", {{1, 2, {'A', 'A'}}})
is(dag:getvalue(), "XAAX", "Patch processed with 2 elements added and deleted.")
dag:addpatchset("30", {{2, 2, {}}})
is(dag:getvalue(), "XA", "Patch processed with 2 elements deleted.")
dag:addpatchset("40", {{0, 2, {'B', 'B'}}})
is(dag:getvalue(), "BB", "Patch processed with 2 elements deleted and 2 added at position 0.")

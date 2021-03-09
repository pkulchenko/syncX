require "testwell"

local sync9 = require "sync9"
local dag = sync9.createnode("0", {'X', 'X'})
is(dag:getvalue(), "XX", "Initial node created.")
dag:addpatchset("20", {{1, 0, {'A', 'A'}}, {2, 0, {'B', 'B'}}})
is(dag:getvalue(), "XABBAX", "Patchset with two patches inserted at different positions.")
dag:addpatchset("30", {{3, 0, {'C', 'C'}}})
is(dag:getvalue(), "XABCCBAX", "Patch inserted at position 3.")
dag:addpatchset("50", {{4, 0, {'D'}}})
is(dag:getvalue(), "XABCDCBAX", "Patch inserted at position 4.")

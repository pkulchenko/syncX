require "testwell"

local sync9 = require "sync9"

-- testing get/set methods
dag = sync9.createnode("0", {'X', '1', '2', '3'})
is(dag:getvalue(), "X123", "Initial node created with expected value.")
is(dag:getlength(), 4, "Initial node created with expected length.")
dag:set(0, "0")
is(dag:getvalue(), "0123", "Set processed.")
is(dag:get(0), "0", "Get processed (1/2).")
is(dag:get(3), "3", "Get processed (2/2).")

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
dag:addpatchset("20", {{1, 2, {'A'}}, {2, 0, {'B', 'B'}}, {3, 1, {'C', 'D'}}})
is(dag:getvalue(), "XABCD3", "Patchset processed with three patches with elements added and deleted.")
dag:addpatchset("30", {{1, 4, {}}})
is(dag:getvalue(), "X3", "Patch processed with 4 elements deleted.")
dag:addpatchset("40", {{0, 2, {'C', 'C'}}})
is(dag:getvalue(), "CC", "Patch processed with 2 elements deleted and 2 added at position 0.")

-- testing embedded shallow processing
-- the content is managed as a string instead of a table
local shallowdata = setmetatable({[0] = "X123"}, {__index = {
      slice = function(tbl, ...) return {[0] = tbl[0]:sub(...)} end,
      getlength = function(tbl) return #tbl[0] end,
      getvalue = function(tbl, offset) return tbl[0] end,
    }})
dag = sync9.createnode("0", shallowdata)
is(dag:getvalue(), "X123", "Initial node created with shallow embedded data.")
dag:addpatchset("20", {{1, 2, {'A'}}, {2, 0, {'B', 'B'}}, {3, 1, {'C', 'D'}}})
is(dag:getvalue(), "XABCD3", "Patchset processed with three patches with elements added and deleted.")
dag:addpatchset("30", {{1, 4, {}}})
is(dag:getvalue(), "X3", "Patch processed with 4 elements deleted.")
dag:addpatchset("40", {{0, 2, {'C', 'C'}}})
is(dag:getvalue(), "CC", "Patch processed with 2 elements deleted and 2 added at position 0.")

-- testing external shallow processing
-- the content is managed as a string external to the graph
local str = "X123"
shallowdata = setmetatable({n = #str}, {__index = {
      slice = function(tbl, i, j) return {n = (j or tbl.n) - i + 1} end,
      getlength = function(tbl) return tbl.n end,
      getvalue = function(tbl, offset) return str:sub(offset + 1, offset + tbl.n) end,
    }})
dag = sync9.createnode("0", shallowdata)
-- set insert/delete handlers that are called when modifications are made
dag:sethandler{
  insert = function(node, version, offset, value)
    str = str:sub(1, offset)..table.concat(value,"")..str:sub(offset+1)
  end,
  delete = function(node, version, offset, length)
    str = str:sub(1, offset)..str:sub(offset+length+1)
  end,
}
is(dag:getvalue(), "X123", "Initial node created with shallow embedded data.")
dag:addpatchset("20", {{1, 2, {'A'}}, {2, 0, {'B', 'B'}}, {3, 1, {'C', 'D'}}})
is(dag:getvalue(), "XABCD3", "Patchset processed with three patches with elements added and deleted.")
dag:addpatchset("30", {{1, 4, {}}})
is(dag:getvalue(), "X3", "Patch processed with 4 elements deleted.")
dag:addpatchset("40", {{0, 2, {'C', 'C'}}})
is(dag:getvalue(), "CC", "Patch processed with 2 elements deleted and 2 added at position 0.")
is(str, dag:getvalue(), "Direct comparison of external shallow data.")

-- test handler independence
local updated
local dag1 = sync9.createnode("0", {'A'})
dag1:sethandler{
  insert = function(node, version, offset, value)
    updated = "Updated 1 with version "..version
  end,
}
local dag2 = sync9.createnode("0", {'A'})
dag2:sethandler{
  insert = function(node, version, offset, value)
    updated = "Updated 2 with version "..version
  end,
}

dag1:addpatchset("10", {{1, 0, {'B'}}})
is(updated, "Updated 1 with version 10", "Different root nodes have different handlers (1/2).")
dag2:addpatchset("20", {{1, 0, {'B'}}})
is(updated, "Updated 2 with version 20", "Different root nodes have different handlers (2/2).")

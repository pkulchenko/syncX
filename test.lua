require "testwell"

local sync9 = require "sync9"
local space

-- test get/set methods
space = sync9.createspace("0", {'X', '1', '2', '3'})
is(space:getvalue(), "X123", "Initial space created with expected value.")
is(space:getlength(), 4, "Initial space created with expected length.")
space:set(0, "0")
is(space:getvalue(), "0123", "Set processed.")
is(space:get(0), "0", "Get processed (1/2).")
is(space:get(3), "3", "Get processed (2/2).")

-- test inserts
space = sync9.createspace("0", {'X', '1'})
is(space:getvalue(), "X1", "Initial space created.")
space:addpatchset("20", {{1, 0, {'A', 'A'}}, {2, 0, {'B', 'B'}}})
is(space:getvalue(), "XABBA1", "Patchset processed with two patches inserted at different positions.")
space:addpatchset("30", {{3, 0, {'C', 'C'}}})
is(space:getvalue(), "XABCCBA1", "Patch processed with 2 elements inserted at position 3.")
space:addpatchset("40", {{4, 0, {'D'}}})
is(space:getvalue(), "XABCDCBA1", "Patch processed with 2 elements inserted at position 4.")
space:addpatchset("50", {{1, 0, {}}})
is(space:getvalue(), "XABCDCBA1", "Patch processed with no deletes and no additions.")

-- test deletes
space = sync9.createspace("0", {'X', '1', '2', '3'})
is(space:getvalue(), "X123", "Initial space created.")
space:addpatchset("20", {{1, 2, {'A'}}, {2, 0, {'B', 'B'}}, {3, 1, {'C', 'D'}}})
is(space:getvalue(), "XABCD3", "Patchset processed with three patches with elements added and deleted.")
space:addpatchset("30", {{1, 4, {}}})
is(space:getvalue(), "X3", "Patch processed with 4 elements deleted.")
space:addpatchset("40", {{0, 2, {'C', 'C'}}})
is(space:getvalue(), "CC", "Patch processed with 2 elements deleted and 2 added at position 0.")

-- test embedded shallow processing
-- the content is managed as a string instead of a table
local shallowdata = setmetatable({[0] = "X123"}, {__index = {
      slice = function(tbl, ...) return {[0] = tbl[0]:sub(...)} end,
      getlength = function(tbl) return #tbl[0] end,
      getvalue = function(tbl) return tbl[0] end,
    }})
space = sync9.createspace("0", shallowdata)
is(space:getvalue(), "X123", "Initial space created with shallow embedded data.")
space:addpatchset("20", {{1, 2, {'A'}}, {2, 0, {'B', 'B'}}, {3, 1, {'C', 'D'}}})
is(space:getvalue(), "XABCD3", "Patchset processed with three patches with elements added and deleted.")
space:addpatchset("30", {{1, 4, {}}})
is(space:getvalue(), "X3", "Patch processed with 4 elements deleted.")
space:addpatchset("40", {{0, 2, {'C', 'C'}}})
is(space:getvalue(), "CC", "Patch processed with 2 elements deleted and 2 added at position 0.")

-- test external shallow processing
-- the content is managed as a string external to the graph
local str = "X123"
shallowdata = setmetatable({n = #str}, {__index = {
      slice = function(tbl, i, j) return {n = (j or tbl.n) - i + 1} end,
      getlength = function(tbl) return tbl.n end,
      getvalue = function(tbl, offset) return str:sub(offset + 1, offset + tbl.n) end,
    }})
space = sync9.createspace("0", shallowdata)
-- set insert/delete handlers that are called when modifications are made
local callbacks = {}
space:sethandler{
  insert = function(version, offset, value)
    table.insert(callbacks, {"ins", version, offset, value})
    str = str:sub(1, offset)..table.concat(value,"")..str:sub(offset+1)
  end,
  delete = function(version, offset, length)
    table.insert(callbacks, {"del", version, offset, length})
    str = str:sub(1, offset)..str:sub(offset+length+1)
  end,
}
is(space:getvalue(), "X123", "Initial space created with shallow embedded data.")
space:addpatchset("20", {{1, 2, {'A'}}, {2, 0, {'B', 'B'}}, {3, 1, {'C', 'D'}}})
is(space:getvalue(), "XABCD3", "Patchset processed with three patches with elements added and deleted.")
space:addpatchset("30", {{1, 4, {}}})
is(space:getvalue(), "X3", "Patch processed with 4 elements deleted.")
is(callbacks[#callbacks][1], "del", "Patch with delete doesn't trigger insert callback.")
space:addpatchset("40", {{0, 2, {'C', 'C'}}})
is(space:getvalue(), "CC", "Patch processed with 2 elements deleted and 2 added at position 0.")
is(str, space:getvalue(), "Direct comparison of external shallow data.")

-- test handler independence
local updated
local space1 = sync9.createspace("0", {'A'})
space1:sethandler{
  insert = function(version)
    updated = "Updated 1 with version "..version
  end,
}
local space2 = sync9.createspace("0", {'A'})
space2:sethandler{
  insert = function(version)
    updated = "Updated 2 with version "..version
  end,
}
space1:addpatchset("10", {{1, 0, {'B'}}})
is(updated, "Updated 1 with version 10", "Different root nodes have different handlers (1/2).")
space2:addpatchset("20", {{1, 0, {'B'}}})
is(updated, "Updated 2 with version 20", "Different root nodes have different handlers (2/2).")

-- test parent comparisons
local resource = sync9.createresource()
local p1 = resource.futureparents:copy()
local p2 = resource.futureparents:copy()
ok(p1:equals(p2), "Empty tables are equal.")
p1.a = true
p1.b = true
ok(p1:equals(p1), "Table is equal to itself.")
p2.b = true
p2.a = true
ok(p1:equals(p2), "Tables with the same content are equal.")
p2.a = false
ok(not p1:equals(p2), "Tables with different content are not equal.")

-- test linear versioning for resources
resource = sync9.createresource()
resource:addversion("00", {{0, 0, {'X', '1'}}})
ok(resource:gettime("00"), "Resource version history is updated.")
is(resource:getvalue(), "X1", "Initial resource created.")
resource:addversion("20", {{1, 0, {'A', 'A'}}, {2, 0, {'B', 'B'}}})
is(resource:getvalue(), "XABBA1", "Resource patchset processed with two patches inserted at different positions.")
resource:addversion("30", {{3, 0, {'C', 'C'}}})
is(resource:getvalue(), "XABCCBA1", "Patch processed with 2 elements inserted at position 3.")
resource:addversion("40", {{4, 0, {'D'}}})
is(resource:getvalue(), "XABCDCBA1", "Patch processed with 2 elements inserted at position 4.")
resource:addversion("50", {{1, 0, {}}})
is(resource:getvalue(), "XABCDCBA1", "Patch processed with no deletes and no additions.")
ok(resource:gettime("50") and resource:gettime("50")["40"], "Resource version history is updated with the parent version.")

-- test branching versioning for resources
resource = sync9.createresource()
resource:addversion("v00", {{0, 0, {'X', '1'}}})
is(resource:getvalue(), "X1", "Initial resource created.")
resource:addversion("v20", {{1, 0, {'A', 'A'}}}, {v00 = true})
is(resource:getvalue(), "XAA1", "Resource patch processed with explicit parent.")
resource:addversion("v10", {{1, 0, {'B'}}}, {v00 = true})
is(resource:getvalue(), "XBAA1", "Resource patch processed with a branching parent inserted earlier based on version number.")
resource:addversion("v30", {{3, 0, {'C'}}}, {v10 = true, v20 = true})
is(resource:getvalue(), "XBACA1", "Resource patch processed with a branching parent inserted later based on version number.")
resource:addversion("v40", {{1, 4}}, {v30 = true})
is(resource:getvalue(), "X1", "Resource patch processed with a delete.")
is(resource:getvalue("v00"), "X1", "Resource returns value for specific version (1/5).")
is(resource:getvalue("v10"), "XB1", "Resource returns value for specific version (3/5).")
is(resource:getvalue("v20"), "XAA1", "Resource returns value for specific version (2/5).")
is(resource:getvalue("v30"), "XBACA1", "Resource returns value for specific version (4/5).")
is(resource:getvalue("v40"), "X1", "Resource returns value for specific version (5/5).")
ok(resource:gettime("v30") and resource:gettime("v30").v10 and resource:gettime("v30").v20,
  "Resource version history is merged with multiple parent versions.")

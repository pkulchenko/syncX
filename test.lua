require "testwell"

local sync9 = require "sync9"
local resource

-- test get/set methods
resource = sync9.createspace("0", {'X', '1', '2', '3'})
is(resource:getvalue(), "X123", "Space created with expected value.")
is(resource:getlength(), 4, "Space created with expected length.")
resource:set(0, "0")
is(resource:getvalue(), "0123", "Set processed.")
is(resource:get(0), "0", "Get processed (1/2).")
is(resource:get(3), "3", "Get processed (2/2).")

-- test inserts
resource = sync9.createspace("0", {'X.Y', '1.2'})
is(resource:getvalue(), "X.Y1.2", "Space created.")
is(resource:getlength(), 2, "Space created with expected length.")
resource:addpatchset("20", {{1, 0, {'A', 'A'}}, {2, 0, {'B', 'B'}}})
is(resource:getvalue(), "X.YABBA1.2", "Patchset processed with two patches inserted at different positions.")
resource:addpatchset("30", {{3, 0, {'C', 'C'}}})
is(resource:getvalue(), "X.YABCCBA1.2", "Patch processed with 2 elements inserted at position 3.")
resource:addpatchset("40", {{4, 0, {'D'}}})
is(resource:getvalue(), "X.YABCDCBA1.2", "Patch processed with 2 elements inserted at position 4.")
resource:addpatchset("50", {{1, 0, {}}})
is(resource:getvalue(), "X.YABCDCBA1.2", "Patch processed with no deletes and no additions.")

-- test deletes
resource = sync9.createspace("0", {'X', '1', '2', '3'})
is(resource:getvalue(), "X123", "Space created.")
resource:addpatchset("20", {{1, 2, {'A'}}, {2, 0, {'B', 'B'}}, {3, 1, {'C', 'D'}}})
is(resource:getvalue(), "XABCD3", "Patchset processed with three patches with elements added and deleted.")
resource:addpatchset("30", {{1, 1}})
resource:addpatchset("31", {{1, 2, {}}})
resource:addpatchset("32", {{1, 1, {''}}})
is(resource:getvalue(), "X3", "Patch processed with 4 elements deleted.")
resource:addpatchset("40", {{0, 2, {'C', 'C'}}})
is(resource:getvalue(), "CC", "Patch processed with 2 elements deleted and 2 added at position 0.")
resource:addpatchset("50", {{1, 1, {'D'}}})
is(resource:getvalue(), "CD", "Patch processed with 1 element deleted and added at the last element.")

-- test embedded shallow processing
-- the content is managed as a string instead of a table
local shallowdata = setmetatable({[0] = "X123"}, {__index = {
      slice = function(tbl, ...) return {[0] = tbl[0]:sub(...)} end,
      getlength = function(tbl) return #tbl[0] end,
      getvalue = function(tbl) return tbl[0] end,
    }})
resource = sync9.createspace("0", shallowdata)
is(resource:getvalue(), "X123", "Space created with shallow embedded data.")
resource:addpatchset("20", {{1, 2, {'A'}}, {2, 0, {'B', 'B'}}, {3, 1, {'C', 'D'}}})
is(resource:getvalue(), "XABCD3", "Patchset processed with three patches with elements added and deleted.")
resource:addpatchset("30", {{1, 4, {}}})
is(resource:getvalue(), "X3", "Patch processed with 4 elements deleted.")
resource:addpatchset("40", {{0, 2, {'C', 'C'}}})
is(resource:getvalue(), "CC", "Patch processed with 2 elements deleted and 2 added at position 0.")

-- test external shallow processing
-- the content is managed as a string external to the graph
local str = "X123"
shallowdata = setmetatable({n = #str}, {__index = {
      slice = function(tbl, i, j) return {n = (j or tbl.n) - i + 1} end,
      getlength = function(tbl) return tbl.n end,
      getvalue = function(tbl, offset) return str:sub(offset + 1, offset + tbl.n) end,
    }})
resource = sync9.createresource("0", shallowdata)
-- set version handler that is called when modifications are made
local callbacks = {}
resource:sethandler{
  version = function(resource, version)
    for _, patch in ipairs(resource:getpatchset(version, {[version] = true})) do
      local addidx, delcnt, value = (table.unpack or unpack)(patch)
      if delcnt > 0 then
        table.insert(callbacks, {"del", version, addidx, delcnt})
        str = str:sub(1, addidx)..str:sub(addidx+delcnt+1)
      end
      if value and #value > 0 then
        table.insert(callbacks, {"ins", version, addidx, value})
        str = str:sub(1, addidx)..table.concat(value,"")..str:sub(addidx+1)
      end
    end
  end,
}
is(resource:getvalue(), "X123", "Space created with shallow embedded data.")
resource:addversion("20", {{1, 2, {'A'}}, {2, 0, {'B', 'B'}}, {3, 1, {'C', 'D'}}})
is(resource:getvalue(), "XABCD3", "Patchset processed with three patches with elements added and deleted.")
resource:addversion("30", {{1, 4, {}}})
is(resource:getvalue(), "X3", "Patch processed with 4 elements deleted.")
-- check that the last and the previous patches are deletes (no inserts added for the last patch)
is(#callbacks > 1 and callbacks[#callbacks-1][1] and callbacks[#callbacks][1], "del", "Patch with delete doesn't trigger insert callback.")
resource:addversion("40", {{0, 2, {'C', 'C'}}})
is(resource:getvalue(), "CC", "Patch processed with 2 elements deleted and 2 added at position 0.")
is(str, resource:getvalue(), "Direct comparison of external shallow data.")

-- test handler independence
local updated
local resource1 = sync9.createresource("0", {'A'})
resource1:sethandler{
  version = function(_, version)
    updated = "Updated 1 with version "..version
  end,
}
local resource2 = sync9.createresource("0", {'A'})
resource2:sethandler{
  version = function(_, version)
    updated = "Updated 2 with version "..version
  end,
}
resource1:addversion("10", {{1, 0, {'B'}}})
is(updated, "Updated 1 with version 10", "Different root nodes have different handlers (1/2).")
resource2:addversion("20", {{1, 0, {'B'}}})
is(updated, "Updated 2 with version 20", "Different root nodes have different handlers (2/2).")

-- test parent comparisons
resource = sync9.createresource()
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
is(resource:getvalue(), "X1", "Resource created.")
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
is(resource:getvalue(), "X1", "Resource created.")
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
ok(resource:getparents("v30").v10 and resource:getparents("v30").v20,
  "Resource version history is merged with multiple parent versions.")

-- test ancestor handling
local ancestors = resource:getancestors({v30 = true})
ok(ancestors.v10 and ancestors.v20 and ancestors.v30 and ancestors.v00, "Ancestor by version returns all ancestors (1/2).")
ancestors = resource:getancestors({v10 = true})
ok(ancestors.v10 and ancestors.v00, "Ancestor by version returns all ancestors (2/2).")

-- test direct resource initialization
resource = sync9.createresource("v00", {'X', '1'})
is(resource:getvalue(), "X1", "Resource created with initialization value.")

-- test branching addition and deletion
resource:addversion("v20", {{1, 0, {'A', 'A'}}}, {v00 = true})
is(resource:getvalue(), "XAA1", "Resource patch processed with explicit parent insert first (1/4).")
resource:addversion("v10", {{0, 2}}, {v00 = true})
is(resource:getvalue("v10"), "", "Resource patch processed with explicit parent insert first (2/4).")
is(resource:getvalue("v20"), "XAA1", "Resource patch processed with explicit parent insert first (3/4).")
is(resource:getvalue(), "AA", "Resource patch processed with explicit parent insert first (4/4).")

-- test branching deletion and addition
resource = sync9.createresource("v00", {'X', '1'})
callbacks = {}
resource:sethandler{
  version = function(_, version) table.insert(callbacks, version) end,
}
is(resource:getvalue(), "X1", "Resource created with initialization value.")
resource:addversion("v10", {{0, 2}}, {v00 = true})
is(resource:getvalue("v10"), "", "Resource patch processed with explicit parent delete first (1/4).")
resource:addversion("v20", {{1, 0, {'A', 'A'}}}, {v00 = true})
is(resource:getvalue("v20"), "XAA1", "Resource patch processed with explicit parent delete first (2/4).")
is(resource:getvalue("v10"), "", "Resource patch processed with explicit parent delete first (3/4).")
is(resource:getvalue(), "AA", "Resource patch processed with explicit parent delete first (4/4).")

-- test patchsets generated "as of" their own version (should be the same as the original patchset)
is(resource:getpatchset("v00", {v00 = true}), {{0, 0, {'X'}}, {1, 0, {'1'}}},
  "Resource patchset for its own version has expected patches (1/3).")
is(resource:getpatchset("v10", {v10 = true}), {{0, 1}, {0, 1}},
  "Resource patchset for its own version has expected patches (2/3).")
is(resource:getpatchset("v20", {v20 = true}), {{1, 0, {'A', 'A'}}},
  "Resource patchset for its own version has expected patches (3/3).")

is(callbacks[1], "v10", "Resource callback reports expected version (1/2).")
is(callbacks[2], "v20", "Resource callback reports expected version (2/2).")

-- test patchsets generated "as of" the current version ("rebase" the patchset)
is(resource:getpatchset("v00"), {{0, 0, {'X'}}, {2, 0, {'1'}}},
  "Resource patchset for the current version has expected patches (1/3).")
is(resource:getpatchset("v10"), {{0, 1}, {2, 1}}, "Resource patchset for the current version has expected patches (2/3).")
is(resource:getpatchset("v20"), {{0, 0, {'A', 'A'}}}, "Resource patchset for the current version has expected patches (3/3).")

-- test branching replacement and overlapping deletion
resource = sync9.createresource("v00", {'X', '1', '2', '3', '4', '5', '6', '7'})

resource:addversion("v10", {{2, 2, {'A'}}}, {v00 = true})
is(resource:getvalue("v10"), "X1A4567", "Resource patch processed with overlapping deletes (1/4).")
resource:addversion("v20", {{1, 6}}, {v00 = true})
is(resource:getvalue("v20"), "X7", "Resource patch processed with overlapping deletes (2/4).")
is(resource:getvalue("v10"), "X1A4567", "Resource patch processed with overlapping deletes (3/4).")
is(resource:getvalue(), "XA7", "Resource patch processed with overlapping deletes (4/4).")

-- test patchsets generated "as of" the current version ("rebase" the patchset)
is(resource:getpatchset("v10"), {{1, 0, {'A'}}}, "Resource patchset for the current version has expected patches (1/2).")
is(resource:getpatchset("v20"), {{1, 1}, {2, 3}}, "Resource patchset for the current version has expected patches (2/2).")

callbacks = {}
resource:walkgraph(function(args)
    table.insert(callbacks, {args.version, args.value, args.level, args.offset, args.isdeleted, args.isnode})
  end)
is(callbacks, {
    {"v00", "X", 1, 0, false, true},
    {"v00", "1", 1, 1, true, false},
    {"v10", "A", 2, 1, false, true},
    {"v00", "23", 1, 2, true, false},
    {"v00", "456", 1, 2, true, false},
    {"v00", "7", 1, 2, false, false},
    }, "Calling walkgraph produces expected callbacks.")

-- test linear versioning for string-based resources
resource = sync9.createresource("v00", "")
resource:addversion("v10", {{0, 0, 'X1'}})
is(resource:getvalue(), "X1", "Resource created.")
is(resource:getpatchset("v10", {v10 = true}), {{0, 0, 'X1'}},
  "Resource patchset for its own version has expected patches.")
resource:addversion("v20", {{1, 0, 'AA'}, {2, 0, 'BB'}})
is(resource:getvalue(), "XABBA1", "Resource patchset processed with two patches inserted at different positions.")
resource:addversion("v30", {{3, 0, 'CC'}})
is(resource:getvalue(), "XABCCBA1", "Patch processed with 2 elements inserted at position 3.")
resource:addversion("v40", {{4, 0, 'D'}})
is(resource:getvalue(), "XABCDCBA1", "Patch processed with 2 elements inserted at position 4.")
resource:addversion("v50", {{1, 0, ""}})
is(resource:getvalue(), "XABCDCBA1", "Patch processed with no deletes and no additions.")

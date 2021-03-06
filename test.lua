require "testwell"

local sync9 = require "sync9"
local resource
local unpack = table.unpack or unpack

-- test inserts
resource = sync9.createspace("0", {'X.Y', '1.2'})
is(resource:getvalue(), {'X.Y', '1.2'}, "Space created.")
is(resource:getlength(), 2, "Space created with expected length.")
resource:addpatchset("20", {{1, 0, {'A', 'A'}}, {2, 0, {'B', 'B'}}})
is(resource:getvalue(), {'X.Y', 'A', 'B', 'B', 'A', '1.2'}, "Patchset processed with two patches inserted at different positions.")
resource:addpatchset("30", {{3, 0, {'C', 'C'}}})
is(resource:getvalue(), {'X.Y', 'A', 'B', 'C', 'C', 'B', 'A', '1.2'}, "Patch processed with 2 elements inserted at position 3.")
resource:addpatchset("40", {{4, 0, {'D'}}})
is(resource:getvalue(), {'X.Y', 'A', 'B', 'C', 'D', 'C', 'B', 'A', '1.2'}, "Patch processed with 2 elements inserted at position 4.")
resource:addpatchset("50", {{1, 0, {}}})
is(resource:getvalue(), {'X.Y', 'A', 'B', 'C', 'D', 'C', 'B', 'A', '1.2'}, "Patch processed with no deletes and no additions.")

-- test deletes
resource = sync9.createspace("0", {'X', '1', '2', '3'})
is(resource:getvalue(), {'X', '1', '2', '3'}, "Space created.")
resource:addpatchset("20", {{1, 2, {'A'}}, {2, 0, {'B', 'B'}}, {3, 1, {'C', 'D'}}})
is(resource:getvalue(), {'X', 'A', 'B', 'C', 'D', '3'}, "Patchset processed with three patches with elements added and deleted.")
resource:addpatchset("30", {{1, 1}})
resource:addpatchset("31", {{1, 2, {}}})
resource:addpatchset("32", {{1, 1, {''}}})
is(resource:getvalue(), {'X', '3'}, "Patch processed with 4 elements deleted.")
resource:addpatchset("40", {{0, 2, {'C', 'C'}}})
is(resource:getvalue(), {'C', 'C'}, "Patch processed with 2 elements deleted and 2 added at position 0.")
resource:addpatchset("50", {{1, 1, {'D'}}})
is(resource:getvalue(), {'C', 'D'}, "Patch processed with 1 element deleted and added at the last element.")

-- test embedded shallow processing
-- the content is managed as a string instead of a table
local shallowdata = setmetatable({[0] = "X123"}, {__index = {
      slice = function(tbl, ...) return {[0] = tbl[0]:sub(...)} end,
      getlength = function(tbl) return #tbl[0] end,
      getvalue = function(tbl) return tbl[0] end,
    }})
resource = sync9.createspace("0", shallowdata)
is(resource:getvalue(), "X123", "Space created with shallow embedded data.")
resource:addpatchset("20", {{1, 2, 'A'}, {2, 0, 'BB'}, {3, 1, 'CD'}})
is(resource:getvalue(), "XABCD3", "Patchset processed with three patches with elements added and deleted.")
resource:addpatchset("30", {{1, 4, {}}})
is(resource:getvalue(), "X3", "Patch processed with 4 elements deleted.")
resource:addpatchset("40", {{0, 2, 'CC'}})
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
      local addidx, delcnt, value = unpack(patch)
      if delcnt > 0 then
        table.insert(callbacks, {"del", version, addidx, delcnt})
        str = str:sub(1, addidx)..str:sub(addidx+delcnt+1)
      end
      if value and #value > 0 then
        table.insert(callbacks, {"ins", version, addidx, value})
        str = str:sub(1, addidx)..value..str:sub(addidx+1)
      end
    end
  end,
}
is(resource:getvalue(), "X123", "Space created with shallow embedded data.")
resource:addversion("20", {{1, 2, 'A'}, {2, 0, 'BB'}, {3, 1, 'CD'}})
is(resource:getvalue(), "XABCD3", "Patchset processed with three patches with elements added and deleted.")
resource:addversion("30", {{1, 4, ""}})
is(resource:getvalue(), "X3", "Patch processed with 4 elements deleted.")
-- check that the last and the previous patches are deletes (no inserts added for the last patch)
is(#callbacks > 1 and callbacks[#callbacks-1][1] and callbacks[#callbacks][1], "del", "Patch with delete doesn't trigger insert callback.")
resource:addversion("40", {{0, 2, 'CC'}})
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
resource:addversion("00", {{0, 0, 'X1'}})
ok(resource:gettime("00"), "Resource version history is updated.")
is(resource:getvalue(), "X1", "Resource created.")
resource:addversion("20", {{1, 0, 'AA'}, {2, 0, 'BB'}})
is(resource:getvalue(), "XABBA1", "Resource patchset processed with two patches inserted at different positions.")
resource:addversion("30", {{3, 0, 'CC'}})
is(resource:getvalue(), "XABCCBA1", "Patch processed with 2 elements inserted at position 3.")
resource:addversion("40", {{4, 0, 'D'}})
is(resource:getvalue(), "XABCDCBA1", "Patch processed with 2 elements inserted at position 4.")
resource:addversion("50", {{1, 0, ""}})
is(resource:getvalue(), "XABCDCBA1", "Patch processed with no deletes and no additions.")
ok(resource:gettime("50") and resource:gettime("50")["40"], "Resource version history is updated with the parent version.")

-- test branching versioning for resources
resource = sync9.createresource()
resource:addversion("v00", {{0, 0, 'X1'}})
is(resource:getvalue(), "X1", "Resource created.")
resource:addversion("v20", {{1, 0, 'AA'}}, {v00 = true})
is(resource:getvalue(), "XAA1", "Resource patch processed with explicit parent.")
resource:addversion("v10", {{1, 0, 'B'}}, {v00 = true})
is(resource:getvalue(), "XBAA1", "Resource patch processed with a branching parent inserted earlier based on version number.")
resource:addversion("v30", {{3, 0, 'C'}}, {v10 = true, v20 = true})
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
resource = sync9.createresource("v00", 'X1')
is(resource:getvalue(), "X1", "Resource created with initialization value.")

-- test branching addition and deletion
resource:addversion("v20", {{1, 0, 'AA'}}, {v00 = true})
is(resource:getvalue(), "XAA1", "Resource patch processed with explicit parent insert first (1/4).")
resource:addversion("v10", {{0, 2}}, {v00 = true})
is(resource:getvalue("v10"), "", "Resource patch processed with explicit parent insert first (2/4).")
is(resource:getvalue("v20"), "XAA1", "Resource patch processed with explicit parent insert first (3/4).")
is(resource:getvalue(), "AA", "Resource patch processed with explicit parent insert first (4/4).")

-- test branching deletion and addition
resource = sync9.createresource("v00", "X1")
callbacks = {}
resource:sethandler{
  version = function(_, version) table.insert(callbacks, version) end,
}
is(resource:getvalue(), "X1", "Resource created with initialization value.")
resource:addversion("v10", {{0, 2}}, {v00 = true})
is(resource:getvalue("v10"), "", "Resource patch processed with explicit parent delete first (1/4).")
resource:addversion("v20", {{1, 0, "AA"}}, {v00 = true})
is(resource:getvalue("v20"), "XAA1", "Resource patch processed with explicit parent delete first (2/4).")
is(resource:getvalue("v10"), "", "Resource patch processed with explicit parent delete first (3/4).")
is(resource:getvalue(), "AA", "Resource patch processed with explicit parent delete first (4/4).")

-- test patchsets generated "as of" their own version (should be the same as the original patchset)
is(resource:getpatchset("v00", {v00 = true}), {{0, 0, 'X'}, {1, 0, '1'}},
  "Resource patchset for its own version has expected patches (1/3).")
is(resource:getpatchset("v10", {v10 = true}), {{0, 1}, {0, 1}},
  "Resource patchset for its own version has expected patches (2/3).")
is(resource:getpatchset("v20", {v20 = true}), {{1, 0, "AA"}},
  "Resource patchset for its own version has expected patches (3/3).")

is(callbacks[1], "v10", "Resource callback reports expected version (1/2).")
is(callbacks[2], "v20", "Resource callback reports expected version (2/2).")

-- test patchsets generated "as of" the current version ("rebase" the patchset)
is(resource:getpatchset("v00"), {{0, 0, 'X'}, {2, 0, '1'}},
  "Resource patchset for the current version has expected patches (1/3).")
is(resource:getpatchset("v10"), {{0, 1}, {2, 1}}, "Resource patchset for the current version has expected patches (2/3).")
is(resource:getpatchset("v20"), {{0, 0, "AA"}}, "Resource patchset for the current version has expected patches (3/3).")

-- test branching replacement and overlapping deletion
resource = sync9.createresource("v00", "X1234567")

resource:addversion("v10", {{2, 2, "A"}}, {v00 = true})
is(resource:getvalue("v10"), "X1A4567", "Resource patch processed with overlapping deletes (1/4).")
resource:addversion("v20", {{1, 6}}, {v00 = true})
is(resource:getvalue("v20"), "X7", "Resource patch processed with overlapping deletes (2/4).")
is(resource:getvalue("v10"), "X1A4567", "Resource patch processed with overlapping deletes (3/4).")
is(resource:getvalue(), "XA7", "Resource patch processed with overlapping deletes (4/4).")

-- test patchsets generated "as of" the current version ("rebase" the patchset)
is(resource:getpatchset("v10"), {{1, 0, 'A'}}, "Resource patchset for the current version has expected patches (1/2).")
is(resource:getpatchset("v20"), {{1, 1}, {2, 3}}, "Resource patchset for the current version has expected patches (2/2).")

-- merge the two branches together
resource:addversion("v30", {{0, 0, ""}}, {v20 = true, v10 = true})

callbacks = {}
resource:walkgraph(function(args)
    table.insert(callbacks, {args.version, args.value, args.level, args.offset, args.isdeleted, args.isnode})
  end)
is(callbacks, {
    {"v00", "", 1, 0, true, true},
    {"v30", "", 2, 0, false, true},
    {"v00", "X", 1, 0, false, false},
    {"v00", "1", 1, 1, true, false},
    {"v10", "A", 2, 1, false, true},
    {"v00", "23", 1, 2, true, false},
    {"v00", "456", 1, 2, true, false},
    {"v00", "7", 1, 2, false, false},
    }, "Calling walkgraph produces expected callbacks.")

resource:prune()
is(resource:getvalue(), "XA7", "Node value stays the same after pruning.")

callbacks = {}
resource:walkgraph(function(args)
    table.insert(callbacks, {args.version, args.value, args.level, args.offset, args.isdeleted, args.isnode})
  end)
is(callbacks, {{"v30", "XA7", 1, 0, false, true}}, "Pruning leaves only one node.")

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

-- test pruning of selected versions
-- prune everything between v00 and v40
resource:prune({v00 = true}, {v40 = true})
is(resource.time.v00, {}, "Prune selected versions (1/4).")
is(resource.time.v40, {v00 = true}, "Prune selected versions (2/4).")
is(resource.time.v50, {v40 = true}, "Prune selected versions (3/4).")
is(resource:getvalue(), "XABCDCBA1", "Prune selected versions (4/4).")

-- test pruning of unrelated versions
resource = sync9.createresource("v00", "")
resource:addversion("v10", {{0, 0, 'X'}})
-- add a patch with a non-existing parent version
resource:addversion("v20", {{0, 0, '1'}}, {vXX = true})
resource:addversion("v30", {{0, 0, ''}}, {v20 = true, v10 = true})
is(resource:getvalue(), "X1", "Prune unrelated versions (1/4).")
resource:prune()
is(resource:getvalue(), "X1", "Prune unrelated versions (2/4).")
-- check that v30 is now the root version
is(resource.time.v30, {}, "Prune unrelated versions (3/4).")
-- check that v30 is the only version in the time graph
is({next(resource.time)}, {"v30", {}}, "Prune unrelated versions (4/4).")

-- test pruning of unrelated versions without local merge
resource = sync9.createresource("v00", "X")
-- add a patch with a non-existing parent version
resource:addversion("v20", {{1, 0, '1'}}, {vXX = true})
-- prune it
resource:prune()
-- add another patch that refers to the earlier version
resource:addversion("v30", {{2, 0, '2'}}, {v20 = true})
is(resource:getvalue(), "X12", "Prune unrelated versions without local merge (1/4).")
resource:prune()
is(resource:getvalue(), "X12", "Prune unrelated versions without local merge (2/4).")
-- check that v30 is now the root version
is(resource.time.v30, {}, "Prune unrelated versions without local merge (3/4).")
-- check that v30 is the only version in the time graph
is({next(resource.time)}, {"v30", {}}, "Prune unrelated versions without local merge (4/4).")

-- test cases from [CRDT puzzles page](https://braid.org/crdt/puzzles) as of 2021-May-5
local combinations = {
  {"a10", "b10", "helloworld"},
  {"b10", "a10", "worldhello"},
}
for i, params in ipairs(combinations) do
  local v1, v2, result = unpack(params)
  resource = sync9.createresource("v00", "")
  resource:addversion(v1, {{0, 0, 'hello'}}, {v00 = true})
  resource:addversion(v2, {{0, 0, 'world'}}, {v00 = true})
  resource:addversion("c10", {{0, 0, ''}}, {[v1] = true, [v2] = true})
  -- `helloworld` based on comparison between a10 and b10
  is(resource:getvalue(), result, ("CRDT puzzles test 1 (%d/%d)."):format(i, #combinations))
end

combinations = {
  {"a10", "b10", "AAABBB"},
  {"b10", "a10", "BBBAAA"},
}
for i, params in ipairs(combinations) do
  local v1, v2, result = unpack(params)
  resource = sync9.createresource("v00", "")
  -- A prepends AAA (one character at a time)
  resource:addversion(v1, {{0, 0, 'A'}}, {v00 = true})
  resource:addversion("a20", {{0, 0, 'A'}}, {[v1] = true})
  resource:addversion("a30", {{0, 0, 'A'}}, {a20 = true})
  -- B prepends BBB (one character at a time)
  resource:addversion(v2, {{0, 0, 'B'}}, {v00 = true})
  resource:addversion("b20", {{0, 0, 'B'}}, {[v2] = true})
  resource:addversion("b30", {{0, 0, 'B'}}, {b20 = true})
  -- versions from A and B are merged together
  resource:addversion("c10", {{0, 0, ''}}, {a30 = true, b30 = true})
  -- `AAABBB` based on comparison between a10 and b10
  is(resource:getvalue(), result, ("CRDT puzzles test 2 (%d/%d)."):format(i, #combinations))
end

combinations = {
  {"a10", "b10", "ACB", "Aa1Cb0B"},
  {"b10", "a10", "BCA", "b0BCAa1"},
}
for i, params in ipairs(combinations) do
  local v1, v2, result1, result2 = unpack(params)
  resource = sync9.createresource("v00", "")
  -- A and B insert "A" and "B" respectively
  resource:addversion(v1, {{0, 0, 'A'}}, {v00 = true})
  resource:addversion(v2, {{0, 0, 'B'}}, {v00 = true})
  -- C merges changes from A and B, and inserts C in between their edits
  resource:addversion("c10", {{1, 0, 'C'}}, {[v1] = true, [v2] = true})
  -- `ACB` depending on the order of a10 anc b10 (could also be `BCA`)
  is(resource:getvalue(), result1, ("CRDT puzzles test 3 (%d/%d)."):format(i*2-1, #combinations*2))
  -- A inserts "a1"
  resource:addversion("a20", {{1, 0, 'a1'}}, {[v1] = true})
  -- B prepends "b0"
  resource:addversion("b20", {{0, 0, 'b0'}}, {[v2] = true})
  -- C merges all the changes piecemeal; although "c10" is merged twice,
  -- it is still only included once in the result
  resource:addversion("c20", {{0, 0, ''}}, {a20 = true, c10 = true})
  resource:addversion("c30", {{0, 0, ''}}, {b20 = true, c10 = true})
  resource:addversion("c40", {{0, 0, ''}}, {c20 = true, c30 = true})
  is(resource:getvalue(), result2, ("CRDT puzzles test 3 (%d/%d)."):format(i*2, #combinations*2))
end

combinations = {
  {"10", "20", "30", "ADBC"},
  {"10", "30", "20", "ADCB"},
  {"20", "30", "10", "CDAB"},
  {"20", "10", "30", "BADC"},
  {"30", "10", "20", "BCDA"},
  {"30", "20", "10", "CDBA"},
}
for i, params in ipairs(combinations) do
  local v1, v2, v3, result = unpack(params)
  resource = sync9.createresource("v00", "")
  -- A, B, C are inserted concurrently into an empty document
  resource:addversion(v1, {{0, 0, 'A'}}, {v00 = true})
  resource:addversion(v2, {{0, 0, 'B'}}, {v00 = true})
  resource:addversion(v3, {{0, 0, 'C'}}, {v00 = true})
  -- D merges changes from A and C and inserts D between
  resource:addversion("d10", {{1, 0, 'D'}}, {[v1] = true, [v3] = true})
  -- B merges all the changes
  resource:addversion("b20", {{0, 0, ''}}, {[v2] = true, d10 = true})
  -- `ADBC` based on comparison between a10, b10 and c10
  is(resource:getvalue(), result, ("CRDT puzzles test 4 (%d/%d)."):format(i, #combinations))
end

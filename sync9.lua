local function argcheck(cond, i, f, extra)
  if not cond then
    error("bad argument #"..i.." to '"..f.."' ("..extra..")", 0)
  end
end

local function splice(tbl, startidx, delcnt, ...)
  local inscnt = select('#', ...)
  local tblcnt = #tbl
  argcheck(startidx >= 1, 2, "splice", "initial position must be positive")
  argcheck(startidx <= tblcnt+1, 2, "splice",
    "initial position must not exceed table size by more than 1")
  argcheck(delcnt >= 0, 2, "splice", "delete count must be non-negative")

  -- remove excess (if any)
  for _ = 1, delcnt-inscnt do table.remove(tbl, startidx) end
  -- insert if needed
  for i = 1, inscnt-delcnt do table.insert(tbl, startidx+i-1, (select(i, ...))) end
  -- assign the rest, as there is enough space
  for i = math.max(1, inscnt-delcnt+1), inscnt do tbl[startidx+i-1] = select(i, ...) end
end

-- modified from https://stackoverflow.com/questions/22697936/binary-search-in-javascript
local function binarySearch(tbl, comparator)
  local m = 1
  local n = #tbl
  while (m <= n) do
    local k = math.floor((n + m) / 2)
    local cmp = comparator(tbl[k])
    if (cmp > 0) then
      m = k + 1
    elseif (cmp < 0) then
      n = k - 1
    else
      return k
    end
  end
  return m
end

local function spliceinto(tbl, part)
  local i = binarySearch(tbl, function (x) return part.version < x.version and -1 or 1 end)
  tbl:splice(i, 0, part)
  return i
end

local function any(tbl, condition)
  for k, v in pairs(tbl) do
    if condition(k,v) then return true end
  end
  return false
end

if not table.unpack then table.unpack = unpack end

-- traverse spaceDAG starting from node and calling `callback` for each part
local function traverse_space_dag(node, isanc, callback)
  local offset = 0
  local function helper(node, version, prev)
    local deleted = node.deletedby:any(function(version) return isanc(version) end)
    -- callback may return `false` to indicate that traversal needs to be stopped
    if callback(node, version, prev, offset, deleted) == false then
      return false end
    if not deleted then offset = offset + #node.elems end
    for _, part in ipairs(node.parts) do
      if isanc(part.version) and helper(part, part.version) == false then return false end
    end
    if node.parts[0] and helper(node.parts[0], version, node) == false then return false end
  end
  return helper(node, node.version)
end

local metaparts = {__index = {
    splice = splice,
    slice = function(...) return {table.unpack(...)} end,
    spliceinto = spliceinto, -- insert in the appropriate slot based on binary search by version
    any = any, -- return `true` if any of the values match condition
    copy = function(tbl) return {table.unpack(tbl)} end,
  }}
local metadags = {__index = {
    getlength = function(node, isanc)
      isanc = isanc or function() return true end  
      local count = 0
      traverse_space_dag(node, isanc, function(node) count = count + node.elems.length end)
      return count
    end,
    getvalue = function(node, isanc)
      isanc = isanc or function() return true end  
      local values = {}
      traverse_space_dag(node, isanc, function(node) table.insert(values, table.concat(node.elems, "")) end)
      return table.concat(values)
    end,
  }}

local function create_space_dag_node(version, elems, deletedby)
  assert(not elems or type(elems) == "table")
  assert(not deletedby or type(deletedby) == "table")
  return setmetatable({
    version = version, -- node version as a string
    elems = setmetatable(elems or {}, metaparts), -- list of elements this node stores
    deletedby = setmetatable(deletedby or {}, metaparts), -- hash of versions this node is deleted by
    parts = setmetatable({}, metaparts), -- list of nodes that are children of this one
    -- parts[0] is a special non-versioned node that has been spliced from the elements of the curent node
  }, metadags)
end

local function space_dag_break_node(node, splitidx, newpart)
  local tail = create_space_dag_node(nil, node.elems:slice(splitidx), node.deletedby:copy())
  tail.parts = node.parts

  node.elems = setmetatable(node.elems:slice(1, splitidx-1), getmetatable(node.elems))
  node.parts = setmetatable(newpart and {newpart} or {}, getmetatable(tail.parts))
  node.parts[0] = tail
  return tail
end

-- add a patchset to a node, which will have `nodeversion` after patching
local function space_dag_add_patchset(node, nodeversion, patches, isanc)
  isanc = isanc or function() return true end
  local pidx = 1
  local function process_patch(node, patchversion, prev, offset, deleted)
    if pidx > #patches then return false end -- nothing to process further
    local addidx, delcnt, val = table.unpack(patches[pidx])
    local hasparts = node.parts:any(function(_, part) return isanc(part.version) end)

    -- this element is already deleted
    if deleted then
      -- this patch only adds elements at the current offset
      if delcnt == 0 and addidx == offset then
        if #node.elems == 0 and hasparts then return end
        local newnode = create_space_dag_node(nodeversion, val)
        if #node.elems == 0 then
          node.parts:spliceinto(newnode)
        else
          space_dag_break_node(node, 1, newnode)
        end
        pidx = pidx + 1
      end
      return
    end

    -- nothing is being deleted, but need to do an insert
    if delcnt == 0 then
      local d = addidx - (offset + #node.elems)
      if d > 0 then return end -- trying to add beyond the max index
      if d == 0 and hasparts then return end -- shortcuts the processing to add a new element to a new node to enforce the order
      local newnode = create_space_dag_node(nodeversion, val)
      if d == 0 then
        node.parts:spliceinto(newnode)
      else
        space_dag_break_node(node, addidx - offset + 1, newnode)
      end
      pidx = pidx + 1
      return
    end
  end
  traverse_space_dag(node, isanc, process_patch)
end

metadags.__index.addpatchset = space_dag_add_patchset

return {
  createnode = create_space_dag_node,  
}

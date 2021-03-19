--
-- Implementation of Sync9 algorithm based on sync9.js from https://github.com/braid-org/braidjs
-- Copyright 2021 Paul Kulchenko
--

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
    if callback(node, version, prev, offset, deleted) == false then return false end
    if not deleted then offset = offset + node.elems:getlength() end
    for _, part in ipairs(node.parts) do
      if isanc(part.version) and helper(part, part.version) == false then return false end
    end
    if node.parts[0] and helper(node.parts[0], version, node) == false then return false end
  end
  return helper(node, node.version)
end

local function space_dag_get(node, index, is_anc)
  -- index value is 0-based
  local value
  local offset = 0
  traverse_space_dag(node, is_anc or function() return true end,
    function(node)
      if (index - offset < node.elems:getlength()) then
        value = node.elems[index - offset + 1]
        return false
      end
      offset = offset + node.elems:getlength()
    end)
  return value
end

local function space_dag_set(node, index, value, is_anc)
  -- index value is 0-based
  local offset = 0
  traverse_space_dag(node, is_anc or function() return true end,
    function(node)
      if (index - offset < node.elems:getlength()) then
        node.elems[index - offset + 1] = value
        return false
      end
      offset = offset + node.elems:getlength()
    end)
end

local create_space_dag_node, space_dag_add_patchset -- forward declarations
local function copy(tbl)
  local res = setmetatable({}, getmetatable(tbl))
  for k, v in pairs(tbl) do res[k] = v end
  return res
end
local metaparts = {__index = {
    splice = splice,
    slice = function(...) return {table.unpack(...)} end,
    spliceinto = spliceinto, -- insert in the appropriate slot based on binary search by version
    any = any, -- return `true` if any of the values match condition
    copy = copy,
  }}
local metaelems = {__index = {
    slice = function(...) return {table.unpack(...)} end,
    getlength = function(tbl) return #tbl end,
    getvalue = function(tbl) return table.concat(tbl, "") end,
  }}
local function getmetanode()
  -- return metatable from a function to control which nodes share metatables
  return {__index = {
      addpatchset = space_dag_add_patchset,
      getlength = function(node, isanc)
        isanc = isanc or function() return true end
        local count = 0
        traverse_space_dag(node, isanc, function(node) count = count + node.elems:getlength() end)
        return count
      end,
      getvalue = function(node, isanc)
        isanc = isanc or function() return true end
        local values = {}
        traverse_space_dag(node, isanc, function(node, _, _, offset, deleted)
            if not deleted then table.insert(values, node.elems:getvalue(offset)) end
          end)
        return table.concat(values)
      end,
      sethandler = function(node, handlers)
        local mt = getmetatable(node)
        if not mt or not mt.__index then return end
        for k, v in pairs(handlers) do
          getmetatable(node).__index["on"..k] = v
        end
      end,
      insert = function(node, nodeversion, val, idx)
        node.parts:spliceinto(create_space_dag_node(node, nodeversion, val))
        if #val > 0 then node.oninsert(nodeversion, idx, val) end
      end,
      delete = function(node, nodeversion, length, idx)
        node.ondelete(nodeversion, idx, length)
      end,
      oninsert = function() end,
      ondelete = function() end,
      get = space_dag_get,
      set = space_dag_set,
    }}
end

create_space_dag_node = function(node, version, elems, deletedby)
  assert(not elems or type(elems) == "table")
  assert(not deletedby or type(deletedby) == "table")
  return setmetatable({
      version = version, -- node version as a string
      -- list of elements this node stores; keep its metatable if one is provided
      elems = getmetatable(elems) and elems or setmetatable(elems or {}, metaelems),
      deletedby = setmetatable(deletedby or {}, metaparts), -- hash of versions this node is deleted by
      parts = setmetatable({}, metaparts), -- list of nodes that are children of this one
      -- parts[0] is a special non-versioned node that has been spliced from the elements of the curent node
      }, node and getmetatable(node) or getmetanode())
end

local function space_dag_break_node(node, splitidx, newpart)
  assert(splitidx >= 0)
  local tail = create_space_dag_node(node, nil,
    setmetatable(node.elems:slice(splitidx+1), getmetatable(node.elems)),
    node.deletedby:copy())
  tail.parts = node.parts

  node.elems = setmetatable(node.elems:slice(1, splitidx), getmetatable(node.elems))
  node.parts = setmetatable(newpart and {newpart} or {}, getmetatable(node.parts))
  node.parts[0] = tail
  return tail
end

-- add a patchset to a node, which will have `nodeversion` after patching
space_dag_add_patchset = function(node, nodeversion, patches, isanc)
  isanc = isanc or function() return true end
  local deleteupto = 0 -- position to delete elements up to
  local deferred = {} -- list of deferred callbacks
  local deletedcnt = 0 -- number of deleted elements in the current patchset
  setmetatable(patches, {__index = {
        next = function(tbl)
          table.remove(tbl, 1) -- remove the processed patch
          deleteupto = 0 -- reset delete tracker, as it's calculated per patch
          -- process deferred callbacks (for example, node splits)
          while #deferred > 0 do table.remove(deferred, 1)() end
        end,
        defer = function(_, callback, unshift)
          table.insert(deferred, unshift and 1 or #deferred+1, callback)
        end,
      }})

  local function process_patch(node, patchversion, prev, offset, isdeleted)
    if #patches == 0 then return false end -- nothing to process further
    -- get and cache length, as all node-breaking/changing cases will call `return`
    local nodelength = node.elems:getlength()
    -- get the next path to work on
    local addidx, delcnt, val = table.unpack(patches[1])
    local hasparts = node.parts:any(function(_, part) return isanc(part.version) end)
    -- since the patches in the patchset are processed as independent patches
    -- (even though the graph is only traversed once),
    -- adjust the offset for the number of deletes to use the correct position
    offset = offset - deletedcnt

    -- this element is already deleted
    if isdeleted then
      -- this patch only adds elements at the current offset
      if delcnt == 0 and addidx == offset then
        if nodelength == 0 and hasparts then return end
        if nodelength > 0 then space_dag_break_node(node, 0) end
        node:insert(nodeversion, val, addidx)
        patches:next()
      end
      return
    end

    -- nothing is being deleted, but need to do an insert
    if delcnt == 0 then
      if addidx < offset then return end -- trying to insert before the current offset
      local d = addidx - (offset + nodelength)
      if d > 0 then return end -- trying to insert after the max index
      if d == 0 and hasparts then return end -- shortcuts the processing to add a new element to a new node to enforce the order
      if d ~= 0 then space_dag_break_node(node, addidx - offset) end
      node:insert(nodeversion, val, addidx)
      patches:next()
      return
    end

    if deleteupto <= offset then
      local d = addidx - (offset + nodelength)
      if d >= 0 then return end -- trying to insert at or after the max index
      deleteupto = addidx + delcnt

      if val then
        if addidx == offset and prev then
          -- defer updates, otherwise inserted nodes affect position tracking
          patches:defer(function() node:insert(nodeversion, val, addidx) end)
          -- fall through to the next check for `deleteupto`
        else
          space_dag_break_node(node, addidx - offset)
          -- defer updates, otherwise inserted nodes affect position tracking
          patches:defer(function() node:insert(nodeversion, val, addidx) end)
          return
        end
      else
        if addidx == offset then
          -- fall through to the next check for `deleteupto`
        else
          space_dag_break_node(node, addidx - offset)
          return
        end
      end
    end

    if deleteupto > offset then
      if deleteupto <= offset + nodelength then
        if deleteupto < offset + nodelength then
          space_dag_break_node(node, deleteupto - offset)
          -- increase the number of deleted elements subtracting the number of added ones
          deletedcnt = deletedcnt + deleteupto - offset - #val
        end
        -- defer delete processing, as it needs to be processed in the opposite order
        patches:defer(function() node:delete(nodeversion, node.elems:getlength(), offset) end, true)
        patches:next()
      else
        -- defer delete processing, as it needs to be processed in the opposite order
        patches:defer(function() node:delete(nodeversion, node.elems:getlength(), offset) end, true)
      end
      node.deletedby[nodeversion] = true
      return
    end
  end
  traverse_space_dag(node, isanc, process_patch)
  patches:next() -- process any outstanding deferred actions
end

local metaparents = {__index = {
    copy = copy,
    equals = function(tbl1, tbl2)
      if #tbl1 ~= #tbl2 then return false end
      local val1, val2, key1, key2
      while true do
        key1, val1 = next(tbl1, key1)
        key2, val2 = next(tbl2, key2)
        if key1 == nil and key2 == nil then break end
        if key1 ~= nil and tbl2[key1] ~= val1
        or key2 ~= nil and tbl1[key2] ~= val2 then
          return false
        end
      end
      return true
    end,
  }}
local metaresource = {__index = {
    getvalue = function(resource, version)
      local isanc
      if version then
        local ancestors = resource:getancestors({[version] = true})
        ancestors[version] = true
        isanc = function(nodeversion) return ancestors[nodeversion] end
      end
      return resource.space:getvalue(isanc)
    end,
    getancestors = function(resource, versions)
      local results = {}
      local function helper(version)
        if results[version] then return end
        if not resource.time[version] then return end -- ignore non-existent versions
        results[version] = true
        for ver in pairs(resource.time[version]) do helper(ver) end
      end
      for ver in pairs(versions) do helper(ver) end
      return results
    end,
    addversion = function(resource, version, patchset, parents)
      assert(#patchset > 0)
      -- this version is already known
      if resource.time[version] then return end

      -- take the current version (future parents) if none are specified
      if not parents then parents = resource.futureparents:copy() end
      resource.time[version] = parents

      -- delete current parents from future ones
      for parent in pairs(parents) do resource.futureparents[parent] = nil end
      resource.futureparents[version] = true

      local isanc
      -- shortcut with a simplified version for a frequently used case
      if resource.futureparents:equals(parents) then
        isanc = function() return true end
      else
        local ancestors = resource:getancestors(parents)
        ancestors[version] = true
        isanc = function(nodeversion) return ancestors[nodeversion] end
      end
      resource.space:addpatchset(version, patchset, isanc)
    end,
    sethandler = function(resource, ...) return resource.space:sethandler(...) end,
    getspace = function(resource) return resource.space end,
    gettime = function(resource, version) return resource.time[version] end,
  }}

local M = {
  createspace = function(...) return create_space_dag_node(nil, ...) end,
}

function M.createresource(version, elem)
  if not version then version = "0" end
  return setmetatable({
      -- create root node
      space = M.createspace(version, elem),
      time = {},
      futureparents = setmetatable({[version] = true}, metaparents),
      }, metaresource)
end

return M

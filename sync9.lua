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

local function all(tbl, condition)
  for k, v in pairs(tbl) do
    if not condition(k,v) then return false end
  end
  return true
end

if not table.unpack then table.unpack = unpack end

-- traverse space graph starting from `node` and calling `callback` for each part
local function traverse_space_dag(node, isanc, callback)
  local offset = 0
  local function helper(node, version, prev, level)
    local deleted = node.deletedby:any(function(version) return isanc(version) end)
    -- callback may return `false` to indicate that traversal needs to be stopped
    if callback(node, version, prev, offset, deleted, level) == false then return false end
    if not deleted then offset = offset + node.elems:getlength() end
    for _, part in ipairs(node.parts) do
      if isanc(part.version) and helper(part, part.version, nil, level + 1) == false then return false end
    end
    if node.parts[0] and helper(node.parts[0], version, node, level) == false then return false end
  end
  return helper(node, node.version, nil, 1)
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
    any = any, -- return `true` if **any** of the values match condition
    all = all, -- return `true` if **all** of the values match condition
    copy = copy,
  }}
-- metatable to handle elements as a table
local metatblelems = {__index = {
    slice = function(tbl, ...)
      return setmetatable({table.unpack(tbl, ...)}, getmetatable(tbl))
    end,
    concat = function(tbl, tbladd) splice(tbl, #tbl+1, 0, tbladd) end,
    getlength = function(tbl) return #tbl end,
    getvalue = function(tbl) return tbl end,
    copy = copy,
  }}
-- metatable to handle elements as a string
local metastrelems = {__index = {
    slice = function(tbl, ...)
      return setmetatable({[0] = tbl[0]:sub(...)}, getmetatable(tbl))
    end,
    concat = function(tbl, str)
      tbl[0] = tbl[0]..(type(str) == "string" and str or str[0])
    end,
    getlength = function(tbl) return #tbl[0] end,
    getvalue = function(tbl) return tbl[0] end,
    copy = function(tbl) return tbl[0] end,
  }}

local function metafy(elems)
  return (type(elems) == "string" and setmetatable({[0] = elems}, metastrelems)
    or getmetatable(elems) and elems
    or setmetatable(elems or {}, metatblelems))
end

create_space_dag_node = function(version, elems, deletedby)
  assert(not elems or type(elems) == "table" or type(elems) == "string", "Unexpected elements type (not 'string' or 'table')")
  assert(not deletedby or type(deletedby) == "table")

  -- deletion calculations fail on table elements with empty strings, so strip those
  if type(elems) == "table" then
    for idx = elems and #elems or 0, 1, -1 do
      if #elems[idx] == 0 then table.remove(elems, idx) end
    end
  end

  local metanode = {__index = {
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
        local vtype
        traverse_space_dag(node, isanc, function(node, _, _, offset, deleted)
            if not deleted then
              local value = node.elems:getvalue(offset)
              -- store the value type if it's not set or if there are no elements
              -- (in case the initial empty value is of a "wrong" type,
              -- for example, an empty table instead of an empty string)
              vtype = #values > 0 and vtype or type(value)
              if vtype ~= type(value) then error("Inconsistent value types", 2) end
              splice(values, #values+1, 0, table.unpack(vtype ~= "table" and {value} or value))
            end
          end)
        return vtype == "table" and values or table.concat(values)
      end,
      walkgraph = function(node, isanc, callback)
        if not callback then return end
        isanc = isanc or function() return true end
        traverse_space_dag(node, isanc, function(node, version, prev, offset, deleted, level)
            local params = {
              version = version,
              value = node.elems:getvalue(offset),
              offset = offset,
              level = level,
              isdeleted = deleted,
              isnode = prev == nil,
              node = node,
            }
            if callback(params) == false then return false end
          end)
      end,
    }}

  return setmetatable({
      version = version, -- node version as a string
      -- list of elements this node stores; keep its metatable if one is proved
      elems = metafy(elems),
      deletedby = setmetatable(deletedby or {}, metaparts), -- hash of versions this node is deleted by
      parts = setmetatable({}, metaparts), -- list of nodes that are children of this one
      }, metanode)
end

local function space_dag_break_node(node, splitidx, version)
  assert(splitidx >= 0)
  local tail = create_space_dag_node(nil,
    setmetatable(node.elems:slice(splitidx+1), getmetatable(node.elems)),
    node.deletedby:copy())
  tail.parts = node.parts

  node.elems = setmetatable(node.elems:slice(1, splitidx), getmetatable(node.elems))
  node.parts = setmetatable({}, getmetatable(node.parts))
  node.parts[0] = tail
  if splitidx == 0 and version then node.deletedby[version] = true end
  return tail
end

-- add a patchset to a node, which will have `nodeversion` after patching
space_dag_add_patchset = function(node, nodeversion, patchset, isanc)
  isanc = isanc or function() return true end
  local deleteupto = 0 -- position to delete elements up to
  local deferred = {} -- list of deferred callbacks
  local deletedcnt = 0 -- number of deleted elements in the current patchset
  setmetatable(patchset, {__index = {
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

  local function process_patch(node, _, prev, offset, isdeleted)
    if #patchset == 0 then return false end -- nothing to process further
    -- get and cache length, as all node-breaking/changing cases will call `return`
    local nodelength = node.elems:getlength()
    -- get the next path to work on
    local addidx, delcnt, val = table.unpack(patchset[1])
    local hasparts = node.parts:any(function(_, part) return isanc(part.version) end)
    -- since the patches in the patchset are processed as independent patches
    -- (even though the graph is only traversed once),
    -- adjust the offset for the number of deletes to use the correct position
    offset = offset - deletedcnt

    -- this element is already deleted
    if isdeleted then
      -- this patch only adds elements at the current offset
      if delcnt == 0 and addidx == offset then
        -- this check is needed to enforce the order of items added inside of a deleted node.
        -- Without this check the order depends on the sorting of version numbers,
        -- which is not ideal for local edits, even though the produced graph is more compact
        if nodelength == 0 and hasparts then return end
        -- break the node if insert is at the beginning of the node
        if nodelength > 0 then space_dag_break_node(node, 0) end
        node.parts:spliceinto(create_space_dag_node(nodeversion, val))
        patchset:next()
      end
      return
    end

    -- nothing is being deleted, but need to do an insert
    if delcnt == 0 then
      if addidx < offset then return end -- trying to insert before the current offset
      local d = addidx - (offset + nodelength)
      if d > 0 then return end -- trying to insert after the max index
      if d == 0 and hasparts then return end -- shortcuts the processing to add a new element to a new node to enforce the order
      if d ~= 0 then space_dag_break_node(node, addidx - offset, nodeversion) end
      node.parts:spliceinto(create_space_dag_node(nodeversion, val))
      patchset:next()
      return
    end

    if deleteupto <= offset then
      local d = addidx - (offset + nodelength)
      if d >= 0 then return end -- trying to insert at or after the max index
      deleteupto = addidx + delcnt

      if val and #val > 0 then
        if addidx == offset and prev then
          -- defer updates, otherwise inserted nodes affect position tracking
          patchset:defer(function()
              node.parts:spliceinto(create_space_dag_node(nodeversion, val))
            end)
          -- fall through to the next check for `deleteupto`
        else
          space_dag_break_node(node, addidx - offset, nodeversion)
          -- defer updates, otherwise inserted nodes affect position tracking
          patchset:defer(function()
              node.parts:spliceinto(create_space_dag_node(nodeversion, val))
            end)
          return
        end
      else
        if addidx == offset then
          -- fall through to the next check for `deleteupto`
        else
          space_dag_break_node(node, addidx - offset, nodeversion)
          return
        end
      end
    end

    if deleteupto > offset then
      if deleteupto <= offset + nodelength then
        if deleteupto < offset + nodelength then
          space_dag_break_node(node, deleteupto - offset, nodeversion)
          -- increase the number of deleted elements subtracting the number of added ones
          deletedcnt = deletedcnt + deleteupto - offset - (val and #val or 0)
        end
        patchset:next()
      end
      node.deletedby[nodeversion] = true
      return
    end
  end
  traverse_space_dag(node, isanc, process_patch)
  patchset:next() -- process any outstanding deferred actions
end

local metaparents = {__index = {
    copy = copy,
    all = all, -- return `true` if all of the values match condition
    equals = function(tbl1, tbl2)
      if #tbl1 ~= #tbl2 then return false end
      local val1, val2, key1, key2
      while true do
        key1, val1 = next(tbl1, key1)
        key2, val2 = next(tbl2, key2)
        -- check if the both tables ended at the same time
        -- they are equal and nothing else needs to be done
        if key1 == nil and key2 == nil then break end
        -- if the keys are not `nil` and are not cross-equal
        -- then tables are not equal; this check is needed,
        -- as "equal" tables can return keys in different order
        if key1 ~= nil and tbl2[key1] ~= val1
        or key2 ~= nil and tbl1[key2] ~= val2 then
          return false
        end
      end
      return true
    end,
  }}

local function prune(resource, keeplist, startlist)
  if not keeplist then keeplist = {} end
  if not startlist then startlist = resource:getparents() end

  -- find the "root" version (the one without paretns) in the time dag
  local root
  for version, parents in pairs(resource.time) do
    if not next(parents) then
      root = version
      break
    end
  end
  assert(root) -- need to have root version
  -- add "missing" references for all orphan versions
  for _, parents in pairs(resource.time) do
    for parent in pairs(parents) do
      if not resource.time[parent] then
        resource.time[parent] = {[root] = true}
      end
    end
  end
  -- also check the `startlist` for any orphan references
  for parent in pairs(startlist) do
    if not resource.time[parent] then
      resource.time[parent] = {[root] = true}
    end
  end

  -- populate children versions based on the parent versions available
  local children = setmetatable({}, metaparents)
  for version, parents in pairs(resource.time) do
    for parent in pairs(parents) do
      if not children[parent] then children[parent] = {} end
      children[parent][version] = true
    end
  end

  -- to prune we need to find "bubbles" in the dag,
  -- with a "bottom" and "top" version,
  -- where any path down from the top will hit the bottom,
  -- and any path up from the bottom will hit the top.
  -- Dag grows top-down, so the oldest (parent) versions are on top.
  -- Also, the bubble should not contain any versions on the `keeplist`
  -- (unless it's the bottom)

  -- compute the bubbles
  local bubbles = {}
  local tops = {}
  local bottoms = {}

  local function markbubble(bottom, top, tag)
    if not bubbles[bottom] then
      bubbles[bottom] = tag
      if bottom ~= top then
        for parent in pairs(resource.time[bottom]) do
          markbubble(parent, top, tag)
        end
      end
    end
  end

  local function findbubble(cur)
    local seen = {[cur] = true}
    local q = {}
    local expecting = {}
    for parent in pairs(resource.time[cur]) do
      table.insert(q, parent)
      expecting[parent] = true
    end
    while #q > 0 do
      cur = table.remove(q)
      assert(resource.time[cur])
      if keeplist[cur] then return end
      if children.all(children[cur], function(c) return seen[c] end) then
        seen[cur] = true
        expecting[cur] = nil
        if not next(expecting) then return cur end
        for parent in pairs(resource.time[cur]) do
          table.insert(q, parent)
          expecting[parent] = true
        end
      end
    end
  end

  local done = {}
  local function f(cur)
    assert(resource.time[cur])
    if done[cur] then return end
    done[cur] = true

    if not bubbles[cur] or tops[cur] then
      local top = findbubble(cur)
      if top then
        bubbles[cur] = nil
        markbubble(cur, top, tops[cur] or cur)
        tops[top] = tops[cur] or cur
        bottoms[tops[cur] or cur] = top
      end
    end
    for parent in pairs(resource.time[cur]) do f(parent) end
  end

  for parent in pairs(startlist) do f(parent) end

  local function space_dag_prune(node)
    local isasc = function() return true end
    traverse_space_dag(node, isasc, function(node)
        local replacement = bubbles[node.version]
        if (replacement and replacement ~= node.version) then
          node.version = replacement
        end

        for version in pairs(node.deletedby) do
          if bubbles[version] then
            -- if the node is deleted by a pruned version,
            -- replace the reference with its replacement
            node.deletedby[version] = nil
            node.deletedby[bubbles[version]] = true
          end
        end
      end)

    -- assign the next element at the very end of the `line` of elements
    local function setnextnode(node, nextnode)
      while (node.parts[0]) do node = node.parts[0] end
      node.parts[0] = nextnode
    end

    -- `line` is a sequence of nodes connected together with next (`parts[0]`) references
    -- this method combines the entire line into one node under cetrain conditions
    local function doline(node)
      local version = node.version
      local prev
      while node do repeat -- use `repeat` to emulate `continue` with `break` statement
        -- only check the first version, as all parts are expected to have the same version
        if node.parts[1] and node.parts[1].version == version then
          for i = 1, #node.parts do
            setnextnode(node.parts[i], i < #node.parts and node.parts[i + 1] or node.parts[0])
          end
          node.parts[0] = node.parts[1]
          node.parts:splice(1, #node.parts) -- remove all parts, as they have been consolidated
        end

        -- if the node is deleted by the current version,
        -- empty the number of elements, as it's unreachable
        if node.deletedby[version] then
          local dummy = create_space_dag_node(node.version, node.elems:slice(1,0))
          node.elems = dummy.elems
          node.deletedby = dummy.deletedby
          if prev then
            node = prev
            break -- continue
          end
        end

        local nextnode = node.parts[0]

        if (#node.parts == 0 and nextnode
          -- no elements in this node or the next node
          and (node.elems:getlength() == 0 or nextnode.elems:getlength() == 0
            -- both this and next node have the same `deletedby` list of versions
            or (node.deletedby:all(function(x) return nextnode.deletedby[x] end)
              and nextnode.deletedby:all(function(x) return node.deletedby[x] end)))) then
          -- empty nodes are treated the same way as deleted nodes
          if node.elems:getlength() == 0 then node.deletedby = nextnode.deletedby end
          node.elems:concat(nextnode.elems)
          node.parts = nextnode.parts
          break -- continue
        end

        for _, node in ipairs(node.parts) do doline(node) end

        prev = node
        node = nextnode
      until not node end
    end

    doline(node)
  end

  space_dag_prune(resource.space)

  -- update time graph with the version replacements
  for prunedversion, replacement in pairs(bubbles) do
    if prunedversion == bottoms[replacement] then
      -- connect the newest (bottom) version to the parents of the oldest (top) pruned version
      resource.time[replacement] = resource.time[prunedversion]
    end
    if prunedversion ~= replacement then
      -- remove all pruned versions from history
      resource.time[prunedversion] = nil
    end
  end

  -- remove any references to pruned versions from `futureparents`
  -- this normally shouldn't happen, but may happen if the orphan version
  -- get pointed to the root version already present in `futureparents`,
  -- in which case it may get pruned and needs to be removed.
  local orphans = {}
  for parent in pairs(resource.futureparents) do
    if not resource.time[parent] then orphans[parent] = true end
  end
  for parent in pairs(orphans) do
    resource.futureparents[parent] = nil
  end
end

local M = {
  createspace = create_space_dag_node,
}

function M.createresource(version, elem)
  if not version then version = "0" end

  local metaresource = {__index = {
      getvalue = function(resource, version)
        local isanc
        if version then
          local ancestors = resource:getancestors({[version] = true})
          isanc = function(nodeversion) return ancestors[nodeversion] end
        end
        return resource.space:getvalue(isanc)
      end,
      walkgraph = function(resource, callback, version)
        local isanc
        if version then
          local ancestors = resource:getancestors({[version] = true})
          isanc = function(nodeversion) return ancestors[nodeversion] end
        end
        return resource.space:walkgraph(isanc, callback)
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
          ancestors[version] = true -- include the version itself
          isanc = function(nodeversion) return ancestors[nodeversion] end
        end
        resource.space:addpatchset(version, patchset, isanc)
        resource:onversion(version)
      end,
      sethandler = function(resource, ...) return resource.space:sethandler(...) end,
      getspace = function(resource) return resource.space end,
      gettime = function(resource, version) return resource.time[version] end,
      getparents = function(resource, version)
        return version and resource.time[version] or resource.futureparents:copy()
      end,
      -- generates patchset for a particular version
      getpatchset = function(resource, version, versions)
        local ancestors = resource:getancestors(versions or resource.futureparents)
        local isanc = function(nodeversion) return ancestors[nodeversion] end
        local patchset = {}
        local function process_patch(node, nodeversion, _, offset)
          if version == nodeversion then
            if not node.deletedby[version] then
              -- insert: the patch matches the version and is not deleted by the same version
              table.insert(patchset, {offset, 0, node.elems:copy()})
            end
          elseif node.deletedby[version] and node.elems:getlength() > 0
          -- skip if this entry is deleted by another ancestor version
          and not node.deletedby:any(function(v) return v ~= version and isanc(v) end) then
            -- delete: the patch is deleted by this version and is not deleted by another ancestor version
            table.insert(patchset, {offset, node.elems:getlength()})
          end
        end
        traverse_space_dag(resource:getspace(), isanc, process_patch)
        return patchset
      end,
      -- remap index as of a particular `indexversions` to the current `versions`
      getindex = function(resource, indexversions, index, versions)
        -- if the question is about one of the "current" versions,
        -- then no remapping calculations need to be done
        if (versions or resource.futureparents):equals(indexversions) then return index end
        local ancestors = resource:getancestors(versions or resource.futureparents)
        local isanc = function(nodeversion) return ancestors[nodeversion] end
        -- assign ancestors and offset for the specified ("old") version
        local ancvers = resource:getancestors(indexversions)
        local isver = function(nodeversion) return ancvers[nodeversion] end
        local offver = 0
        local adjustment
        local function process_patch(node, nodeversion, deleted, offset)
          -- this is the node that is known by `nodeversion`
          if ancvers[nodeversion] then
            local nodelen = node.elems:getlength()
            -- adjustment is between the current `offset`
            -- and the calculated position for the desired version(s)
            -- which can be achieved over added or deleted elements
            if not node.deletedby:any(isanc) then
              offver = offver + nodelen
              if index <= offver then
                adjustment = offset - (offver - nodelen)
                return false
              end
            elseif not node.deletedby:any(isver) then
              offver = offver + math.min(nodelen, index-offver)
              if index <= offver then
                adjustment = offset - offver
                return false
              end
            end
          end
        end
        traverse_space_dag(resource:getspace(), isanc, process_patch)
        return index + (adjustment or 0)
      end,
      prune = prune,
      sethandler = function(node, handlers)
        local mt = getmetatable(node)
        if not mt or not mt.__index then return end
        for k, v in pairs(handlers) do
          getmetatable(node).__index["on"..k] = v
        end
      end,
      onversion = function() end,
    }}

  return setmetatable({
      -- create root node
      space = M.createspace(version, elem),
      time = {[version] = {}},
      futureparents = setmetatable({[version] = true}, metaparents),
      }, metaresource)
end

return M

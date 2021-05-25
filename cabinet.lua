--
-- Implementation of CRDT algorithm based on https://braid.org/algorithms/shelf
-- Copyright 2021 Paul Kulchenko
--
local mt = {}

local function setproxy(t, options)
  -- if our metatable is set already, then do nothing
  -- this assumes that all other tables are set recursively
  if getmetatable(t) == mt then return t end

  options = options or {}
  options.values = t
  options.version = options.version or 1
  options.versions = options.versions or {}
  local version = options.version

  -- store the original table in a private reference
  -- to avoid conflicts with actual values
  local self = setmetatable({[mt] = options}, mt)
  for k, v in pairs(t) do
    if type(v) == "table" then
      -- apply recursively to all tables to convert them too
      t[k] = setproxy(v, {version = version, parent = self, key = k})
    else
      self[k] = v
    end
    self[mt].versions[k] = self[mt].versions[k] or version
  end
  return self
end

local function vivify(self, key, val)
  local mt = debug.getmetatable(self)
  local p = mt.parent
  p[mt.key] = setproxy({}, {parent = p, key = mt.key})
  p[mt.key][key] = val
end

local function loadsafe(data)
  local f, err = (load or loadstring)(data)
  if not f then return f, err end
  local c = 0
  local hf, hm, hc = debug.gethook()
  debug.sethook(function() c=c+1; if c>=3 then error("failed safety check") end end, "c")
  local ok, res = pcall(f)
  c = 0
  debug.sethook(hf, hm, hc)
  return ok, res
end

local function addpatch(t, path, strvalue)
  local key, version, oldvalue
  local ok, value = loadsafe("return ("..strvalue..")")
  if not ok then error("Unexpected value in `addpatch`: "..value) end
  -- save the callback value as `t` may change
  local onupdate = t[mt].onupdate
  while true do
    key, version, path = path:match("([^:]+):([^/]+)/?(.*)")
    version = tonumber(version)
    key = tonumber(key) or key -- keys that look like numbers are stored as numbers
    -- stop traversal if the new version of any element in the path
    -- is lower than the existing version
    if version < (t[mt].versions[key] or 0) then
      return
    end
    if path == "" then
      -- if the versions are the same:
      if t[mt].versions[key] == version
      -- table wins or larger value wins
      and (type(t[key]) == "table" or t[key] >= value) then
        break
      end
      oldvalue = t[key]
      t[key] = value
      t[mt].versions[key] = version
      break
    end
    t = t[key]
  end
  -- send merged updates
  if onupdate then onupdate(t, key, oldvalue) end
end

local function notify(t, key, value, path)
  local mt = getmetatable(t)
  local parent = t[mt] and t[mt].parent
  path = (key..":"..(t[mt].versions[key] or t[mt].version)) .. (path and "/"..path or "")
  local subscriptions = t[mt].subscriptions and t[mt].subscriptions[t[mt].tag]
  for _, s in ipairs(subscriptions or {}) do
    addpatch(s, path, value)
  end
  if parent then notify(parent, t[mt].key, value, path) end
end

function mt.__index(self, key)
  return (self[mt].values[key] == nil
    and debug.setmetatable(nil, {parent = self, key = key, __index = mt.__index, __newindex = vivify})
    or self[mt].values[key])
end

local function tostr(s)
  return (type(s) == "number" and ("%.17g"):format(s)
    or type(s) == "table" and "{}"
    or type(s) ~= "string" and tostring(s)
    -- escape NEWLINE/010 and EOF/026
    or ("%q"):format(s):gsub("\010","n"):gsub("\026","\\026"))
end

function mt.__len(self) return #self[mt].values end

function mt.__newindex(self, key, value)
  -- if the value doesn't change, don't need to do anything
  -- as long as the version number is specified;
  -- if not, it's a new assignment, so still process those
  if self[mt].values[key] == value and self[mt].versions[key] then return end
  local version = (self[mt].versions[key] or self[mt].version) + 1
  -- if the `value` is `nil`, there are three options
  -- 1. update the version without removing the version tracking (as implemented)
  -- 2. remove the version key, which will effectively reset the version count
  -- 3. remove the version key and increment the table version
  self[mt].versions[key] = version
  -- send updates to subscriptions
  notify(self, key, tostr(value))
  -- turn the assigned table into proxied table
  if type(value) == "table" then
    value = setproxy(value, {parent = self, version = version, key = key})
  end
  self[mt].values[key] = value
end

local M = {}
local subscriptions = {}
function M.publish(tag, tbl)
  return setproxy(tbl or {}, {tag = tag, subscriptions = subscriptions})
end
function M.unpublish(tag) subscriptions[tag] = nil end
function M.subscribe(tag, onupdate)
  -- create tag if it's not present, as subscription can be done before publishing
  subscriptions[tag] = subscriptions[tag] or {}
  local tbl = setproxy({}, {onupdate = onupdate})
  table.insert(subscriptions[tag], tbl)
  return tbl
end

return M

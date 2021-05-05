local sync9 = require "sync9"
local unpack = table.unpack or unpack

local DOCCOUNT = 4

local alphabet = "abcdefghijklmnopqrstuvwxyz 0123456789"
local function randchar()
  local pos = math.random(#alphabet)
  return alphabet:sub(pos, pos)
end

local docsyncs = {}
local patches = {}
for idx = 1, DOCCOUNT do
  docsyncs[idx] = sync9.createresource(0, "")
  patches[idx] = {}
end

local function mergependingpatches(docidx, num)
  for idx = 1, #patches do
    if not docidx or idx == docidx then
      -- apply the changes to the "edited" document
      local patchcnt = math.min(num or #patches[idx], #patches[idx])
      for i = 1, patchcnt do
        docsyncs[idx]:addversion(unpack(table.remove(patches[idx], 1)))
      end
    end
  end
end  

local function getgraph(resource)
  local callbacks = {}
  resource:walkgraph(function(args)
    table.insert(callbacks, table.concat({args.version, args.value, args.level, args.offset,
          args.isdeleted and "t" or "f", args.isnode and "t" or "f"}, "-"))
  end)
  return table.concat(callbacks, ";"), #callbacks
end

local seed = math.random(10000)
math.randomseed(seed)

for i = 1, 5000 do
  if i % 1000 == 0 then
    local nodecnt = 0
    docsyncs[1]:walkgraph(function() nodecnt = nodecnt + 1 end)
    print(("%d: value length = %s; graph node count = %s"):format(i, #docsyncs[1]:getvalue(), nodecnt))
  end

  -- pick the document to work with
  local docidx = math.random(#docsyncs)
  local docsize = #(docsyncs[docidx]:getvalue())
  local version = ("%06d-%s"):format(i, docidx)
  local parents = docsyncs[docidx]:getparents()

  local insert = docsize == 0 or math.random() < 0.5
  -- insert can be done before and after a position, but delete only before
  local pos = math.random(docsize + (insert and 1 or 0)) - 1
  local length = math.random(math.min(10, insert and 10 or math.max(1, docsize)))
  local content = randchar():rep(length)
  
  -- distribute the patch among the documents
  for idx = 1, #patches do
    local patch = insert and {pos, 0, content} or {pos, length}
    if idx == docidx then
      -- apply the changes to the "edited" document
      docsyncs[docidx]:addversion(version, {patch}, parents)
    else  
      -- push pending patches to other documents
      table.insert(patches[idx], {version, {patch}, parents})
    end
  end

  -- merge some of (up to 3) the pending changes for a random document
  if math.random() <= 0.3 then mergependingpatches(math.random(DOCCOUNT), math.random(3)) end

  -- cross-merge all of them with some probability
  if math.random() <= 0.1 then mergependingpatches() end

  -- compare the results if there are no outstanding patches
  local pendingcnt = 0
  for i = 1, #patches do pendingcnt = pendingcnt + #patches[i] end
  if pendingcnt == 0 then
    local value = docsyncs[1]:getvalue()
    local graph, num = getgraph(docsyncs[1])
    for i = 2, #patches do
      assert(value == docsyncs[i]:getvalue(),
        ("Failed value comparison %s (seed=%d)\n1=%s\n%d=%s\n"):format(
          i, seed, value, i, docsyncs[i]:getvalue()))
      assert(graph == getgraph(docsyncs[i]),
        ("Failed graph comparison %s (seed=%d)\n1=%s\n%d=%s\n"):format(
          i, seed, graph, i, getgraph(docsyncs[i])))
    end
    -- prune all histories and compare again
    for i = 1, #patches do
      docsyncs[i]:prune()
      assert(value == docsyncs[i]:getvalue(),
        ("Failed value comparison after pruning %s (seed=%d)\nexpected=%s\nreceived=%s\n"):format(
          i, seed, value, docsyncs[i]:getvalue()))
      local parents = docsyncs[i]:getparents()
      -- if there is a single parent on top, everything has been pruned
      if not next(parents, next(parents)) then
        local pgraph, pnum = getgraph(docsyncs[i])
        assert(pnum == 1,
          ("Failed history pruning %s (seed=%d)\nexpected=%s\nreceived=%s (%s)\n"):format(
            i, seed, 1, pnum, pgraph))
      end
    end
  end
end

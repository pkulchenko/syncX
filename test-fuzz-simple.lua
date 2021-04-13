local sync9 = require "sync9"

local alphabet = "abcdefghijklmnopqrstuvwxyz 0123456789"
local function randchar()
  local pos = math.random(#alphabet)
  return alphabet:sub(pos, pos)
end

local docraw = ''
local docsync = sync9.createresource(0, "")
local seed = math.random(10000)
math.randomseed(seed)

for i = 1, 5000 do
  if i % 1000 == 0 then
    local nodecnt = 0
    docsync:walkgraph(function(args) nodecnt = nodecnt + 1 end)
    print(("%d: value length = %s; graph node count = %s"):format(i, #docraw, nodecnt))
  end

  local pos = math.random(#docraw+1)-1
  local length = math.random(math.min(10, #docraw))
  if #docraw == 0 or math.random() < (#docraw < 100 and 0.55 or 0.45) then
    local content = randchar():rep(length)
    docraw = docraw:sub(1, pos) .. content .. docraw:sub(pos+1)
    docsync:addversion(i, {{pos, 0, content}})
  else
    docraw = docraw:sub(1, pos) .. docraw:sub(pos+1+length)
    docsync:addversion(i, {{pos, length}})
  end

  if docsync:getvalue() ~= docraw then
    print(("Failed test %s (seed=%d)\nexpected %s\nreceived %s\n"):format(i, seed, docsync:getvalue(), docraw))
    os.exit()
  end
end

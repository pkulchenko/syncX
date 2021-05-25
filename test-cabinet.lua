require "testwell"

local cabinet = require "cabinet"
local c = cabinet.subscribe("state", function(t, key, oldval) end)
local t = cabinet.publish("state", {a = 1, b = true})
is(t.a, 1)
is(c.a, t.a)
is(t.b, true)
is(c.b, t.b)

t.c.d = "abc"
is(t.c.d, "abc")
is(c.c.d, t.c.d)

t.c.e = 4
is(t.c.e, 4)
is(c.c.e, t.c.e)

t.a = 2
is(t.a, 2)
is(c.a, t.a)

t.a = "3"
is(t.a, "3")
is(c.a, t.a)

t.c.a = 4
is(t.c.a, 4)
is(c.c.a, t.c.a)

t.c.f[1] = 2
is(#t.c.f, 1)
is(t.c.f[1], 2)
is(c.c.f[1], t.c.f[1])

t.c.d = nil
is(type(t.c.d), "nil")
is(type(c.c.d), "nil")

t.c.d = {g = 5}
is(t.c.d.g, 5)
is(c.c.d.g, 5)

t.c.d.h = 6
is(t.c.d.h, 6)
is(c.c.d.h, 6)

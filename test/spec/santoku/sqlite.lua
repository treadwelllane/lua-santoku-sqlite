local assert = require("luassert")
local test = require("santoku.test")
local err = require("santoku.err")

local sql = require("santoku.sqlite")

test("sqlite", function ()

  test("should wrap various functions", function ()

    local ok, db = sql.open_memory()
    assert.equals(true, ok)

    local ok, run_ddl = db:runner([[
      create table cities (
        name text,
        state text
      );
    ]])
    assert.equals(true, ok, run_ddl)

    local ok, res = run_ddl()
    assert.equals(true, ok, res)
    assert.is_nil(res)

    local ok, addcity = db:runner([[
      insert into cities (name, state) values ($1, $2)
    ]])
    assert.equals(true, ok, addcity)

    local ok, val = addcity("New York", "New York")
    assert.equals(true, ok)
    assert.is_nil(val)

    local ok, val = addcity("Buffalo", "New York")
    assert.equals(true, ok)
    assert.is_nil(val)

    local ok, val = addcity("Albany", "New York")
    assert.equals(true, ok)
    assert.is_nil(val)

    local ok, val = addcity("Tampa", "Florida")
    assert.equals(true, ok)
    assert.is_nil(val)

    local ok, val = addcity("Miami", "Florida")
    assert.equals(true, ok)
    assert.is_nil(val)

    local ok, getcity = db:getter([[
      select * from cities where name = $1
    ]])
    assert.equals(true, ok, getcity)

    local ok, city = getcity("Tampa")
    assert.equals(true, ok)
    assert.same(city, { name = "Tampa", state = "Florida" })

    local ok, city = getcity("Albany")
    assert.equals(true, ok)
    assert.same(city, { name = "Albany", state = "New York" })

    local ok, getcitystate = db:getter([[
      select * from cities where name = $1
    ]], "state")
    assert.equals(true, ok, getcitystate)

    local ok, state = getcitystate("Albany")
    assert.equals(true, ok)
    assert.same(state, "New York")

    local ok, getstates = db:iter([[
      select * from cities
    ]])
    assert.equals(true, ok, getstates)

    local ok, states = getstates()
    assert.equals(true, ok, states)

    states = states:vec()
    assert.same(states[1], { true, { name = "New York", state = "New York" }, n = 2 })
    assert.same(states[2], { true, { name = "Buffalo", state = "New York" }, n = 2 })
    assert.same(states[3], { true, { name = "Albany", state = "New York" }, n = 2 })
    assert.equals(states.n, 5)

    local ok, allstates = db:all([[
      select * from cities
    ]])
    assert.equals(true, ok, allstates)

    local ok, states = allstates()
    assert.equals(true, ok, states)

    assert.same(states[1], { name = "New York", state = "New York" })
    assert.same(states[2], { name = "Buffalo", state = "New York" })
    assert.same(states[3], { name = "Albany", state = "New York" })
    assert.equals(states.n, 5)

  end)

  test("should handle co iterators", function ()

    local db = err.check(sql.open_memory())

    err.check(db:exec([[
      create table numbers (
        n integer
      );
    ]]))

    local addn = err.check(db:inserter([[
      insert into numbers (n) values ($1)
    ]]))

    for i = 1, 100 do
      err.check(addn(i))
    end

    local getns = err.check(db:iter([[
      select * from numbers
    ]]))

    local as = err.check(getns()):co():take(2):map(err.check):vec()
    local bs = err.check(getns()):co():take(2):map(err.check):vec()

    assert.same(as, bs)

  end)

  test("should handle with clauses", function ()

    local db = err.check(sql.open_memory())

    err.check(db:exec([[
      create table numbers (
        n integer
      );
    ]]))

    local addn = err.check(db:inserter([[
      insert into numbers (n) values ($1)
    ]]))

    for i = 1, 100 do
      err.check(addn(i))
    end

    local getns = err.check(db:getter([[
      with evens as (select * from numbers where n % 2 == 0)
      select n from evens
      order by n desc
    ]]))

    assert.same({ true, { n = 100 } }, { getns() })

  end)

end)

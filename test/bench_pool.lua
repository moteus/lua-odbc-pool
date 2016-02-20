-- Configuration
local CONNECTION_COUNT    = 1
local THREADS_COUNT       = 2
local QUERY_COUNT         = 100
local WAIT_TIMEOUT        = 100 -- msec
local AVG_QUERY_DURATION  = 200 -- msec

----------------------------------------

local function worker(seed, sql, timeout, qduration, n)
  local odbcpool = require "odbc.dba.pool"
  local ztimer   = require "lzmq.timer"

  math.randomseed(seed)

  local cli = odbcpool.client("benchmark")

  local acquire_fail = 0
  local timer = ztimer.monotonic():start()

  for i = 1, n do
    local ok, err = cli:acquire(timeout, function(cnn)
      if sql then
        local val, err = cnn:first_value(sql)
        if err and not val then
          sql_fail = sql_fail + 1
        end
      end
      if qduration then
        ztimer.sleep(math.random(qduration*2))
      end
    end)
    if err and not ok then
      acquire_fail = acquire_fail + 1
    end
  end

  return {acquire_fail, timer:stop()}
end

local IS_WINDOWS = (require"package".config:sub(1,1) == '\\')
local CNN = {
  Driver   = IS_WINDOWS and "SQLite3 ODBC Driver" or "SQLite3";
  Database = ":memory:";
}

local ok, Threads = pcall(require, "llthreads.ex")
if not ok then Threads = require "llthreads2.ex" end
local ztimer     = require "lzmq.timer"
local odbc       = require "odbc"
local odbcpool   = require "odbc.pool"

local env = odbc.environment()

local cnn = env:connection()
odbc.assert(cnn:driverconnect(CNN))
cnn:destroy()

local cli     = odbcpool.client("benchmark")
local rthread = odbcpool.reconnect_thread(cli, CNN)
rthread:start()

local connections = {}
for i = 1, CONNECTION_COUNT do
  local cnn = odbc.assert(env:connection())
  connections[#connections+1] = cnn
  cli:reconnect(cnn)
end

local threads = {}
for i = 1, THREADS_COUNT do
  local thread = Threads.new(worker, 
    os.time() + (12548 * i),
    SQL,
    WAIT_TIMEOUT,
    AVG_QUERY_DURATION,
    QUERY_COUNT
  )
  threads[#threads + 1] = thread
end

local timer = ztimer.monotonic():start()
for _, thread in ipairs(threads) do
  thread:start()
end

local results = {}
for _, thread in ipairs(threads) do
  local status, result = thread:join()
  assert(status)
  results[#results + 1] = result
end

local elapsed = timer:stop()

local total_query = QUERY_COUNT * THREADS_COUNT
local fail_query, query_duration = 0, 0
for _, result in ipairs(results) do
  fail_query = fail_query + result[1]
  query_duration = query_duration + result[2]
end

local msg = string.format([[
Input data:
  Threads             : %d
  Total query         : %d (%d per thread)
  Avg query duration  : %d[msec]
  Max wait            : %d[msec]
Result:
  Elapset time        : %.2f[sec]
  Real query duration : %.2f[msec]
  Fail                : %d (%.2f%%)
]],
  THREADS_COUNT,
  total_query, QUERY_COUNT,
  AVG_QUERY_DURATION,
  WAIT_TIMEOUT,

  elapsed / 1000,
  query_duration / total_query,
  fail_query, 100 * (fail_query / total_query)
)

print(msg)

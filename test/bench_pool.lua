#!/usr/bin/env lua

local function show_help() print([[
Usage:
   bench [-h] [-c VALUE] [-t VALUE] [-q VALUE] [-d VALUE] 
        [-w VALUE]  [--sql QUERY]
Arguments:
  -c VALUE --connection-count VALUE
                        Number of concurent connections
  -t VALUE --thread-count value VALUE
                        Number of concurent connections
  -q VALUE --query-count value VALUE
                        Number of query in each thread
  -d VALUE --query-duration VALUE
                        Avg duration of each query in msec
  -w VALUE --wait-timeout VALUE
                        Max timeout to wait avaliable connection
  --s STR --connection-string STR
                        Connection string to connect to DB.
                        By default use SQLite3 in memory DB.
  --sql QUERY
                        Query to execute. By default use 
                        only timeout to emulate query to DB
]]) end

-----------------------------------------------------------------------------------------
-- parse command line arguments
local args = {} do
local arg = { ... } 

local function read_key_(arg, i)
   if arg[i]:sub(1,1) ~= '-' then return nil, nil, i end
   if not arg[i]:sub(2,2) then return "-", nil, i+1 end

   local key, value
   if arg[i]:sub(2,2) ~= '-' then
      key   = arg[i]:sub(2,2)
      value = arg[i]:sub(3)
      if #value == 0 then
         i = i + 1
         value = arg[i]
      elseif value:sub(1,1) == '=' then
         value = value:sub(2)
      end
      return key, value, i + 1
   end

   key = arg[i]:sub(3):match("^([^=]+)=")
   if key then
      value = arg[i]:sub(4 + #key)
      return key, value, i+1
   end

   return arg[i]:sub(3), arg[i+1], i+2
end

local function read_key(arg, i)
   local key, value, n = read_key_(arg, i)
   if n == (i + 1) then return key, value, n end
   assert(n == (i + 2))

   if not value then return key, "true", n - 1 end

   if (#value > 1) and (value:sub(1, 1) == '-') and (not value:match("^%-%d+$")) then
      return key, "true", n - 1
   end

   return key, value, n
end

local function check_int(key, value)
  local v = tonumber(value)
  if not v then
    print("Invalid value switch `" ..  key .. "': " .. value)
    os.exit(-1)
  end
  return v
end

local i = 1
while arg[i] do
  local key, value
  key, value, i = read_key(arg, i)
  if key then
    if (key == "h") or (key == "help") then
      show_help()
      os.exit(0)
    elseif (key == "c") or (key == "connection-count") then
      args.connection_count = check_int(key, value)
    elseif (key == "t") or (key == "thread-count") then
      args.thread_count = check_int(key, value)
    elseif (key == "q") or (key == "query-count") then
      args.query_count = check_int(key, value)
    elseif (key == "d") or (key == "query-duration") then
      args.query_duration = check_int(key, value)
    elseif (key == "w") or (key == "wait-timeout") then
      args.wait_timeout = check_int(key, value)
    elseif (key == "s") or (key == "connection-string") then
      args.connection_string = value
    elseif (key == "sql") then
      args.sql = value
    else
      print("Invalid switch: " .. key)
      show_help()
      os.exit(1)
    end
  end
end

end
-----------------------------------------------------------------------------------------

-- Configuration
local CONNECTION_COUNT    = args.connection_count or 1
local THREADS_COUNT       = args.thread_count     or 2
local QUERY_COUNT         = args.query_count      or 100
local WAIT_TIMEOUT        = args.wait_timeout     or 100 -- msec
local AVG_QUERY_DURATION  = args.query_duration
local SQL                 = args.sql
local CNN
if args.connection_string then CNN = {args.connection_string} end
if not (SQL or AVG_QUERY_DURATION) then AVG_QUERY_DURATION = 100 end

-----------------------------------------------------------------------------------------

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
CNN = CNN or {
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
  Elapsed time        : %.2f[sec]
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

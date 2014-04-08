local odbc     = require "odbc"
local odbcpool = require "odbc.dba.pool"

local IS_WINDOWS = (require"package".config:sub(1,1) == '\\')

local CNN_COUNT = 1

local QUEUE_NAME = "test_pool"

local CNN = {
  Driver   = IS_WINDOWS and "SQLite3 ODBC Driver" or "SQLite3";
  Database = ":memory:";
}

-------------------------------------------------------------------------------
-- This is main thread that store pool and connections alive

local server_cli = odbcpool.client(QUEUE_NAME)
local rthread = odbcpool.reconnect_thread(server_cli, CNN)
rthread:start()

local env  = odbc.environment()

local connections = {}
for i = 1, CNN_COUNT do
  local cnn = odbc.assert(env:connection())
  connections[#connections+1] = cnn
  server_cli:reconnect(cnn)
end

-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- This is example for any client thread

local cli = odbcpool.client(QUEUE_NAME)

local HANDLE = connections[1]:handle()

local a,b,c = cli:acquire(1000, function(cnn)
  return true, 1, nil, 3
end)

assert(a == 1)
assert(b == nil)
assert(c == 3)

cli:acquire(1000, function(cnn)
  -- disconnect but do not notify about that queue
  assert(HANDLE == cnn:handle())
  assert(cnn:connected())
  assert(cnn:first_value("select 'hello'") == 'hello')
  cnn:disconnect()
end)

cli:acquire(1000, function(cnn)
  -- queue does not reconnect connection
  -- tell queue that we need reconnect connection
  assert(HANDLE == cnn:handle())
  assert(not cnn:connected())
  return false
end)

cli:acquire(1000, function(cnn)
  -- test that connection was reconnected
  assert(HANDLE == cnn:handle())
  assert(cnn:connected())
  assert(cnn:first_value("select 'hello'") == 'hello')
end)

-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
-- Stop reconnect thread

rthread:stop()
-------------------------------------------------------------------------------

print("Done!")

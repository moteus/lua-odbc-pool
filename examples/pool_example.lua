local odbc     = require "odbc"
local odbcpool = require "odbc.pool"

local QUEUE_NAME = "pool_test"

-------------------------------------------------------------------------------
-- This is main thread that store pool and connections alive

local server_cli = odbcpool.client(QUEUE_NAME)
local rthread = odbcpool.reconnect_thread(server_cli, 'emptydb', 'TestUser', 'sql')
rthread:start()

local env  = odbc.environment()

local connections = {}
for i = 1, 3 do
  local cnn = odbc.assert(env:connection())
  connections[#connections+1] = cnn
  server_cli:reconnect(cnn)
end

-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- This is example for any client thread

local cli = odbcpool.client(QUEUE_NAME)

cli:acquire(function(cnn)
  print(cnn:first_value("select 'Hello, '"), cnn:first_value("select 'world'"))
end)

-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
-- Stop reconnect thread

rthread:stop()

-------------------------------------------------------------------------------

local odbc     = require "odbc"
local odbcpool = require "odbc.pool"
require "lzmq.pool".init(2)

-------------------------------------------------------------------------------
-- This is main thread that store pool and connections alive

local server_cli = odbcpool.client(1, 2)
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

local cli = odbcpool.client(1, 2)

cli:acquire(function(cnn)
  print(cnn:first_value("select 'Hello, '"), cnn:first_value("select 'world'"))
end)

-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
-- Stop reconnect thread

rthread:stop()

-------------------------------------------------------------------------------

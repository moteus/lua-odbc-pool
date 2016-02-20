lua-odbc-pool
=============

ODBC connections pool

[![Build Status](https://travis-ci.org/moteus/lua-odbc-pool.png?branch=master)](https://travis-ci.org/moteus/lua-odbc-pool)

This library allows use same ODBC connections from different threads/states.
Also this library supports asyncronus reconnection to database in separate thread.
This library based on [LUQ](https://github.com/moteus/lua-luq), [lzmq](https://github.com/moteus/lzmq), [lua-llthreads2](https://github.com/moteus/lua-llthreads2) and [lua-odbc](https://github.com/moteus/lua-odbc) libraries.
Note. This library may does not work with original `lua-llthreads` library.


##Usage

``` Lua
--
-- Client thread
--

local odbcpool = require "odbc.dba.pool"

local QUEUE_NAME = "MYDB"

local cli = odbcpool.client(QUEUE_NAME)

cli:acquire(function(cnn)
  -- if this function raise error or return `false` then
  -- connection will put to reconnection queue

  print(cnn:first_value("select 'Hello, '"), cnn:first_value("select 'world'"))

  -- assume driver supports `connected` option
  return not not cnn:connected()
end)
```

```Lua
--
-- Server thread
--

local odbc     = require "odbc"
local odbcpool = require "odbc.pool"

local QUEUE_NAME = "MYDB"

-- Create client to work with odbc.pool
local cli = odbcpool.client(QUEUE_NAME)

-- Create and start reconnect work thread
-- Here we specify connection options
local rthread = odbcpool.reconnect_thread(cli, 'emptydb', 'TestUser', 'sql')
rthread:start()


-- Create ODBC connections and put them to reconnect queue
local env = odbc.environment()

local connections = {}
for i = 1, 3 do
  local cnn = odbc.assert(env:connection())
  connections[#connections+1] = cnn
  cli:reconnect(cnn)
end

-- Here we wait until end of application.
-- We just need keep alive `rthread`, `env` and `connections` variables

-- Suppose we run this code in coroutine then we can just yield
-- and main thread resume us when application should be closed

coroutine.yield()

-- Now we can stop reconnect work thread
rthread:stop()
```

###Benchmark

You can run benchmark tool form `test` dir.

```
$ lua bench_pool.lua -c 2 -t 20 -q 500 -d 10 -w 50
```

Here we allocate 2 connections and run 20 threads.
Each thread call `acquire` method 500 times and do 
query with avg duration 10 msec. Each acquire wait 
at most 50 msec. Benchmark do not call actually sql
query but do sleep to emulate it.

**Result:**
```
Input data:
  Connections         : 2
  Threads             : 20
  Total query         : 10000 (500 per thread)
  Avg query duration  : 10[msec]
  Max wait            : 50[msec]
Result:
  Elapsed time        : 43.45[sec]
  Real query duration : 81.74[msec]
  Fail                : 2416 (24.16%)
```

Here we see that 24% of call `acquire` fails with timeout.
And wiait+execute of query take about 81 msec.

[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/moteus/lua-odbc-pool/trend.png)](https://bitdeli.com/free "Bitdeli Badge")


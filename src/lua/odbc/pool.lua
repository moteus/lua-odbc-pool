local string   = require "string"
local odbc     = require "odbc.dba"
local zmq      = require "lzmq"
local zpool    = require "lzmq.pool.core"
local zthreads = require "lzmq.threads"

-------------------------------------------------------------------------------
-- Client side
local client = {} do 
client.__index = client

function client:new(work_id, reconnect_id)
  assert(type(work_id) == 'number')
  assert(type(reconnect_id) == 'number')
  assert(work_id > 0)
  assert(reconnect_id > 0)
  assert(work_id ~= reconnect_id)

  local o = setmetatable({
    _private = {
      work_id      = work_id - 1;
      reconnect_id = reconnect_id - 1;
    }
  }, self)

  return o
end

function client:work_queue_id()
  return self._private.work_id
end

function client:reconnect_queue_id()
  return self._private.reconnect_id
end

function client:reconnect(cnn)
  cnn:disconnect()
  zpool.put(self._private.reconnect_id, (cnn:handle()))
end

function client:put(cnn)
  local h = cnn:handle()
  cnn:reset_handle(h) -- to close all statements
  zpool.put(self._private.work_id, h)
end

function client:get(cnn, timeout)
  local work_id = self._private.work_id
  local h
  if timeout then
    h = zpool.get_timeout(work_id, timeout)
  else
    h = zpool.get(work_id)
  end

  if h == 'timeout' then return nil, 'timeout' end

  assert(type(h) == 'userdata')

  if cnn then
    odbc.assert(cnn:reset_handle(h))
  else
    cnn = odbc.assert(odbc.init_connection(h))
    cnn:setautoclosestmt(true)
  end

  return cnn
end

function client:check_and_put(cnn, check)
  check = check or cnn.connected
  if check(cnn) then self:put(cnn)
  else self:reconnect(cnn) end
end

local function acquire_return(self, cnn, ok, connected, ...)
  self._private.temp_cnn = cnn

  if not ok then
    self:check_and_put(cnn)
    return error(tostring(connected))
  end

  if connected == false then self:reconnect(cnn) else self:put(cnn) end

  return ...
end

function client:acquire(timeout, cb)
  if not cb then cb, timeout = timeout, nil end

  assert((timeout == nil) or (type(timeout) == 'number'))
  assert(cb) -- cb is callable

  local cnn, err = self:get(self._private.temp_cnn, timeout)
  if not cnn then return nil, err end
  
  -- to allow recursion
  -- i do not think recursion is very useful
  self._private.temp_cnn = nil
  return acquire_return(self, cnn, pcall(cb, cnn))
end

end
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Background thread
local reconnect_thread = {} do
reconnect_thread.__index = reconnect_thread

local function reconnect_thread_proc(pipe, wait_on, put_to, ...)
  local odbc   = require "odbc"
  local zmq    = require "lzmq"
  local ztimer = require "lzmq.timer"
  local zpool  = require "lzmq.pool.core"

  local timeout, cnn = 5000, nil

  local function interrupted()
    local msg, err = pipe:recvx(zmq.DONTWAIT)
    if msg and msg == 'FINISH' then return true end
    if (not err) or (err:mnemo() ~= 'EAGAIN') then return true end
  end

  local next_cnn do
    local cnn function next_cnn()
      local h = zpool.get_timeout(wait_on, timeout)
      if h ~= 'timeout' then
        assert(type(h) == 'userdata')
        if cnn then cnn:reset_handle(h) else cnn = odbc.init_connection(h) end
        return cnn
      end
    end
  end

  while true do
    if interrupted() then break end
    local cnn = next_cnn()
    if cnn then
      cnn:disconnect()
      local hcnn = cnn:handle()
      local ok, err = cnn:connect(...)
      if ok then 
        zpool.put(put_to, hcnn)
      else
        zpool.put(wait_on, hcnn)
        if interrupted() then break end
        ztimer.sleep(5000)
      end
    end
  end
end

function reconnect_thread:new(cli, ...)
  assert(cli and (getmetatable(cli) == client))
  local ctx = zmq.assert(zmq.context())
  local reconnect_id, work_id = cli:reconnect_queue_id(), cli:work_queue_id()
  local thread, pipe = zthreads.fork(
    ctx, string.dump(reconnect_thread_proc),
    reconnect_id, work_id, ...
  )
  if not thread then
    ctx:destroy()
    assert(thread, pipe)
  end

  local o = setmetatable({
    _private = {
      cli    = cli;
      ctx    = ctx;
      thread = thread;
      pipe   = pipe;
    }
  },self)

  return o
end

function reconnect_thread:start()
  assert(self._private.thread:start(true, true))
  return self
end

function reconnect_thread:stop()
  local _private = self._private
  self._private = nil
  _private.pipe:send("FINISH")
  _private.ctx:destroy(5000)
  return _private.thread:join()
end

end
-------------------------------------------------------------------------------

return {
  client = function(...) return client:new(...) end;
  reconnect_thread = function(...) return reconnect_thread:new(...) end;
}

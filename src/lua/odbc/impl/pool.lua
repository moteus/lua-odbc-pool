return function(odbc)
local string   = require "string"
local luq      = require "luq"
local zthreads = require "lzmq.threads"

local LUQ_QUEUE_PREFIX = "odbc/pool/"

-------------------------------------------------------------------------------
-- Client side
local client = {} do 
client.__index = client

function client:new(id)
  assert(id)

  local work_id      = LUQ_QUEUE_PREFIX .. id
  local reconnect_id = "{RECONNECT}" .. work_id

  work_q      = luq.queue(work_id)
  reconnect_q = luq.queue(reconnect_id)

  local o = setmetatable({
    _private = {
      work_id      = work_id;
      reconnect_id = reconnect_id;
      work_q       = work_q;
      reconnect_q  = reconnect_q;
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
  self._private.reconnect_q:put((cnn:handle()))
end

function client:put(cnn)
  local h = cnn:handle()
  cnn:reset_handle(h) -- to close all statements
  self._private.work_q:put(h)
end

function client:get(cnn, timeout)
  local work_q = self._private.work_q
  local h
  if timeout then
    h = work_q:get_timeout(timeout)
  else
    h = work_q:get()
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
  local luq    = require "luq"

  local wait_q, put_q = luq.queue(wait_on), luq.queue(put_to)

  local logger_ctor -- = [[return print]]

  local function load_logger()
    if logger_ctor then
      local l = assert((loadstring or load)(logger_ctor))
      return assert(l())
    end
    return function() end
  end

  local log = load_logger()

  local timeout = 5000

  local function interrupted()
    local msg, err = pipe:recvx(zmq.DONTWAIT)
    log("I: thread recv: " .. (msg or tostring(err)))
    if msg and msg == 'FINISH' then return true end
    if (not err) or (err:mnemo() ~= 'EAGAIN') then return true end
  end

  local next_cnn do
    local cnn function next_cnn()
      local h = wait_q:get_timeout(timeout)
      if h ~= 'timeout' then
        assert(type(h) == 'userdata')
        log("I: thread select new connection: " .. tostring(h))
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
      local ok, err 
      if type(...) == 'table' then
        ok, err = cnn:driverconnect(...)
      else
        ok, err = cnn:connect(...)
      end
      if ok then 
        log("I: connection " .. tostring(cnn:handle()) .. " pass")
        put_q:put(hcnn)
      else
        log("I: connection " .. tostring(cnn:handle()) .. " fail: " .. tostring(err))
        wait_q:put(hcnn)
        if interrupted() then break end
        ztimer.sleep(timeout)
      end
    end
  end
end

function reconnect_thread:new(cli, ...)
  assert(cli and (getmetatable(cli) == client))
  local reconnect_id, work_id = cli:reconnect_queue_id(), cli:work_queue_id()
  local actor, err = zthreads.xactor(
    reconnect_thread_proc,
    reconnect_id, work_id, ...
  )
  assert(actor, err)

  actor:set_sndtimeo(1000)

  local o = setmetatable({
    _private = {
      cli    = cli;
      actor  = actor;
    }
  },self)

  return o
end

function reconnect_thread:start()
  assert(self._private.actor:start(true, true))
  return self
end

function reconnect_thread:stop()
  local _private = self._private
  self._private = nil
  _private.actor:send("FINISH")
  _private.actor:close()
  return true
end

end
-------------------------------------------------------------------------------

return {
  client = function(...) return client:new(...) end;
  reconnect_thread = function(...) return reconnect_thread:new(...) end;
}

end

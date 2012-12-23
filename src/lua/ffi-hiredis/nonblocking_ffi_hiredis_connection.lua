--------------------------------------------------------------------------------
--- Non-blocking ffi-hiredis connection.
-- @module ffi-hiredis.nonblocking_ffi_hiredis_connection
-- This file is a part of ffi-hiredis library
-- @copyright ffi-hiredis authors (see file `COPYRIGHT` for the license)
--------------------------------------------------------------------------------

local log, dbg, spam, log_error
      = import 'lua-aplicado/log.lua' { 'make_loggers' } (
          "ffi-hiredis/nonblocking_ffi_hiredis_connection", "NFH"
        )

--------------------------------------------------------------------------------

local error, select, setmetatable, tonumber, tostring
    = error, select, setmetatable, tonumber, tostring

--------------------------------------------------------------------------------

local ffi = require 'ffi'

--------------------------------------------------------------------------------

local arguments,
      optional_arguments,
      method_arguments
      = import 'lua-nucleo/args.lua'
      {
        'arguments',
        'optional_arguments',
        'method_arguments'
      }

local is_number
      = import 'lua-nucleo/type.lua'
      {
        'is_number'
      }

local assert_is_string
      = import 'lua-nucleo/typeassert.lua'
      {
        'assert_is_string'
      }

local make_generator_mt,
      invariant
      = import 'lua-nucleo/functional.lua'
      {
        'make_generator_mt',
        'invariant'
      }

local unique_object
      = import 'lua-nucleo/misc.lua'
      {
        'unique_object'
      }

--------------------------------------------------------------------------------

local ffi_hiredis = import 'ffi-hiredis/ffi.lua' ()

--------------------------------------------------------------------------------

local ffi_gc_if_not_null = function(obj, gc_handler)
  if obj ~= nil then
    return ffi.gc(obj, gc_handler)
  end

  return obj
end

--------------------------------------------------------------------------------

local HIREDIS_NIL = setmetatable(
    { type = ffi_hiredis.REDIS_REPLY_NIL, name = "NIL" },
    {
      __metatable = "HIREDIS_NIL";
      __newindex = function() error("HIREDIS_NIL is read-only") end;
    }
  )

local HIREDIS_WOULD_BLOCK = unique_object()

--------------------------------------------------------------------------------

-- TODO: Looks like it will make more sense to use redisReader
--       and our own socket. Non-blocking hiredis API is too undocumented.
local make_nonblocking_ffi_hiredis_connection
do
  local get_connection
  do
    -- Our connection must be persistent.
    -- If we can't connect, we fail with error.
    -- TODO: Must we fail with error?
    -- TODO: Allow several retries?
    local connect = function(host, port)
      arguments(
          "string", host,
          "number", port
        )

      local conn = ffi_gc_if_not_null(
          ffi_hiredis.redisConnectNonBlock(host, port), -- Note NonBlock
          ffi_hiredis.redisFree
        )
      if conn == nil then -- NULL returned
        -- TODO: Use human-readable string
        log_error("can't connect to", host, port, "errno", ffi.errno)
        error("redisConnect failed, errno: " .. ffi.errno)
      end
      if conn.err ~= 0 then
        log_error(
            "can't connect to", host, port, "error:", ffi.string(conn.errstr)
          )
        error("redisConnect failed: " .. ffi.string(conn.errstr))
      end

      log("connected to", host, port)

      return conn
    end

    get_connection = function(self)
      method_arguments(self)

      if not self.conn_ then
        self.conn_ = connect(self.host_, self.port_)
      end

      return self.conn_
    end
  end

  local handle_reply
  do
    local reply_handlers = { }
    do
      local create_constobject_cache = function(type)
        return setmetatable(
            { },
            make_generator_mt(function(name)
              return { type = type, name = name }
            end)
          )
      end

      local create_constobject_reply_handler = function(type)
        local cache = create_constobject_cache(type)

        return function(reply)
          return cache[ffi.string(reply.str, reply.len)]
        end
      end

      reply_handlers[ffi_hiredis.REDIS_REPLY_STATUS] =
        create_constobject_reply_handler(ffi_hiredis.REDIS_REPLY_STATUS)

      reply_handlers[ffi_hiredis.REDIS_REPLY_ERROR] =
        create_constobject_reply_handler(ffi_hiredis.REDIS_REPLY_ERROR)

      reply_handlers[ffi_hiredis.REDIS_REPLY_NIL] = invariant(HIREDIS_NIL)

      reply_handlers[ffi_hiredis.REDIS_REPLY_STRING] = function(reply)
        return ffi.string(reply.str, reply.len)
      end

      reply_handlers[ffi_hiredis.REDIS_REPLY_INTEGER] = function(reply)
        -- Needs conversion from FFI number to Lua number
        return tonumber(reply.integer)
      end

      reply_handlers[ffi_hiredis.REDIS_REPLY_ARRAY] = function(reply)
        local r = { }

        -- Doh. Zero-based C array, be careful.
        for i = 0, reply.elements - 1 do
          -- Note that r is 1-based.
          local nested_reply = reply.element[i]
          r[#r + 1] = reply_handlers[nested_reply.type](nested_reply)
        end

        return r
      end
    end

    handle_reply = function(conn, reply)
      if reply == nil then
        if conn.err ~= 0 then
          log_error("can't handle reply:", ffi.string(conn.errstr))
          return nil, "hiredis error: " .. ffi.string(conn.errstr)
        end

        return HIREDIS_WOULD_BLOCK
      end

      return reply_handlers[reply.type](reply)
    end
  end

  local pack_argv
  do
    local argv_t = ffi.typeof([[const char * [?] ]])
    local argvlen_t = ffi.typeof([[const size_t [?] ]])

    pack_argv = function(...)
      local nargs = select("#", ...)
      local argv = { }
      local argvlen = { }

      for i = 1, nargs do
        local v = select(i, ...)
        if is_number(v) then
          v = tostring(v)
        end
        assert_is_string(v)
        argv[i] = v
        argvlen[i] = #v
      end

      return nargs, argv_t(nargs, argv), argvlen_t(nargs, argvlen)
    end
  end

  local append_command_impl = function(conn, ...)
    local status = ffi_hiredis.redisAppendCommandArgv(
        conn,
        pack_argv(...)
      )

    if status ~= ffi_hiredis.REDIS_OK then
      log_error("can't append command:", ffi.string(conn.errstr))
      return nil, "hiredis error: " .. ffi.string(conn.errstr)
    end

    return true
  end

  local get_reply_impl
  do
    local flush_write_buffer
    do
      local doneness_t = ffi.typeof([[int [1] ]])

      -- TODO: Shouldn't we select on fd on write if we're not done yet?
      flush_write_buffer = function(conn)
        local done = doneness_t()
        done[0] = 0

        -- Assuming hiredis is sane, so no endless-loop protection
        while done[0] == 0 do
          local status = ffi_hiredis.redisBufferWrite(conn, done)
          if status ~= ffi_hiredis.REDIS_OK then
            log_error("can't write to buffer:", ffi.string(conn.errstr))
            return nil, "hiredis error: " .. ffi.string(conn.errstr)
          end
        end

        return true
      end
    end

    local reply_ptr_t = ffi.typeof [[redisReply * [1] ]]

    get_reply_impl = function(conn)
      local res, err = flush_write_buffer(conn)
      if res == nil then
        return nil, err
      end

      local status = ffi_hiredis.redisBufferRead(conn)
      if status ~= ffi_hiredis.REDIS_OK then
        log_error("can't read from buffer:", ffi.string(conn.errstr))
        return nil, "hiredis error: " .. ffi.string(conn.errstr)
      end

      local reply_ptr = reply_ptr_t()

      local status = ffi_hiredis.redisGetReply(conn, reply_ptr)
      if status ~= ffi_hiredis.REDIS_OK then
        -- No need to free reply object, it hasn't been initialized.
        log_error("can't get reply:", ffi.string(conn.errstr))
        return nil, "hiredis error: " .. ffi.string(conn.errstr)
      end

      return handle_reply(
          conn,
          ffi_gc_if_not_null(reply_ptr[0], ffi_hiredis.freeReplyObject)
        )
    end
  end

  local get_reply = function(self)
    method_arguments(self)

    return get_reply_impl(get_connection(self))
  end

  -- Note that redisCommand is not used,
  -- as it makes little sense in non-blocking mode.
  local command = function(self, ...)
    method_arguments(self)

    local conn = get_connection(self)

    local res, err = append_command_impl(conn, ...)
    if res == nil then
      return nil, err
    end

    return get_reply_impl(conn)
  end

  local append_command = function(self, ...)
    method_arguments(self)

    return append_command_impl(get_connection(self), ...)
  end

  local get_fd = function(self)
    method_arguments(self)

    return get_connection(self).fd
  end

  -- Note that connection would be reopened on next operation.
  local close = function(self)
    method_arguments(self)

    if self.conn_ ~= nil then
      -- Force connection collection
      ffi_hiredis.redisFree(self.conn_)
      ffi.gc(self.conn_, nil) -- We've just collected it, disable GC.

      self.conn_ = nil
    end
  end

  -- TODO: Support UDS
  make_nonblocking_ffi_hiredis_connection = function(host, port)
    arguments(
        "string", host,
        "number", port
      )

    return
    {
      command = command;
      append_command = append_command;
      get_reply = get_reply;
      close = close;
      --
      get_fd = get_fd;
      --
      conn_ = nil;
      host_ = host;
      port_ = port;
    }
  end
end

--------------------------------------------------------------------------------

return
{
  HIREDIS_NIL = HIREDIS_NIL;
  HIREDIS_WOULD_BLOCK = HIREDIS_WOULD_BLOCK;
  --
  make_nonblocking_ffi_hiredis_connection
    = make_nonblocking_ffi_hiredis_connection;
}

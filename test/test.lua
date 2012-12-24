--------------------------------------------------------------------------------
--- Tests for ffi-hiredis
-- This file is a part of ffi-hiredis library
-- @copyright ffi-hiredis authors (see file `COPYRIGHT` for the license)
--------------------------------------------------------------------------------
-- TODO: Write proper tests, this is more like an usage example
--------------------------------------------------------------------------------

assert(jit, "LuaJIT 2.0.0+ only")

--------------------------------------------------------------------------------

if (...) ~= "--luarocks" then
  -- Preferring working copy to luarocks tree.
  package.path = "src/lua/?.lua;" .. package.path
end

--------------------------------------------------------------------------------

pcall(require, 'luarocks.require') -- Ignoring errors

--------------------------------------------------------------------------------

require 'lua-nucleo.module'
require 'lua-nucleo.strict'

require = import 'lua-nucleo/require_and_declare.lua' { 'require_and_declare' }

--------------------------------------------------------------------------------

do
  local LOG_LEVEL,
        LOG_FLUSH_MODE,
        FLUSH_SECONDS_DEFAULT,
        wrap_file_sink,
        make_common_logging_config
        = import 'lua-nucleo/log.lua'
        {
          'LOG_LEVEL',
          'LOG_FLUSH_MODE',
          'FLUSH_SECONDS_DEFAULT',
          'wrap_file_sink',
          'make_common_logging_config'
        }

  import 'lua-aplicado/log.lua' { 'create_common_logging_system' } (
      "",
      wrap_file_sink(io.stdout),
      make_common_logging_config(
          {
            [LOG_LEVEL.ERROR] = true;
            [LOG_LEVEL.LOG]   = true;
            [LOG_LEVEL.DEBUG] = true;
            [LOG_LEVEL.SPAM]  = true;
          },
          { },
          LOG_FLUSH_MODE.ALWAYS,
          FLUSH_SECONDS_DEFAULT
        )
    )
end

--------------------------------------------------------------------------------

local log, dbg, spam, log_error
      = import 'lua-aplicado/log.lua' { 'make_loggers' } (
          "ffi-hiredis/test", "TEST"
        )

--------------------------------------------------------------------------------

-- sudo luarocks install https://raw.github.com/justincormack/ljsyscall/master/rockspec/ljsyscall-scm-1.rockspec
local syscall = require 'syscall'

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

local make_nonblocking_ffi_hiredis_connection,
      HIREDIS_NIL,
      HIREDIS_WOULD_BLOCK
      = import 'ffi-hiredis/nonblocking_ffi_hiredis_connection.lua'
      {
        'make_nonblocking_ffi_hiredis_connection',
        'HIREDIS_NIL',
        'HIREDIS_WOULD_BLOCK'
      }

--------------------------------------------------------------------------------

local test = function()
  log("NIL is", tostring(HIREDIS_NIL))
  log("WOULD_BLOCK is", HIREDIS_WOULD_BLOCK)

  local conn = make_nonblocking_ffi_hiredis_connection("127.0.0.1", 6379)
  log("connected")

  local keys = conn:command("KEYS", "*")
  log("keys", keys)

  if keys == HIREDIS_WOULD_BLOCK then
    log("selecting on", conn:get_fd())
    local fds = syscall.select({ readfds = { conn:get_fd() } })
    log("selected", fds)

    log("fetching data")
    log("keys is", conn:get_reply())
    log("after fetch")
  end

  log("set", conn:append_command("SET", "FFI_HIREDIS", 1))
  log("ac")

  log("before gr")
  local data = conn:get_reply()
  log("data", data)
  log("after gr")

  if data == HIREDIS_WOULD_BLOCK then
    log("selecting on", conn:get_fd())
    local fds = syscall.select({ readfds = { conn:get_fd() } })
    log("selected", fds)

    log("fetching data")
    log("data is", conn:get_reply())
    log("after fetch")
  end

  log("before empty get reply")
  log("gr", conn:get_reply())
  log("after empty get reply")

  log("imitating slow redis")

  log("clearing the list")
  local data = conn:command("DEL", "FFI_HIREDIS")
  log("data", data)

  if data == HIREDIS_WOULD_BLOCK then
    log("selecting on", conn:get_fd())
    local fds = syscall.select({ readfds = { conn:get_fd() } })
    log("selected", fds)

    log("fetching data")
    log("data is", conn:get_reply())
    log("after fetch")
  end

  log("forking")
  local pid = syscall.fork()
  if pid == 0 then
    log("child")
    log("child sleeping")
    require 'socket'.sleep(3)
    log("child sleept")
    local conn = make_nonblocking_ffi_hiredis_connection("127.0.0.1", 6379)
    log("child connected")
    local data = conn:command("LPUSH", "FFI_HIREDIS", "DATA")
    log("child data", data)

    if data == HIREDIS_WOULD_BLOCK then
      log("child selecting on", conn:get_fd())
      local fds = syscall.select({ readfds = { conn:get_fd() } })
      log("child selected", fds)

      log("child fetching data")
      log("child data is", conn:get_reply())
      log("child after fetch")
    end

    log("leaving child")
    os.exit(0, true)
  else
    log("parent, child's pid", pid)
    log("blocking on BLPOP")

    local data = conn:command("BLPOP", "FFI_HIREDIS", 0)
    log("data", data)

    if data == HIREDIS_WOULD_BLOCK then
      log("selecting on", conn:get_fd())
      local fds = syscall.select({ readfds = { conn:get_fd() } })
      log("selected", fds)

      log("fetching data")
      log("data is", conn:get_reply())
      log("after fetch")
    end
  end

  log("OK")
end

--------------------------------------------------------------------------------

test()

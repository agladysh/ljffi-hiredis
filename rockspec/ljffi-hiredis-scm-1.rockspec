package = "ljffi-hiredis"
version = "scm-1"
source =
{
  url = "git://github.com/agladysh/ljffi-hiredis.git";
  branch = "master";
}
description =
{
  summary = "hiredis bindings to LuaJIT FFI";
  homepage = "https://github.com/agladysh/ljffi-hiredis/";
  license = "MIT/X11";
}
dependencies =
{
  "lua == 5.1"; -- In fact this should be "luajit >= 2.0.0"
  "lua-nucleo >= 0.0.6";
  "lua-aplicado >= 0.0.2";
}
build =
{
  type = "none";
  install =
  {
    lua =
    {
      ["ffi-hiredis.ffi"] = "src/lua/ffi-hiredis/ffi.lua";
      ["ffi-hiredis.nonblocking_ffi_hiredis_connection"]
        = "src/lua/ffi-hiredis/nonblocking_ffi_hiredis_connection.lua";
    };
  };
}

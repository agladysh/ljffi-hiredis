ljffi-hiredis: hiredis bindings to LuaJIT FFI
=============================================

See the copyright information in the file named `COPYRIGHT`.

Helpful links:

https://github.com/redis/hiredis
https://github.com/agladysh/lua-hiredis

Prebuilt `libhiredis.so` is bundled for your convenience in `bin/hiredis/`.
Put it to some place that `ffi.load("hiredis")` knows about.

Sorry, no further docs on this point. Read the source
(start with `test/test.lua`).

See also the `TODO` file.

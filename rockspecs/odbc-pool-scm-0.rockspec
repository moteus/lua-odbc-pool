package = "odbc-pool"
version = "scm-0"
source = {
  url = "https://github.com/moteus/lua-odbc-pool/archive/master.zip",
  dir = "lua-odbc-pool-master",
}

description = {
  summary = "ODBC connections pool",
  detailed = [[This library allows use same ODBC connections from different threads/states.]],
  homepage = "https://github.com/moteus/lua-odbc-pool",
  license  = "MIT/X11",
}

dependencies = {
  "lua >= 5.1, < 5.3",
  "odbc",
  "luq",
  -- "lzmq" or "lzmq-ffi",
  -- "lua-llthreads2" or "lua-llthreads2-compat" or "lua-llthreads",
}

build = {
  type = "builtin",
  copy_directories = {},

  modules = {
    [ "odbc.pool"      ] = "src/lua/odbc/pool.lua",
    [ "odbc.impl.pool" ] = "src/lua/odbc/impl/pool.lua",
    [ "odbc.dba.pool"  ] = "src/lua/odbc/dba/pool.lua",
  }
}

/// chdb_spec: avro-cpp's public Config.hh includes <intsafe.h> on Windows only to get SSIZE_T for its
/// `typedef SSIZE_T ssize_t`. mingw-w64's intsafe.h includes wtypesbase.h and rpc.h, which define
/// macros such as `interface` (as `struct`) and `small`, and those break every ClickHouse source that
/// includes an avro header (`Interface interface` in ClientInfo.h). basetsd.h defines SSIZE_T alone.
/// This directory is on the include path only for avro and the code that uses it.
#pragma once
#include <basetsd.h>

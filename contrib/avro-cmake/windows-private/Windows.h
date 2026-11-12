/// chdb_spec: avro-cpp's FileStream.cc includes "Windows.h", the spelling MSVC's case-insensitive file
/// systems accept. The mingw-w64 header is windows.h, and a cross-build from Linux looks it up
/// case-sensitively.
#pragma once
#include <windows.h>

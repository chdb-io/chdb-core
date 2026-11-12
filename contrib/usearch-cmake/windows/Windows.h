/// chdb_spec: what usearch's Windows branch needs from a mingw-w64 cross-build.
///
/// usearch includes <Windows.h>, the spelling MSVC's case-insensitive file systems accept. The
/// mingw-w64 header is windows.h, and a cross-build from Linux looks it up case-sensitively.
#pragma once
#include <windows.h>

/// ClickHouse's patch to usearch's memory-mapping arena (index_plugins.hpp) compares the result of
/// page_allocator_t::allocate with the POSIX MAP_FAILED in code shared with Windows, where that
/// allocator is VirtualAlloc and returns NULL on failure. POSIX's value never matches it.
#ifndef MAP_FAILED
#define MAP_FAILED ((void *)-1)
#endif

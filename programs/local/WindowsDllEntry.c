/// chdb_spec: the entry point of _chdb.pyd on Windows (see programs/CMakeLists.txt).
///
/// When a process exits, Windows first terminates every thread but the exiting one and only then
/// detaches the DLLs; mingw-w64's DllMainCRTStartup then runs the module's C++ static destructors
/// and atexit functions. With the engine still running (an open connection, then os._exit, which
/// tests/run_all.py's workers use) those destructors wait for pool threads that no longer exist,
/// and the process never ends. POSIX _exit skips them, so do the same: on a process-terminating
/// detach (reserved != NULL) return without running the CRT's detach. The normal exit path is not
/// affected: Python's atexit has already shut the engine down while its threads were alive (see
/// chdb_cleanup_at_exit), and the process frees the memory the skipped destructors would have.
/// FreeLibrary (reserved == NULL) and every other event go through the CRT as usual.

#include <windows.h>

BOOL WINAPI DllMainCRTStartup(HANDLE instance, DWORD reason, LPVOID reserved);
BOOL WINAPI chdbDllMainCRTStartup(HANDLE instance, DWORD reason, LPVOID reserved);

BOOL WINAPI chdbDllMainCRTStartup(HANDLE instance, DWORD reason, LPVOID reserved)
{
    if (reason == DLL_PROCESS_DETACH && reserved != NULL)
        return TRUE;
    return DllMainCRTStartup(instance, reason, reserved);
}

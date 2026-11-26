#pragma once

namespace CHDB
{

/// Records that the embedded ClickHouse engine has started initializing in the
/// current process and installs the POSIX pthread_atfork child handler once.
/// Must be called when the engine actually boots (EmbeddedServer / LocalServer),
/// never on library load, so a fork() before first engine init keeps working.
void markEngineInitialized() noexcept;

/// Returns true when the current process is a POSIX fork() child created after
/// the embedded ClickHouse engine was already initialized in the parent.
/// Lock-free and safe to call before acquiring any mutexes or during teardown.
bool isForkedAfterEngineInit() noexcept;

/// Static, allocation-free error message returned when a forked child attempts
/// to use chDB after the engine was initialized in the parent process.
const char * forkedAfterEngineInitError() noexcept;

}

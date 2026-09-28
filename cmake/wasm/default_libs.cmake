# Default libraries for the Emscripten/WebAssembly target.
#
# Unlike the other platforms, this one does not use `-nodefaultlibs` and does not build its own
# compiler-rt, libc++, libc++abi or libunwind: the Emscripten sysroot already provides all of
# them, along with a musl-derived libc and a pthread implementation on top of Web Workers.
# Overriding any of it would mean rebuilding the sysroot, which is what Emscripten exists to
# avoid. So this file only recreates the one CMake target the rest of the tree expects.
#
# The `-pthread` and `-sMEMORY64` flags are ABI flags and are applied to every target in
# `cmake/target.cmake`, which runs before this file. chdb makes threads optional there, so the
# tag below is conditional: with WASM_THREADS=OFF this is a no-op interface target and the
# bundle carries no SharedArrayBuffer dependency.

add_library (Threads::Threads INTERFACE IMPORTED)
if (WASM_THREADS)
    set_target_properties (Threads::Threads PROPERTIES INTERFACE_COMPILE_OPTIONS "-pthread")
endif ()

message (STATUS "Default libraries: provided by the Emscripten sysroot (threads=${WASM_THREADS})")

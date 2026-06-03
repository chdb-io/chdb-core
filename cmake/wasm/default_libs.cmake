# Default libraries for the Emscripten/WebAssembly target.
#
# Unlike the Linux build, we do NOT use -nodefaultlibs or build our own
# compiler-rt / libc++ / libunwind: the Emscripten sysroot already provides a
# musl-based libc, libc++, libc++abi, compiler-rt and a pthread emulation layer.
# We only need to recreate the handful of CMake targets the rest of the tree
# expects (Threads::Threads), and wire threading flags.

# Threading: emscripten implements pthreads on top of Web Workers + SharedArrayBuffer.
# For the initial MVP build we default to single-threaded (no -pthread); the
# Threads::Threads target is still required by many contrib libs (abseil, etc.),
# so create it as a no-op interface. Flip WASM_THREADS=ON to enable real threads.
option (WASM_THREADS "Build the WASM target with pthreads (requires SharedArrayBuffer)" OFF)

add_library(Threads::Threads INTERFACE IMPORTED)
if (WASM_THREADS)
    set_target_properties(Threads::Threads PROPERTIES INTERFACE_COMPILE_OPTIONS "-pthread")
    add_compile_options(-pthread)
    add_link_options(-pthread -sUSE_PTHREADS=1 -sPTHREAD_POOL_SIZE=4)
endif ()

# The Emscripten toolchain links its own standard libraries; leave
# CMAKE_*_STANDARD_LIBRARIES untouched and do not include unwind.cmake / cxx.cmake.
message(STATUS "WASM default libraries: provided by Emscripten sysroot (threads=${WASM_THREADS})")

# Default libraries for the Emscripten/WebAssembly target.
#
# Unlike the Linux build, we do NOT use -nodefaultlibs or build our own
# compiler-rt / libc++ / libunwind: the Emscripten sysroot already provides a
# musl-based libc, libc++, libc++abi, compiler-rt and a pthread emulation layer.
# We only need to recreate the handful of CMake targets the rest of the tree
# expects (Threads::Threads), and wire threading flags.

# Threading: emscripten implements pthreads on top of Web Workers + SharedArrayBuffer.
# The WASM_THREADS option and the actual -pthread / pool-size flags are declared in
# cmake/target.cmake's OS_WASM block (compile/link) and programs/wasm/CMakeLists.txt
# (link); that block runs before this file. Here we only recreate the
# Threads::Threads target the rest of the tree expects (abseil, etc.), tagging it
# with -pthread when threads are enabled so contrib libs compile consistently. With
# WASM_THREADS=OFF it is a no-op interface (single-threaded, no SharedArrayBuffer).
add_library(Threads::Threads INTERFACE IMPORTED)
if (WASM_THREADS)
    set_target_properties(Threads::Threads PROPERTIES INTERFACE_COMPILE_OPTIONS "-pthread")
endif ()

# The Emscripten toolchain links its own standard libraries; leave
# CMAKE_*_STANDARD_LIBRARIES untouched and do not include unwind.cmake / cxx.cmake.
message(STATUS "WASM default libraries: provided by Emscripten sysroot (threads=${WASM_THREADS})")

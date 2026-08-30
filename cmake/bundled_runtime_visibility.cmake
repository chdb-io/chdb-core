# Hidden visibility for the bundled LLVM runtimes (libc++, libc++abi, libunwind).
#
# `libchdb.so` / `libchdb.dylib` gate their exports at link time: `--version-script` with
# chdb/libchdb_export.map on Linux, `-exported_symbols_list` with
# chdb/libchdb_export_macos.txt on macOS. `libchdb.a` has no link step of its own, so the
# equivalent gate has to be baked into the objects at compile time.
#
# Without it, ld on a modern macOS deployment target turns the runtime's weak definitions
# into `<weak-def-coalesce>` fixups, and dyld is free to bind them to the system
# libc++/libc++abi rather than the bundled copy. Runtime state then straddles two runtimes
# and the first C API call that touches it hangs or aborts.
#
# `-fvisibility=hidden` on its own is not enough: the LLVM runtimes annotate their ABI
# symbols `__attribute__((visibility("default")))`, which beats the command-line default.
# The `_LIBCPP_DISABLE_VISIBILITY_ANNOTATIONS` / `_LIBCXXABI_DISABLE_VISIBILITY_ANNOTATIONS`
# / `_LIBUNWIND_HIDE_SYMBOLS` macros switch those annotations off; each target sets its own
# next to its other definitions.
#
# Everything is PRIVATE. The annotation-disabling macros must not reach targets that merely
# consume the libc++ headers, or their inline code would be compiled against a different
# visibility ABI than the runtime it links with.
#
# Scope is deliberately three targets on one platform. A project-wide `-fvisibility=hidden`
# duplicates inline statics such as `Context::global_context_instance` per translation unit
# and breaks singleton identity - see the CFI block in the root CMakeLists.txt for the
# empirical write-up.

if (OS_DARWIN AND CHDB_STATIC_LIBRARY_BUILD)
    set (HIDE_BUNDLED_RUNTIME ON)
else ()
    set (HIDE_BUNDLED_RUNTIME OFF)
endif ()

# Replaceable global operator new/delete are emitted default-visible no matter what
# `-fvisibility` says; hiding them needs a dedicated flag. Clang 18 renamed it and
# deprecated the old spelling (which is now a `-Wdeprecated` error under `-Werror`).
# Build-time flag probes are blocked project-wide (cmake/block_build_time_checks.cmake),
# so this is a version gate; the project requires Clang >= 21 (cmake/tools.cmake), which
# makes the older branch defensive only.
if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 18)
    set (GLOBAL_NEW_DELETE_HIDDEN_FLAG "-fvisibility-global-new-delete=force-hidden")
else ()
    set (GLOBAL_NEW_DELETE_HIDDEN_FLAG "-fvisibility-global-new-delete-hidden")
endif ()

# Apply the visibility flags to one bundled runtime target. Callers guard on
# HIDE_BUNDLED_RUNTIME and add that target's own annotation-disabling macros.
function (hide_bundled_runtime target)
    # Assembly sources take no `-fvisibility` (they honour the macros instead, via
    # libunwind's assembly.h), so restrict the flags to the compiled languages.
    target_compile_options (${target} PRIVATE
        "$<$<COMPILE_LANGUAGE:C,CXX>:-fvisibility=hidden>"
        "$<$<COMPILE_LANGUAGE:C,CXX>:${GLOBAL_NEW_DELETE_HIDDEN_FLAG}>")
endfunction ()

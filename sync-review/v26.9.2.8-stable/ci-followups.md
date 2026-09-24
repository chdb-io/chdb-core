# CI follow-ups for the v26.9.2.8-stable sync

One root cause per entry, and per commit. These commits stay separate from the two baseline
commits so the failure and its fix can be read together later.

## 1. libchdb.so does not link: local-exec TLS relocations from inline assembly

**Failure.** `Build & Test Free-Threading Wheels (Linux x86_64)`, step *Build & test FT wheels*,
run 35937391236. Hundreds of identical errors, then the linker gave up:

```
ld.lld-21: error: relocation R_X86_64_TPOFF32 against FiberLocalStorageThreadStorage
                  cannot be used with -shared
>>> defined in src/libclickhouse_common_io.a(FiberLocal.cpp.o)
>>> referenced by ThreadStatusExt.cpp
ld.lld-21: error: too many errors emitted, stopping now
```

**Root cause.** `src/Common/FiberLocal.h` is new in v26.9. Its `load()`/`store()` fast path
reaches thread-local storage from inline assembly that names the TLS symbol itself:

```
movq %fs:FiberLocalStorageThreadStorage@tpoff+N, %rax      # x86_64
add x0, x0, :tprel_hi12:FiberLocalStorageThreadStorage+N   # aarch64
```

Those spellings *are* the local-exec model. A shared object cannot carry them, because the
offset from the thread pointer is not known until the program is linked. Upstream only builds
executables, so upstream never sees it; every chdb artifact is `libchdb.so`, or an archive a
consumer may link into one.

`-ftls-model=global-dynamic` does not help, and neither does the `USE_MUSL` branch in
`CMakeLists.txt` that sets it: the relocation is written into the assembly, not chosen by the
compiler from the TLS model.

**Fix.** The header already carries the portable branch — `currentSlots()[slot]`, a `noinline`
accessor with a memory barrier, which gives the same "do not hoist this across a fiber switch"
guarantee the assembly was written for. `CMakeLists.txt` defines
`CHDB_PORTABLE_FIBER_LOCAL_TLS` in the chdb_spec section and the two `#if`s in `FiberLocal.h`
test for it, so upstream's structure survives and the next sync's diff stays two lines.

**Why the local checks missed it.** The local matrix built the Linux **static** library and ran
its Go cgo consumer, which links an executable — `-shared` never happened, so the relocation
was legal every time. macOS is Mach-O and takes the `#else` branch already. No local
configuration linked an ELF shared object, which is the only one that fails.

**What catches it next time.** Build `libchdb.so` on Linux, not only `libchdb.a`: they are
different link modes and only one of them rejects these relocations. Cheaper still, and what
was used to confirm this root cause before touching the tree: compile the TLS-touching code
`-fPIC` and link it `-shared` on its own. It reproduces in seconds, on either architecture.

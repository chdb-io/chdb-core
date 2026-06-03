# chdb WASM 支持改造方案

目标：将 chdb 编译为 WebAssembly，在浏览器 / Node 中运行。产物为 `libchdb.wasm` + JS 胶水，**砍掉 Python 绑定层**，只暴露 `programs/local/chdb.h` 的 C API。

工具链：Emscripten（emcc/em++，基于 Clang）。本地开发与 CI 全程用 Node.js 测试，无需浏览器；浏览器验收用 headless Chrome。

总量估计：人类工程师 **14–18 周**；最难的是 Phase 3（ClickHouse 核心 syscall stub）。

---

## 一、构建系统（CMake）

### 1.1 平台检测

| 文件 | 行 | 改动 |
|---|---|---|
| `cmake/target.cmake` | 1–20 | 新增 `elseif (CMAKE_SYSTEM_NAME MATCHES "Emscripten")` → 定义 `OS_WASM`，`add_definitions(-D OS_WASM)` |
| `cmake/target.cmake` | 36–80 | 交叉编译禁用块新增 `OS_WASM` 分支，禁用列表见下 |
| `PreLoad.cmake` | 80–115 | Linux toolchain 选择块旁加 WASM 分支：当 `EMSCRIPTEN` 时指向新 toolchain 文件，跳过 musl/hermetic 逻辑 |
| `cmake/emscripten-toolchain.cmake` | 新建 | 链 emsdk 自带 `Emscripten.cmake`，追加 chdb flags：`-sALLOW_MEMORY_GROWTH=1 -sMAXIMUM_MEMORY=4GB -sWASM_BIGINT -pthread -sUSE_PTHREADS=1 -sPTHREAD_POOL_SIZE` |

### 1.2 编译开关禁用（抄 `cmake/target.cmake` 现有 RISC-V / LoongArch 模板）

```cmake
# OS_WASM 分支内
set (ENABLE_JEMALLOC OFF)        # WASM 无 jemalloc
set (ENABLE_GRPC OFF)
set (ENABLE_HDFS OFF)
set (ENABLE_MYSQL OFF)
set (ENABLE_LIBPQXX OFF)
set (ENABLE_RUST OFF)            # corrosion 不支持 wasm32 target（先关）
set (ENABLE_EMBEDDED_COMPILER OFF)  # LLVM JIT 不可用
set (ENABLE_DWARF_PARSER OFF)
set (ENABLE_LDAP OFF)
set (OPENSSL_NO_ASM ON)
set (ENABLE_NURAFT OFF)
set (ENABLE_KAFKA OFF)
set (ENABLE_AMQPCPP OFF)
set (ENABLE_CASSANDRA OFF)
set (ENABLE_AZURE_BLOB_STORAGE OFF)
set (ENABLE_S3 OFF)             # aws-c-* 依赖 socket
set (ENABLE_KRB5 OFF)
set (ENABLE_PARQUET OFF)        # 先关，Phase 2 末尝试开
```

### 1.3 顶层 CMakeLists.txt

| 行 | 改动 |
|---|---|
| 643–680 | jemalloc 块：已有 `if(NOT ENABLE_JEMALLOC)` 走 `USE_JEMALLOC=0`，确认 WASM 走此路径即可（基本无需改） |
| 216, 255, 262, 309, 479, 533 | `OS_LINUX` 专属块（binary 重映射、official build、split debug）需排除 WASM，加 `AND NOT OS_WASM` 或确保条件不命中 |
| `cmake/cpu_features.cmake` 55–105 | x86/ARM 特性检测块加 WASM 分支：仅 `-msimd128`，禁 SSE/AVX/NEON 检测 |

---

## 二、contrib 依赖（263 个目录，按风险分档）

### A 档 — 预期可直接编（约 1 周）
`zlib-ng`（已带 wasm32 检测）、`abseil-cpp`、`fmtlib`、`re2`、`xz`、`zstd`、`lz4`、`brotli`、`bzip2`、`cityhash102`、`cctz`、`SimSIMD`（带 wasm32）、`croaring`、`base64`、`double-conversion`、`miniselect`、`pdqsort`

### B 档 — 需手动 patch（约 1.5 周）
| 库 | 问题 |
|---|---|
| `openssl` | 走 `OPENSSL_NO_ASM`，验证 RNG 用 `getentropy` |
| `c-ares` | DNS：stub 或禁用 |
| `protobuf` / `capnproto` | 通常可编，注意 codegen host 工具 |
| `arrow` | 仅编 core，砍 flight/网络；Parquet 后置 |
| `icu` | 体积大，data 需打包进 VFS |
| `boost` | 仅 header + context（协程 fiber 在 wasm 需 asyncify，见 Phase 3） |
| `libcxx`/`libcxxabi` | emscripten 自带，改用其 sysroot，**不要**编 chdb 内置版本 |

### C 档 — 直接禁用（开关已在第一节关闭，无需编译）
`grpc`、`aws-*`(20+ 目录)、`azure`、`libhdfs3`、`librdkafka`/`cppkafka`、`AMQP-CPP`、`NuRaft`、`cassandra`、`mariadb-connector-c`、`libpq`、`openldap`、`krb5`、`cyrus-sasl`、`libunwind`（用 emscripten 异常）、`jemalloc`、`corrosion`/rust 全家

---

## 三、ClickHouse 核心 syscall stub（工作量大头）

WASM 无 fork/exec/signal/mmap(MAP_SHARED)/futex(部分)/dlopen/网络。需逐文件加 `#if defined(OS_WASM)` 分支。

### 3.1 线程与并发
| 文件 | 行/规模 | 改动 |
|---|---|---|
| `src/Common/ThreadPool.cpp` | 962 行 | emscripten pthread 可用，但主线程不能阻塞 join。审查 `scheduleImpl`/`finalize`，WASM 下降级：`max_threads=1` 同步执行，或确保仅在 worker 中 join |
| `src/Common/ThreadPool.h` | — | 同上，编译期默认线程数 |
| `src/Common/SharedMutex.h` | 5 | `#ifdef OS_LINUX`(futex) 块需 WASM fallback 到 `std::shared_mutex` |
| `src/Common/CurrentThread.*`、`setThreadName.cpp` | 7–120 | 加 WASM 分支（`pthread_setname_np` 在 emscripten 是 no-op，可直接走 Linux 路径或 stub） |
| `src/Common/FiberStack.*` | — | 协程栈：WASM 需 `-sASYNCIFY` 或改同步；这是潜在阻塞点 |

### 3.2 内存映射
| 文件 | 改动 |
|---|---|
| `src/Common/MMap*` / `MMappedFile*` | `MAP_SHARED` 不可用，WASM 下 fallback 到 `pread` 全量读入堆 |
| `src/IO/MMapReadBufferFromFile*` | 同上 fallback |
| `src/Common/remapExecutable.cpp` | 已限 `OS_LINUX && __amd64__`，WASM 自动不命中，确认即可 |

### 3.3 进程 / 信号 / 守护
| 文件 | 行 | 改动 |
|---|---|---|
| `src/Daemon/BaseDaemon.cpp` | 840 | 信号处理、coredump、StackTrace 注册全 stub（chdb 嵌入模式本就不需要 daemon，加 `OS_WASM` 整体短路） |
| `src/Common/ShellCommand.cpp` | 412 | fork/execvp → WASM 全部 `throw NOT_IMPLEMENTED` |
| `src/Common/StackTrace.cpp` | 750 | unwind/backtrace stub，返回空栈 |
| `src/Common/EnvironmentChecks.cpp` | — | CPU 指令集运行期检查，WASM 直接通过 |
| `src/Client/ReplxxLineReader.cpp` | — | 交互式 readline 不需要，禁用（chdb 走 query API） |

### 3.4 系统信息 / 监控（多为 `OS_LINUX` 专属，WASM 走 stub/默认值）
| 文件 | 改动 |
|---|---|
| `src/Common/ProcfsMetricsProvider.*` | `/proc` 读取，WASM stub 返回 0 |
| `src/Common/MemoryStatisticsOS.*` | 同上 |
| `src/Common/AsynchronousMetrics.*` (127–135) | OS 指标，WASM 缩减实现 |
| `src/Common/QueryProfiler.cpp` (38–141) | 基于 timer signal，WASM 禁用 |
| `src/Common/OSThreadNiceValue.cpp` | nice 调度，no-op |
| `src/Common/Epoll.h`、`EventFD.h`、`TimerDescriptor.cpp` | Linux 专属 fd，WASM 不命中或 stub |
| `src/Common/CgroupsMemoryUsageObserver.cpp` | cgroup，WASM 禁用 |
| `src/Common/getHashOfLoadedBinary.cpp`、`SymbolIndex.cpp` | 读自身 ELF，WASM stub |
| `src/Common/MemoryWorker.*` (21) | jemalloc purge 线程，`ENABLE_JEMALLOC=OFF` 后自动短路 |

### 3.5 存储 / 网络表函数
- 禁用 `StorageURL`、`StorageS3`、`StorageHDFS`、`StorageKafka`、`StorageMySQL`、`StoragePostgreSQL` 等（随第一节开关关闭）
- 保留 `StorageMemory`、`StorageFile`（走 emscripten VFS / MEMFS）、`numbers`/`system.*`
- `src/Functions/` 默认可编；个别用 inline asm 的（如 `__builtin_cpu_supports`）需 WASM 守卫

### 3.6 文件系统
- emscripten 提供 MEMFS（内存）/ IDBFS（IndexedDB 持久化）/ NODEFS（Node 本地 fs）
- 数据导入走 JS 侧 `FS.writeFile` 注入，再用 `file()` 表函数查询
- ICU / timezone data 需在链接期 `--embed-file` 打进 VFS

---

## 四、入口层改造（programs/local/）

### 4.1 砍掉 Python（35 个文件耦合 pybind11 / Python.h）
`programs/local/CMakeLists.txt`：WASM 下走 `NOT USE_PYTHON` 分支（已存在）。排除全部 `Python*.cpp`、`Numpy*.cpp`、`Pandas*.cpp`、`PyArrow*.cpp`、`LocalChdb.cpp`、`ChdbPyType.cpp`、`PybindWrapper.cpp`、`AIQueryProcessor.cpp` 等。

保留链路：
```
chdb.cpp → ChdbClient.cpp → EmbeddedServer.cpp → LocalServer.cpp
+ chdb-arrow.cpp / ArrowStreamSource / StorageArrowStream / TableFunctionArrowStream
```

### 4.2 link 目标（`programs/local/CMakeLists.txt`）
WASM 静态库只 link：
`clickhouse_common_io`、`clickhouse_common_config`、`clickhouse_parsers`、`clickhouse_functions`、`clickhouse_aggregate_functions`、`clickhouse_storages_system`、`boost::program_options`。
**去掉** `clickhouse_table_functions` 中的网络部分（或编精简版）。

### 4.3 入口审查
| 文件 | 改动 |
|---|---|
| `programs/local/EmbeddedServer.cpp` | chdb 常驻 server，审 fs/getenv/HOME/网络假设，加 WASM 默认路径 |
| `programs/local/LocalServer.cpp` | mmap 缓存、信号、终端检测加 WASM 分支 |
| `programs/local/chdb.h` (445) | 无需改，作为 WASM export 边界 |

---

## 五、新增：WASM 绑定与打包（新建 programs/wasm/）

| 文件 | 内容 |
|---|---|
| `programs/wasm/chdb_wasm.cpp` | `EMSCRIPTEN_BINDINGS`，把 `chdb_query_to_buffer` / `chdb_result_*` 包成 JS 可调用 |
| `programs/wasm/CMakeLists.txt` | 链 `libchdb_wasm.a`，emcc link flags，`--embed-file` 打包 tz/icu data |
| `programs/wasm/index.ts` | TS 封装：`query(sql, format)` Promise API |
| `programs/wasm/package.json` | npm 包骨架 |
| `tests/wasm/*.test.ts` | Node + vitest：`SELECT 1`、`numbers(1000)` 过滤、聚合、JOIN、Arrow 输出 |

---

## 六、本地 Linux 测试

| 方式 | 阶段 | 命令 |
|---|---|---|
| Node.js（主开发） | Phase 1–4 | `node chdb.mjs -e "SELECT 1"`，线程加 `--experimental-wasm-threads` |
| Wasmtime/Wasmer | 验证脱浏览器依赖 | WASI 子集跑核心 |
| Headless Chrome / Playwright | Phase 4 验收 | `chromium --headless --enable-features=SharedArrayBuffer` |

CI 全程 Node，无需浏览器。SharedArrayBuffer 需 COOP/COEP header（部署时配置）。

---

## 七、阶段与工期

| 阶段 | 内容 | 人类工期 |
|---|---|---|
| Phase 0 | 产物决策 + 脚手架 + emsdk Clang 版本核对 | 0.5 周 |
| Phase 1 | CMake 平台分支 + toolchain + 禁用列表（一、1.5 节） | 1.5 周 |
| Phase 2 | contrib 子集 WASM 化（二节） | 3–4 周 |
| Phase 3 | ClickHouse 核心 syscall stub + libchdb（三、四节） | 4–6 周 |
| Phase 4 | WASM 绑定 + 端到端验收（五、六节） | 1.5–2 周 |
| **合计** | | **10.5–14 周**，+30% 风险缓冲 → **14–18 周** |

---

## 八、关键风险

1. **emsdk Clang 版本** vs `cmake/tools.cmake` 要求的 Clang 21——可能需放宽版本要求。Phase 0 必须先实测。
2. **协程 fiber**（`FiberStack`）：ClickHouse pipeline 用协程，WASM 需 `-sASYNCIFY`（拖慢 + 增体积）或改同步执行——Phase 3 最大不确定项。
3. **线程模型**：pthread 需 SharedArrayBuffer，部署受 COOP/COEP 限制；最坏退化为单线程，性能 ~原生 30–50%。
4. **体积**：估计 30–100 MB（gzip 8–25 MB），远大于 DuckDB-WASM(~10MB)/sql.js(~3MB)。
5. **上游 bug**：emscripten + ClickHouse 组合可能从未被测试，可能触发非 chdb 代码内可修的 LLVM/emscripten bug，卡在外部依赖。

## 九、建议的低成本验证（先做）
1. Phase 0 + 1（1–2 天）：能否 cmake configure 通过。
2. Spike：单独把 `clickhouse_common_io`（核心里最小、依赖最少的库）编出 `.wasm`。能编通过 → 路径可行；编不过 → 改走 REST 服务方案（chdb 后端 + JS 客户端）。

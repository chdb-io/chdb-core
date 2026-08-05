include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
include(CMakePrintHelpers)

add_headers_and_sources(clickhouse_functions_array .)

if (CHDB_LITE)
    # chdb embedded build: drop arrayDistance/distanceTransposed/arrayNormalizedGini/arrayDotProduct/arrayLevenshtein
    # plus niche array/map helpers (mapOp, arrayCumSum, arrayFold, ...).
    message(STATUS "CHDB_LITE: dropping niche array / vector-distance / map functions")
    foreach (_drop arrayDistance.cpp distanceTransposed.cpp arrayNormalizedGini.cpp arrayDotProduct.cpp arrayLevenshtein.cpp mapPopulateSeries.cpp mapOp.cpp arrayShingles.cpp arrayJaccardIndex.cpp arrayShiftRotate.cpp arrayCumSum.cpp arrayCumSumNonNegative.cpp arrayDifference.cpp arrayFold.cpp arrayAUC.cpp arrayRandomSample.cpp arrayShuffle.cpp arrayEnumerateDense.cpp arrayEnumerateDenseRanked.cpp arrayEnumerateUniqRanked.cpp indexOfAssumeSorted.cpp)
        list(REMOVE_ITEM clickhouse_functions_array_sources "${_drop}" "./${_drop}")
    endforeach()
    set(_chdb_vec_stub "${CMAKE_CURRENT_BINARY_DIR}/chdbVectorStubs.cpp")
    file(WRITE "${_chdb_vec_stub}"
        "// chdb stubs: vector helpers throw at call site (functions register but error when invoked)\n"
        "#include <Functions/IFunction.h>\n"
        "#include <Interpreters/Context_fwd.h>\n"
        "#include <Common/Exception.h>\n"
        "namespace DB {\n"
        "namespace ErrorCodes { extern const int NOT_IMPLEMENTED; }\n"
        "#define CHDB_VEC_STUB { throw Exception(ErrorCodes::NOT_IMPLEMENTED, \"chdb: vector functions disabled at build time\"); }\n"
        "FunctionPtr createFunctionArrayL1Distance(ContextPtr) CHDB_VEC_STUB\n"
        "FunctionPtr createFunctionArrayL2Distance(ContextPtr) CHDB_VEC_STUB\n"
        "FunctionPtr createFunctionArrayL2SquaredDistance(ContextPtr) CHDB_VEC_STUB\n"
        "FunctionPtr createFunctionArrayLpDistance(ContextPtr) CHDB_VEC_STUB\n"
        "FunctionPtr createFunctionArrayLinfDistance(ContextPtr) CHDB_VEC_STUB\n"
        "FunctionPtr createFunctionArrayCosineDistance(ContextPtr) CHDB_VEC_STUB\n"
        "FunctionPtr createFunctionArrayL2DistanceTransposed(ContextPtr) CHDB_VEC_STUB\n"
        "FunctionPtr createFunctionArrayCosineDistanceTransposed(ContextPtr) CHDB_VEC_STUB\n"
        "FunctionPtr createFunctionArrayDotProduct(ContextPtr) CHDB_VEC_STUB\n"
        "FunctionPtr createFunctionArrayDotProductTransposed(ContextPtr) CHDB_VEC_STUB\n"
        "FunctionPtr createFunctionArrayL2DistanceTransposedQuantized(ContextPtr) CHDB_VEC_STUB\n"
        "FunctionPtr createFunctionArrayCosineDistanceTransposedQuantized(ContextPtr) CHDB_VEC_STUB\n"
        "FunctionPtr createFunctionArrayDotProductTransposedQuantized(ContextPtr) CHDB_VEC_STUB\n"
        "}\n")
    list(APPEND clickhouse_functions_array_sources "${_chdb_vec_stub}")
endif()

# createInternalFunctionHasOverloadResolver, FunctionArrayResize,  FunctionEmptyArrayToSingle
# Used by ArrayExistsToHasPass.cpp and ArrayJoinAction.cpp
set(DBMS_ARRAY_FUNCTIONS
    arrayResize.cpp
    has.cpp
    emptyArrayToSingle.cpp
)
extract_into_parent_list(clickhouse_functions_array_sources dbms_sources ${DBMS_ARRAY_FUNCTIONS})
extract_into_parent_list(clickhouse_functions_headers dbms_headers arrayResize.h has.h emptyArrayToSingle.h)

add_library(clickhouse_functions_array OBJECT ${clickhouse_functions_array_sources} ${clickhouse_functions_array_headers})
target_link_libraries(clickhouse_functions_array PRIVATE dbms clickhouse_functions_gatherutils ch_contrib::stringzilla)

if (TARGET ch_contrib::vectorscan)
    target_link_libraries(clickhouse_functions_array PRIVATE ch_contrib::vectorscan)
endif()

# Libcxx hardening means out-of-bounds checks which causes noticeable degradations for some workflows.
# TUs where libcxx hardening needs to be disabled are specified here.
set_source_files_properties(array.cpp PROPERTIES COMPILE_FLAGS "-U_LIBCPP_HARDENING_MODE -D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_NONE")

# TODO: at v3 the SLP vectorizer widens the per-row accumulator loops in arrayNorm and
# arrayDistance to 256-bit YMM (collapses to a single accumulator on the BF16 paths,
# regressing `LinfNorm`/`L1Distance`/`LinfDistance` ~7-15%).  Compiling at v2 keeps
# 128-bit codegen; the AVX-512 specialisations carry function-level target attributes
# and override this file flag.
if (ARCH_AMD64 AND X86_ARCH_LEVEL VERSION_GREATER_EQUAL 3)
    set_source_files_properties(arrayNorm.cpp PROPERTIES COMPILE_FLAGS "-march=x86-64-v2")
    set_source_files_properties(arrayDistance.cpp PROPERTIES COMPILE_FLAGS "-march=x86-64-v2")
endif()

# The project sets `-ffp-contract=off` globally (see top-level CMakeLists.txt) for numerical determinism.
# Re-enable FMA fusion only for these two TUs: the dot-product and distance kernels rely on the compiler
# generating `vfmadd*` from `a*b + c` patterns in their hot inner loops. The per-file `COMPILE_FLAGS` are
# appended after `CMAKE_CXX_FLAGS`, so this override wins over the global `-ffp-contract=off`.
set_source_files_properties(arrayDotProduct.cpp PROPERTIES COMPILE_FLAGS "-ffp-contract=fast")
# Append so the conditional `-march=x86-64-v2` set above for arrayDistance.cpp / arrayNorm.cpp is preserved
# (`set_source_files_properties` has no APPEND form; use `set_property ... APPEND_STRING`).
set_property(SOURCE arrayDistance.cpp APPEND_STRING PROPERTY COMPILE_FLAGS " -ffp-contract=fast")
# `arrayNorm.cpp` carries an `x86_64_v4` (AVX-512) specialisation that fuses `a*b + c` into FMA on the
# `L2`/`L2Squared` paths; re-enable contraction here too (the default reduction stays at `v2`/128-bit).
set_property(SOURCE arrayNorm.cpp APPEND_STRING PROPERTY COMPILE_FLAGS " -ffp-contract=fast")

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(clickhouse_functions_array PRIVATE "-g1")
endif()

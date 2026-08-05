include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions .)

# Big-type comparison/arithmetic matrix trim (Int128/Int256/UInt128/UInt256/Decimal128/Decimal256/BFloat16)
# is gated in FunctionsComparison.h / FunctionBinaryArithmetic.h via
# `#if defined(CHDB_LITE) && CHDB_LITE` — umbrella's add_compile_definitions(CHDB_LITE=1) is enough.

if (CHDB_LITE)
    # chdb embedded build: drop heavy niche scalar functions (polygons, NumericIndexedVector, etc.).
    message(STATUS "CHDB_LITE: dropping niche heavy scalar functions")
    set(_chdb_drop_funcs
        # Polygon family - heavy boost::geometry templates, niche
        pointInPolygon.cpp
        polygonArea.cpp
        polygonConvexHull.cpp
        polygonPerimeter.cpp
        polygonsDistance.cpp
        polygonsEquals.cpp
        polygonsIntersect.cpp
        polygonsIntersection.cpp
        polygonsSymDifference.cpp
        polygonsUnion.cpp
        polygonsWithin.cpp
        readWktPolygon.cpp
        readWktMultiPolygon.cpp
        readWktLineString.cpp
        readWktMultiLineString.cpp
        readWktPoint.cpp
        readWktRing.cpp
        # NumericIndexedVector functions (sparse vector ops, niche)
        FunctionsNumericIndexedVector.cpp
        # Geographic (uses S2/H3)
        geoToS2.cpp
        s2ToGeo.cpp
        # Niche string formatting / encoding
        formatRow.cpp
        formatRowNoNewline.cpp
        formatReadableQuantity.cpp
        formatReadableTimeDelta.cpp
        # Niche arithmetic helpers
        avg2.cpp
        midpoint.cpp
        # ULID (rarely used)
        generateULID.cpp
        ULIDStringToDateTime.cpp
        # Niche
        FunctionFromUnixTimestamp64.cpp
        widthBucket.cpp
        bitTestAny.cpp
        bitTestAll.cpp
        # IS [NOT] DISTINCT FROM — niche null-safe equality, big templated
        isDistinctFrom.cpp
        isNotDistinctFrom.cpp
        # OrZero / OrNull arithmetic variants
        intDivOrZero.cpp
        intDivOrNull.cpp
        moduloOrZero.cpp
        moduloOrNull.cpp
        positiveModuloOrNull.cpp
        # Niche hash functions: keep Misc (it has cityHash64, xxHash, MD5, sipHash...)
        # Drop only the truly niche RIPEMD/SSL/Murmur ones (~6 MB unstripped)
        FunctionsHashingRipe.cpp
        FunctionsHashingSSL.cpp
        FunctionsHashingMurmur.cpp
        # Locality-sensitive hashing (ngramSimHash, wordShingleSimHash, ngramMinHash...)
        # Niche near-duplicate detection. ~2 MB unstripped.
        FunctionsStringHash.cpp
        # Roaring-bitmap functions (bitmapBuild, bitmapAnd, etc.). They consume
        # AggregateFunction(groupBitmap) blobs, which we already drop via
        # CHDB_LITE drops it from the aggregate library too, so these are dead in chdb-core-lite.
        # ~2.5 MB unstripped.
        FunctionsBitmap.cpp
        # Niche string / encoding
        ascii.cpp
        soundex.cpp
        stem.cpp
        wkt.cpp
        readWkt.cpp
        # niche standalone functions
        FunctionNaiveBayesClassifier.cpp
        FunctionShowCertificate.cpp
        FunctionFile.cpp
        FunctionFQDN.cpp
        filesystem.cpp
        # Vector / array distance (mostly used in vector search)
        arrayDistance.cpp
        arrayDotProduct.cpp
        arrayNorm.cpp
        arrayJaccardIndex.cpp
        arrayLevenshtein.cpp
        arrayShingles.cpp
        arrayShiftRotate.cpp
        arrayCumSum.cpp
        arrayCumSumNonNegative.cpp
        arrayDifference.cpp
        arrayFold.cpp
        arrayAUC.cpp
        arrayRandomSample.cpp
        arrayShuffle.cpp
        arrayEnumerateDense.cpp
        arrayEnumerateDenseRanked.cpp
        arrayEnumerateUniqRanked.cpp
        # Map ops (niche)
        mapPopulateSeries.cpp
        mapOp.cpp
        # niche bit ops: shift / rotate (~6 MB unstripped)
        bitShiftLeft.cpp
        bitShiftRight.cpp
        bitRotateLeft.cpp
        bitRotateRight.cpp
        # niche math: gcd / lcm (~2.8 MB unstripped)
        gcd.cpp
        lcm.cpp
        # niche
        bitHammingDistance.cpp
        FunctionsBitToArray.cpp
        formatQuery.cpp
        domainWithoutWWW.cpp
        cutQueryStringAndFragment.cpp
        cutToFirstSignificantSubdomainCustom.cpp
        cutFragment.cpp
        cutQueryString.cpp
        cutToFirstSignificantSubdomain.cpp
        cutURLParameter.cpp
        cutWWW.cpp
        normalizeQuery.cpp
        normalizedQueryHash.cpp
        validateNestedArraySizes.cpp
        evalMLMethod.cpp
        decodeHTMLComponent.cpp
        encodeXMLComponent.cpp
        decodeXMLComponent.cpp
        timeSlots.cpp
        # Dictionary lookup functions: useful only with dictionaries (already trimmed); fail clearly when called.
        FunctionsExternalDictionaries.cpp
        FunctionDictGetKeys.cpp
        # AI SQL functions (aiClassify / aiExtract / aiGenerate / aiTranslate):
        # make outbound HTTP calls to OpenAI / Anthropic; orthogonal to the embedded SQL engine.
        # AI/ subdirectory (providers + quota tracker) is gated separately below.
        aiClassify.cpp
        aiExtract.cpp
        aiGenerate.cpp
        aiTranslate.cpp
        FunctionBaseAI.cpp
    )
    foreach (f IN LISTS _chdb_drop_funcs)
        list(REMOVE_ITEM clickhouse_functions_sources ${f})
    endforeach()
endif()

option(CHDB_MINIMAL_FUNCTIONS "Build with minimal SQL functions for chdb" OFF)

if (CHDB_MINIMAL_FUNCTIONS)
    message(STATUS "Building with MINIMAL SQL functions (whitelist mode)")
    set(clickhouse_functions_sources "")
    list(APPEND clickhouse_functions_sources
        "${CMAKE_CURRENT_SOURCE_DIR}/IFunction.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/IFunctionAdaptors.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/FunctionDynamicAdaptor.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/FunctionFactory.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/FunctionHelpers.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/FunctionsLogical.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/if.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/multiIf.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/multiMatchAny.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/checkHyperscanRegexp.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/CastOverloadResolver.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/FunctionsConversion.cpp"
        # impl00.cpp: FunctionConvert<UInt8> — the v26.3-era successor to the old
        # single-file impl0.cpp; needed for basic toUInt8 to link.
        "${CMAKE_CURRENT_SOURCE_DIR}/FunctionsConversion_impl00.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/FunctionsConversion_impl1.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/FunctionsConversion_impl2.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/FunctionsConversion_impl3.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/searchAnyAll.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/extractTimeZoneFromFunctionArguments.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/generateSnowflakeID.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/registerFunctions.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/FunctionGenerateRandomStructure.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/chdbVersion.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/serverConstants.cpp"
        # Comparison functions
        "${CMAKE_CURRENT_SOURCE_DIR}/equals.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/notEquals.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/less.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/lessOrEquals.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/greater.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/greaterOrEquals.cpp"
        # Arithmetic functions (needed for `SELECT 1+1` etc.)
        "${CMAKE_CURRENT_SOURCE_DIR}/plus.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/minus.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/multiply.cpp"
    )
endif()
if (NOT CHDB_LITE)
    # CHDB_LITE drops the AI SQL functions (aiClassify/aiExtract/aiGenerate/aiTranslate);
    # their provider/quota helpers in AI/ are only referenced from those .cpp files, so skip the subdir too.
    add_headers_and_sources(clickhouse_functions AI)
endif()

# These files are moved from clickhouse_functions into dbms to make dbms self-contained (not depend on clickhouse_functions)
# This allows less dependency and linker work (specially important when building many example executables)
set(DBMS_FUNCTIONS
    IFunction.cpp # IFunctionOverloadResolver::getLambdaArgumentTypes, IExecutableFunction::execute... (Many AST visitors, analyzer passes, some storages...)
    IFunctionAdaptors.cpp # FunctionToFunctionBaseAdaptor (Used by FunctionFactory.cpp)
    FunctionDynamicAdaptor.cpp # IFunctionOverloadResolver::getLambdaArgumentTypes, IExecutableFunction::execute... (Many AST visitors, analyzer passes, some storages...)
    FunctionVariantAdaptor.cpp # IFunctionOverloadResolver::build, IExecutableFunction::execute... (For Variant type support)
    FunctionFactory.cpp # FunctionFactory::instance() (Many AST visitors, analyzer passes, some storages...)
    FunctionHelpers.cpp # convertConstTupleToConstantElements, checkAndGetColumnConstStringOrFixedString, checkAndGetNestedArrayOffset ...)
    FunctionsLogical.cpp # createInternalFunctionAndOverloadResolve / createInternalFunctionOrOverloadResolver ... (Multiple)
    if.cpp # createInternalFunctionIfOverloadResolver (Used by MultiIfToIfPass.cpp)
    multiIf.cpp # createInternalMultiIfOverloadResolver (Used by IfChainToMultiIfPass.cpp)
    checkHyperscanRegexp.cpp # checkHyperscanRegexp (Used by MultiMatchAnyImpl.h, multiMatchAny.cpp)
    CastOverloadResolver.cpp # createInternalCast (Used by WindowTransform.cpp, KeyCondition.cpp...)
    FunctionsConversion.cpp # createFunctionBaseCast (Used by CastOverloadResolver.cpp)
    FunctionsConversion_impl00.cpp
    FunctionsConversion_impl01.cpp
    FunctionsConversion_impl02.cpp
    FunctionsConversion_impl03.cpp
    FunctionsConversion_impl04.cpp
    FunctionsConversion_impl05.cpp
    FunctionsConversion_impl06.cpp
    FunctionsConversion_impl07.cpp
    FunctionsConversion_impl08.cpp
    FunctionsConversion_impl09.cpp
    FunctionsConversion_impl10.cpp
    FunctionsConversion_impl11.cpp
    FunctionsConversion_impl12.cpp
    FunctionsConversion_impl13.cpp
    FunctionsConversion_impl14.cpp
    FunctionsConversion_impl15.cpp
    FunctionsConversion_impl16.cpp
    FunctionsConversion_impl17.cpp
    FunctionsConversion_impl18.cpp
    FunctionsConversion_impl19.cpp
    FunctionsConversion_impl20.cpp
    FunctionsConversion_impl21.cpp
    FunctionsConversion_impl22.cpp
    FunctionsConversion_impl23.cpp
    FunctionsConversion_impl24.cpp
    FunctionsConversion_impl25.cpp
    FunctionsConversion_impl26.cpp
    FunctionsConversion_impl27.cpp
    FunctionsConversion_impl28.cpp
    FunctionsConversion_impl1.cpp
    FunctionsConversion_impl2.cpp
    FunctionsConversion_impl3.cpp
    FunctionTopKFilter.cpp # createInternalFunctionTopKFilterResolver used from optimizeTopK.cpp
    productQuantization.cpp # createInternalFunctionPQDistanceResolver used from useVectorSearchWithQuantizedCodes.cpp
    vectorQuantization.cpp # createInternalFunctionQuantizeDistanceResolver used from useVectorSearchWithQuantizedCodes.cpp
    extractTimeZoneFromFunctionArguments.cpp # extractTimeZoneFromFunctionArguments (DateTimeTransforms.h, FunctionsConversion.cpp)
    generateSnowflakeID.cpp
    FunctionGenerateRandomStructure.cpp # FunctionGenerateRandomStructure::generateRandomDataType (Used by StorageGenerateRandom.cpp)
)
extract_into_parent_list(clickhouse_functions_sources dbms_sources ${DBMS_FUNCTIONS})
extract_into_parent_list(clickhouse_functions_headers dbms_headers
    IFunction.h
    FunctionFactory.h
    FunctionHelpers.h
    extractTimeZoneFromFunctionArguments.h
    FunctionsLogical.h
    CastOverloadResolver.h
)

add_library(clickhouse_functions_obj OBJECT ${clickhouse_functions_headers} ${clickhouse_functions_sources})
if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(clickhouse_functions_obj PRIVATE "-g1")
endif()

list (APPEND OBJECT_LIBS $<TARGET_OBJECTS:clickhouse_functions_obj>)

list (APPEND PUBLIC_LIBS
        ch_contrib::wyhash
        ch_contrib::cityhash
        ch_contrib::farmhash
        clickhouse_dictionaries
        clickhouse_dictionaries_embedded
        clickhouse_parsers
        ch_contrib::consistent_hashing
        common
        dbms
        ch_contrib::metrohash
        ch_contrib::murmurhash
        ch_contrib::morton_nd
)

list (APPEND PRIVATE_LIBS
        ch_contrib::zlib
        boost::filesystem
        ch_contrib::libdivide
        ch_contrib::xxHash
        ch_contrib::stringzilla
)

if (TARGET OpenSSL::Crypto)
    list (APPEND PUBLIC_LIBS OpenSSL::Crypto)
endif()

if (TARGET ch_contrib::icu)
    list (APPEND PRIVATE_LIBS ch_contrib::icu)
endif ()

if (TARGET ch_contrib::fastops)
    list (APPEND PRIVATE_LIBS ch_contrib::fastops)
endif ()

if (TARGET ch_contrib::llvm)
    list (APPEND PRIVATE_LIBS ch_contrib::llvm)
endif ()

if (TARGET ch_contrib::bech32)
    list (APPEND PRIVATE_LIBS ch_contrib::bech32)
endif()

if (TARGET ch_contrib::simdutf)
    list (APPEND PRIVATE_LIBS ch_contrib::simdutf)
endif()

if (ENABLE_NLP)
    list (APPEND PRIVATE_LIBS ch_contrib::cld2)
endif()

if (TARGET ch_contrib::sqids)
    list (APPEND PRIVATE_LIBS ch_contrib::sqids)
endif()

if (TARGET ch_contrib::idna)
    list (APPEND PRIVATE_LIBS ch_contrib::idna)
endif()

if (TARGET ch_contrib::h3)
    list (APPEND PRIVATE_LIBS ch_contrib::h3)
endif()

if (TARGET ch_contrib::utf8proc)
    list (APPEND PRIVATE_LIBS ch_contrib::utf8proc)
endif()

if (TARGET ch_contrib::wagyu)
    list (APPEND PRIVATE_LIBS ch_contrib::wagyu)
endif()

if (TARGET ch_contrib::vectorscan)
    list (APPEND PRIVATE_LIBS ch_contrib::vectorscan)
endif()

if (TARGET ch_contrib::simdjson)
    list (APPEND PRIVATE_LIBS ch_contrib::simdjson)
endif()

if (TARGET ch_contrib::rapidjson)
    list (APPEND PRIVATE_LIBS ch_contrib::rapidjson)
endif()

if (TARGET ch_contrib::pocketfft)
    list (APPEND PRIVATE_LIBS ch_contrib::pocketfft)
endif()

if (TARGET ch_contrib::crc32-vpmsum)
    list (APPEND PUBLIC_LIBS ch_contrib::crc32-vpmsum)
endif()

add_subdirectory(GatherUtils)
list (APPEND PRIVATE_LIBS clickhouse_functions_gatherutils)

add_subdirectory(URL)
list (APPEND OBJECT_LIBS $<TARGET_OBJECTS:clickhouse_functions_url>)

add_subdirectory(array)
list (APPEND OBJECT_LIBS $<TARGET_OBJECTS:clickhouse_functions_array>)

add_subdirectory(TimeSeries)
list (APPEND OBJECT_LIBS $<TARGET_OBJECTS:clickhouse_functions_time_series>)

if (TARGET ch_contrib::datasketches)
    add_subdirectory(UniqTheta)
    list (APPEND OBJECT_LIBS $<TARGET_OBJECTS:clickhouse_functions_uniqtheta>)
endif()

add_subdirectory(JSONPath)
list (APPEND PRIVATE_LIBS clickhouse_functions_jsonpath)

add_subdirectory(keyvaluepair)

# Signed integer overflow on user-provided data inside boost::geometry - ignore.
set_source_files_properties("pointInPolygon.cpp" PROPERTIES COMPILE_FLAGS -fno-sanitize=signed-integer-overflow)

# TODO: at v3 these wrapper TUs regress because the SLP vectorizer / SIMD-aware codegen
# fires inside the per-row dispatch (YMM scratch zero-init in `stringJaccardIndex`,
# YMM `ColumnString` prologue forcing `vzeroupper` around `randomString`'s `getUInt`,
# longer BMI2 encodings in `XXH3`'s short-input fast path crossing a 16-byte fetch boundary).
# Compiling at v2 restores master codegen; the actual AVX2 kernels they call live in other TUs.
if (ARCH_AMD64 AND X86_ARCH_LEVEL VERSION_GREATER_EQUAL 3)
    set_source_files_properties(FunctionsStringDistance.cpp PROPERTIES COMPILE_FLAGS "-march=x86-64-v2")
    set_source_files_properties(randomString.cpp PROPERTIES COMPILE_FLAGS "-march=x86-64-v2")
    set_source_files_properties(FunctionsHashingMisc.cpp PROPERTIES COMPILE_FLAGS "-march=x86-64-v2")
endif()

if (USE_GPERF)
    # Only for regenerating
    add_custom_target(generate-html-char-ref-gperf ./HTMLCharacterReference.sh
            SOURCES ./HTMLCharacterReference.sh
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            BYPRODUCTS "${CMAKE_CURRENT_SOURCE_DIR}/HTMLCharacterReference.gperf"
            )
    add_custom_target(generate-html-char-ref ${GPERF} -t HTMLCharacterReference.gperf --output-file=HTMLCharacterReference.generated.cpp
            && clang-format -i HTMLCharacterReference.generated.cpp
            # for clang-tidy, since string.h is deprecated
            && sed -i 's/\#include <string.h>/\#include <cstring>/g' HTMLCharacterReference.generated.cpp
            # the table must be constexpr for the compile-time entity-expansion static_assert in the .gperf
            && sed -i 's/static const struct NameAndGlyph wordlist/static constexpr struct NameAndGlyph wordlist/g' HTMLCharacterReference.generated.cpp
            SOURCES HTMLCharacterReference.gperf
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            )
    add_dependencies(generate-html-char-ref generate-html-char-ref-gperf)
    if (NOT TARGET generate-source)
        add_custom_target(generate-source)
    endif ()
    add_dependencies(generate-source generate-html-char-ref)
endif ()

# Update after all the includes
set(dbms_sources "${dbms_sources}" PARENT_SCOPE)
set(dbms_headers "${dbms_headers}" PARENT_SCOPE)

target_link_libraries(clickhouse_functions_obj PUBLIC ${PUBLIC_LIBS} PRIVATE ${PRIVATE_LIBS})

if (USE_STATIC_LIBRARIES OR NOT SPLIT_SHARED_LIBRARIES)
    # Used to forward the linking information to the final binaries such as clickhouse / unit_tests_dbms,
    # since such information are lost after we convert to OBJECT target
    add_library(clickhouse_functions INTERFACE)
    target_link_libraries(clickhouse_functions INTERFACE ${OBJECT_LIBS} ${PUBLIC_LIBS} ${PRIVATE_LIBS})
else()
    add_library(clickhouse_functions SHARED ${OBJECT_LIBS})
    target_link_libraries(clickhouse_functions PUBLIC ${PUBLIC_LIBS} PRIVATE ${PRIVATE_LIBS})
endif ()

if (CHDB_VERSION)
    add_compile_definitions(CHDB_VERSION_STRING="${CHDB_VERSION}")
endif ()

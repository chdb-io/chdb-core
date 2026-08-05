include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_aggregate_functions .)
add_headers_and_sources(clickhouse_aggregate_functions Combinators)
add_headers_and_sources(clickhouse_aggregate_functions TimeSeries)

if (CHDB_LITE)
    # chdb embedded build: drop niche / heavy aggregate functions to reduce binary size.
    message(STATUS "CHDB_LITE: dropping niche aggregates")
    # Each entry: <filename>;<exact register_fn name from registerAggregateFunctions.cpp>
    # Drop only files whose register fn name we know precisely; we generate stubs
    # so the central registerAggregateFunctions.cpp still links.
    # NOTE: do NOT drop AggregateFunctionUniqCombined<N>.cpp - they are template
    # specialisations referenced by a switch in AggregateFunctionUniqCombined.cpp.
    # Parallel lists: drop[i] is dropped, drop_reg[i] is its register fn name to stub
    set(_chdb_drop_files
        # Heavy quantile variants — keep Quantile, QuantileExact, QuantileTiming
        AggregateFunctionQuantileExactWeighted.cpp
        AggregateFunctionQuantileExactHigh.cpp
        AggregateFunctionQuantileExactLow.cpp
        AggregateFunctionQuantileExactExclusive.cpp
        AggregateFunctionQuantileExactInclusive.cpp
        AggregateFunctionQuantileInterpolatedWeighted.cpp
        AggregateFunctionQuantileBFloat16.cpp
        AggregateFunctionQuantileBFloat16Weighted.cpp
        AggregateFunctionQuantileTDigest.cpp
        AggregateFunctionQuantileTDigestWeighted.cpp
        AggregateFunctionQuantileDD.cpp
        AggregateFunctionQuantileDeterministic.cpp
        AggregateFunctionQuantileGK.cpp
        AggregateFunctionQuantileTimingWeighted.cpp
        AggregateFunctionGroupNumericIndexedVector.cpp
        AggregateFunctionGroupArrayMoving.cpp
        AggregateFunctionSparkbar.cpp
        AggregateFunctionAvgWeighted.cpp
        AggregateFunctionMLMethod.cpp
        AggregateFunctionMannWhitney.cpp
        AggregateFunctionStudentTTest.cpp
        AggregateFunctionWelchTTest.cpp
        AggregateFunctionRankCorrelation.cpp
        AggregateFunctionAnalysisOfVariance.cpp
        AggregateFunctionTheilsU.cpp
        AggregateFunctionContingencyCoefficient.cpp
        AggregateFunctionCramersV.cpp
        AggregateFunctionCramersVBiasCorrected.cpp
        AggregateFunctionCategoricalInformationValue.cpp
        AggregateFunctionSimpleLinearRegression.cpp
        AggregateFunctionLargestTriangleThreeBuckets.cpp
        AggregateFunctionRetention.cpp
        AggregateFunctionSequenceMatch.cpp
        AggregateFunctionSequenceNextNode.cpp
        AggregateFunctionEntropy.cpp
        AggregateFunctionDeltaSum.cpp
        AggregateFunctionDeltaSumTimestamp.cpp
        AggregateFunctionExponentialMovingAverage.cpp
        AggregateFunctionAggThrow.cpp
        AggregateFunctionFlameGraph.cpp
        AggregateFunctionBoundingRatio.cpp
        AggregateFunctionHistogram.cpp
        AggregateFunctionDistinctDynamicTypes.cpp
        AggregateFunctionGroupArrayInsertAt.cpp
        AggregateFunctionMaxIntersections.cpp
        AggregateFunctionKolmogorovSmirnovTest.cpp
        AggregateFunctionIntervalLengthSum.cpp
        # TimeSeries module: niche, used for Prometheus-style queries. Drop ~13 MB unstripped.
        TimeSeries/AggregateFunctionTimeseriesHelpers.cpp
        TimeSeries/AggregateFunctionLast2Samples.cpp
        TimeSeries/AggregateFunctionTimeSeriesGroupArray.cpp
        # uniqCombined precision variants: keep 17 (default), drop 12-16 and 18-20.
        # The switch in AggregateFunctionUniqCombined.cpp is gated by CHDB_LITE
        # to throw at runtime if a non-17 precision is requested.
        AggregateFunctionUniqCombined12.cpp
        AggregateFunctionUniqCombined13.cpp
        AggregateFunctionUniqCombined14.cpp
        AggregateFunctionUniqCombined15.cpp
        AggregateFunctionUniqCombined16.cpp
        AggregateFunctionUniqCombined18.cpp
        AggregateFunctionUniqCombined19.cpp
        AggregateFunctionUniqCombined20.cpp
        # Niche: groupArraySorted, groupBitmap*
        AggregateFunctionGroupArraySorted.cpp
        AggregateFunctionGroupBitmap.cpp
        # Bitwise aggregates (groupBitOr/And/Xor) — niche, ~1.1 MB unstripped
        AggregateFunctionBitwise.cpp
        # sumMap / minMap / maxMap variants — niche, ~1.2 MB unstripped
        AggregateFunctionSumMap.cpp
        # groupArrayIntersect — niche, ~1 MB unstripped
        AggregateFunctionGroupArrayIntersect.cpp
        # Extra niche aggregates: uniqUpTo (caps cardinality), quantilePrometheus,
        # studentTTestOneSample, varianceMatrix, anyHeavy, meanZTest, groupConcat,
        # distinctJSONPathsAndTypes, estimateCompressionRatio — ~3 MB unstripped total.
        AggregateFunctionUniqUpTo.cpp
        AggregateFunctionQuantilePrometheusHistogram.cpp
        AggregateFunctionStudentTTestOneSample.cpp
        AggregateFunctionVarianceMatrix.cpp
        AggregateFunctionAnyHeavy.cpp
        AggregateFunctionMeanZTest.cpp
        AggregateFunctionGroupConcat.cpp
        AggregateFunctionDistinctJSONPaths.cpp
        AggregateFunctionEstimateCompressionRatio.cpp
    )
    set(_chdb_drop_regs
        registerAggregateFunctionsQuantileExactWeighted
        registerAggregateFunctionsQuantileExactHigh
        registerAggregateFunctionsQuantileExactLow
        registerAggregateFunctionsQuantileExactExclusive
        registerAggregateFunctionsQuantileExactInclusive
        registerAggregateFunctionsQuantileInterpolatedWeighted
        registerAggregateFunctionsQuantileBFloat16
        registerAggregateFunctionsQuantileBFloat16Weighted
        registerAggregateFunctionsQuantileTDigest
        registerAggregateFunctionsQuantileTDigestWeighted
        registerAggregateFunctionsQuantileDD
        registerAggregateFunctionsQuantileDeterministic
        registerAggregateFunctionsQuantileApprox
        registerAggregateFunctionsQuantileTimingWeighted
        registerAggregateFunctionsNumericIndexedVector
        registerAggregateFunctionMoving
        registerAggregateFunctionSparkbar
        registerAggregateFunctionAvgWeighted
        registerAggregateFunctionMLMethod
        registerAggregateFunctionMannWhitney
        registerAggregateFunctionStudentTTest
        registerAggregateFunctionWelchTTest
        registerAggregateFunctionRankCorrelation
        registerAggregateFunctionAnalysisOfVariance
        registerAggregateFunctionTheilsU
        registerAggregateFunctionContingency
        registerAggregateFunctionCramersV
        registerAggregateFunctionCramersVBiasCorrected
        registerAggregateFunctionCategoricalIV
        registerAggregateFunctionSimpleLinearRegression
        registerAggregateFunctionLargestTriangleThreeBuckets
        registerAggregateFunctionRetention
        registerAggregateFunctionsSequenceMatch
        registerAggregateFunctionSequenceNextNode
        registerAggregateFunctionEntropy
        registerAggregateFunctionDeltaSum
        registerAggregateFunctionDeltaSumTimestamp
        registerAggregateFunctionExponentialMovingAverage
        registerAggregateFunctionAggThrow
        registerAggregateFunctionFlameGraph
        registerAggregateFunctionRate
        registerAggregateFunctionHistogram
        registerAggregateFunctionDistinctDynamicTypes
        registerAggregateFunctionGroupArrayInsertAt
        registerAggregateFunctionsMaxIntersections
        registerAggregateFunctionKolmogorovSmirnovTest
        registerAggregateFunctionIntervalLengthSum
        registerAggregateFunctionTimeseries
        registerAggregateFunctionLast2Samples
        registerAggregateFunctionTimeseriesGroupArray
        # UniqCombined precision drops have no register fns — emit dummy unused stubs
        _chdb_unused_uniqcombined_12_stub
        _chdb_unused_uniqcombined_13_stub
        _chdb_unused_uniqcombined_14_stub
        _chdb_unused_uniqcombined_15_stub
        _chdb_unused_uniqcombined_16_stub
        _chdb_unused_uniqcombined_18_stub
        _chdb_unused_uniqcombined_19_stub
        _chdb_unused_uniqcombined_20_stub
        registerAggregateFunctionGroupArraySorted
        registerAggregateFunctionsBitmap
        registerAggregateFunctionsBitwise
        registerAggregateFunctionSumMap
        registerAggregateFunctionGroupArrayIntersect
        registerAggregateFunctionUniqUpTo
        registerAggregateFunctionsQuantilePrometheusHistogram
        registerAggregateFunctionStudentTTestOneSample
        registerAggregateFunctionsVarianceMatrix
        registerAggregateFunctionAnyHeavy
        registerAggregateFunctionMeanZTest
        registerAggregateFunctionGroupConcat
        registerAggregateFunctionDistinctJSONPathsAndTypes
        registerAggregateFunctionEstimateCompressionRatio
    )
    list(LENGTH _chdb_drop_files _n)
    math(EXPR _last "${_n} - 1")
    set(_chdb_stub_decls "namespace DB { class AggregateFunctionFactory; }\nnamespace DB {\n")
    foreach (i RANGE 0 ${_last})
        list(GET _chdb_drop_files ${i} _file)
        list(GET _chdb_drop_regs ${i} _reg)
        list(REMOVE_ITEM clickhouse_aggregate_functions_sources ${_file})
        set(_chdb_stub_decls "${_chdb_stub_decls}    void ${_reg}(AggregateFunctionFactory &) {}\n")
    endforeach()
    set(_chdb_stub_decls "${_chdb_stub_decls}}\n")
    set(_chdb_stub_file "${CMAKE_CURRENT_BINARY_DIR}/chdbAggregateStubs.cpp")
    file(WRITE "${_chdb_stub_file}" "${_chdb_stub_decls}")
    list(APPEND clickhouse_aggregate_functions_sources "${_chdb_stub_file}")
endif()

extract_into_parent_list(clickhouse_aggregate_functions_sources dbms_sources
    AggregateFunctionFactory.cpp # AggregateFunctionFactory::instance() Used many times
    Combinators/AggregateFunctionCombinatorFactory.cpp # AggregateFunctionCombinatorFactory::instance() Used by AggregateFunctionFactory.cpp
    AggregateFunctionCount.cpp # Used in optimizeUseAggregateProjection.cpp
    IAggregateFunction.cpp # For AggregateFunctionCount.cpp, WindowTransform.cpp, FunctionsConversion.cpp...
    parseAggregateFunctionParameters.cpp # getAggregateFunctionNameAndParametersArray, getAggregateFunctionParametersArray (Graphite.cpp, ExpressionAnalyzer.cpp)
)
extract_into_parent_list(clickhouse_aggregate_functions_headers dbms_headers
    IAggregateFunction.h
    Combinators/IAggregateFunctionCombinator.h
    AggregateFunctionFactory.h
    Combinators/AggregateFunctionCombinatorFactory.h
    FactoryHelpers.h
    parseAggregateFunctionParameters.h
)

# TODO: at v3 the `UniquesHashSet::merge` probe loop lowers `good(hash) = (hash >> s) << s == hash`
# to BMI2 `bzhi`/`shlxq`/`shrxq`, which is slightly slower than the legacy `shl %cl + not + test`
# sequence on Sapphire Rapids (regresses `uniq_stored #1` by ~9%). Disabling BMI1/BMI2 on this TU
# keeps the rest of v3 (AVX2/FMA/etc.) while restoring the legacy shift codegen.
if (ARCH_AMD64 AND X86_ARCH_LEVEL VERSION_GREATER_EQUAL 3)
    set_source_files_properties(AggregateFunctionUniq.cpp PROPERTIES COMPILE_FLAGS "-mno-bmi -mno-bmi2")
endif()

add_library(clickhouse_aggregate_functions ${clickhouse_aggregate_functions_headers} ${clickhouse_aggregate_functions_sources})
target_link_libraries(clickhouse_aggregate_functions PRIVATE dbms PUBLIC ch_contrib::cityhash)

if(ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (ENABLE_FUZZING)
    add_subdirectory(fuzzers)
endif()

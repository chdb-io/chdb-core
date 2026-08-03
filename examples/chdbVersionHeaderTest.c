/**
 * chdbVersionHeaderTest.c
 *
 * The chDB version is available two ways without opening a connection or
 * running SELECT chdb():
 *   - CHDB_VERSION      : compile-time string constant in chdb.h.
 *   - chdb_version()    : C function returning the linked library's version,
 *                         which stays correct even if the header and the
 *                         library come from different versions.
 *
 * This checks both are well-formed (non-empty, not "None", contain a digit)
 * and agree in a matched build.
 *
 * Build:
 *   clang examples/chdbVersionHeaderTest.c -I./programs/local -L. -lchdb \
 *         -o examples/chdbVersionHeaderTest
 *   LD_LIBRARY_PATH=. ./examples/chdbVersionHeaderTest
 */

#include <ctype.h>
#include <stdio.h>
#include <string.h>

#include "chdb.h"

#ifndef CHDB_VERSION
#error "CHDB_VERSION is not defined in chdb.h"
#endif

static int looks_like_version(const char * v)
{
    int failed = 0;
    if (v == NULL || v[0] == '\0') {
        fprintf(stderr, "  ASSERT FAIL: version is empty/NULL\n");
        return 1;
    }
    if (strcmp(v, "None") == 0) {
        fprintf(stderr, "  ASSERT FAIL: version is \"None\"\n");
        failed = 1;
    }
    int has_digit = 0;
    for (const char * p = v; *p; p++)
        if (isdigit((unsigned char) *p)) { has_digit = 1; break; }
    if (!has_digit) {
        fprintf(stderr, "  ASSERT FAIL: version has no digit: \"%s\"\n", v);
        failed = 1;
    }
    return failed;
}

int main(void)
{
    int failed = 0;

    const char * macro_ver = CHDB_VERSION;      /* compile-time (header) */
    const char * lib_ver = chdb_version();      /* runtime (linked library) */

    failed += looks_like_version(macro_ver);
    failed += looks_like_version(lib_ver);

    if (strcmp(macro_ver, lib_ver) != 0) {
        fprintf(stderr, "  ASSERT FAIL: header (%s) and library (%s) versions differ\n",
                macro_ver, lib_ver);
        failed += 1;
    }

    printf("CHDB_VERSION (header)   -> %s\n", macro_ver);
    printf("chdb_version() (library) -> %s\n", lib_ver);
    printf("\n== summary: %d failed ==\n", failed);
    return failed ? 1 : 0;
}

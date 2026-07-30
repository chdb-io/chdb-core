/**
 * chdbVersionHeaderTest.c
 *
 * The chDB version is available as a compile-time string constant in chdb.h,
 * so callers can read it without opening a connection or running SELECT chdb().
 *
 * This test uses only the header (no chdb_connect / no libchdb calls) to prove
 * exactly that: CHDB_VERSION must be defined and look like a version string.
 *
 * Build (header only, no linking against libchdb):
 *   clang examples/chdbVersionHeaderTest.c -I./programs/local -o examples/chdbVersionHeaderTest
 *   ./examples/chdbVersionHeaderTest
 */

#include <ctype.h>
#include <stdio.h>
#include <string.h>

#include "chdb.h"

#ifndef CHDB_VERSION
#error "CHDB_VERSION is not defined in chdb.h"
#endif

int main(void)
{
    int failed = 0;
    const char * v = CHDB_VERSION;

    /* Non-empty. */
    if (v[0] == '\0') {
        fprintf(stderr, "  ASSERT FAIL: CHDB_VERSION is empty\n");
        failed += 1;
    }
    /* Not the literal "None" (guards against the version-injection regression). */
    if (strcmp(v, "None") == 0) {
        fprintf(stderr, "  ASSERT FAIL: CHDB_VERSION is \"None\"\n");
        failed += 1;
    }
    /* Looks like a version: contains at least one digit. */
    int has_digit = 0;
    for (const char * p = v; *p; p++)
        if (isdigit((unsigned char) *p)) { has_digit = 1; break; }
    if (!has_digit) {
        fprintf(stderr, "  ASSERT FAIL: CHDB_VERSION has no digit: \"%s\"\n", v);
        failed += 1;
    }

    printf("CHDB_VERSION (from header, no connection) -> %s\n", v);
    printf("\n== summary: %d failed ==\n", failed);
    return failed ? 1 : 0;
}

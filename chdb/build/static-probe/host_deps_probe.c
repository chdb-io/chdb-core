/* A consumer with its own static copies of libraries libchdb.a bundles (chdb-io/chdb-core#342).
 *
 * chDB bundles zlib-ng, zstd and OpenSSL. A host that links its own static libz.a, libzstd.a
 * or libcrypto.a into the same executable is ordinary, and the archive must not break it:
 * the link must not fail with duplicate definitions (check_static_lib_hermetic.sh links
 * without --allow-multiple-definition), and neither side may bind to the other's copy.
 *
 * Each library is checked both ways. The host's calls must reach the host's copy, which
 * reports the version of the headers it was compiled against; chDB's copies are other
 * versions, or at least other builds (zlib-ng reports a "zlib-ng" version string). And data
 * crosses between the two copies: the host compresses files with its own libraries that chDB
 * then reads with its own, and both compute the same SHA-256 digest.
 *
 * zstd is optional (the build host may have no libzstd-dev); the gate defines
 * HOST_DEPS_ZSTD when it links one.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/opensslv.h>
#include <zlib.h>
#ifdef HOST_DEPS_ZSTD
#include <zstd.h>
#endif

#include "chdb.h"

static const char * const CSV = "1,alpha\n2,beta\n3,gamma\n";

static int fail(const char * message)
{
    fprintf(stderr, "host_deps_probe: %s\n", message);
    return 1;
}

/* Runs `query` and requires its CSV output to be exactly `expected`. */
static int check_query(chdb_connection * conn, const char * query, const char * expected)
{
    chdb_result * result = chdb_query(*conn, query, "CSV");
    if (!result)
        return fail("chdb_query returned NULL");
    const char * error = chdb_result_error(result);
    if (error)
    {
        fprintf(stderr, "host_deps_probe: %s: %s\n", query, error);
        chdb_destroy_query_result(result);
        return 1;
    }
    const char * buffer = chdb_result_buffer(result);
    size_t length = chdb_result_length(result);
    int ok = buffer && length == strlen(expected) && memcmp(buffer, expected, length) == 0;
    if (!ok)
        fprintf(stderr, "host_deps_probe: %s: expected \"%s\", got \"%.*s\"\n", query, expected, (int)length,
                buffer ? buffer : "");
    chdb_destroy_query_result(result);
    return ok ? 0 : 1;
}

int main(void)
{
    char dir_template[] = "/tmp/host_deps_probe.XXXXXX";
    const char * dir = mkdtemp(dir_template);
    if (!dir)
        return fail("mkdtemp failed");
    char gz_path[256], zst_path[256], query[1024];
    snprintf(gz_path, sizeof(gz_path), "%s/data.csv.gz", dir);
    snprintf(zst_path, sizeof(zst_path), "%s/data.csv.zst", dir);

    /* The host's own copies answer for themselves. */
    if (strcmp(zlibVersion(), ZLIB_VERSION) != 0)
    {
        fprintf(stderr, "host_deps_probe: zlibVersion() is %s, the host's zlib.h says %s\n", zlibVersion(), ZLIB_VERSION);
        return 1;
    }
    if (OpenSSL_version_num() != OPENSSL_VERSION_NUMBER)
    {
        fprintf(stderr, "host_deps_probe: OpenSSL_version_num() is %lx, the host's opensslv.h says %lx\n",
                (unsigned long)OpenSSL_version_num(), (unsigned long)OPENSSL_VERSION_NUMBER);
        return 1;
    }

    /* Host zlib writes a gzip file. */
    gzFile gz = gzopen(gz_path, "wb");
    if (!gz || gzputs(gz, CSV) != (int)strlen(CSV) || gzclose(gz) != Z_OK)
        return fail("host zlib could not write the gzip file");

#ifdef HOST_DEPS_ZSTD
    if (ZSTD_versionNumber() != ZSTD_VERSION_NUMBER)
    {
        fprintf(stderr, "host_deps_probe: ZSTD_versionNumber() is %u, the host's zstd.h says %u\n",
                ZSTD_versionNumber(), (unsigned)ZSTD_VERSION_NUMBER);
        return 1;
    }
    size_t bound = ZSTD_compressBound(strlen(CSV));
    void * compressed = malloc(bound);
    size_t compressed_size = ZSTD_compress(compressed, bound, CSV, strlen(CSV), 3);
    if (ZSTD_isError(compressed_size))
        return fail("host zstd could not compress");
    FILE * zst = fopen(zst_path, "wb");
    if (!zst || fwrite(compressed, 1, compressed_size, zst) != compressed_size || fclose(zst) != 0)
        return fail("could not write the zstd file");
    free(compressed);
#endif

    /* Host OpenSSL computes a digest chDB computes too. */
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_length = 0;
    if (!EVP_Digest("chdb", 4, digest, &digest_length, EVP_sha256(), NULL) || digest_length != 32)
        return fail("host OpenSSL could not compute SHA-256");
    char host_hex[65 + 2];
    for (unsigned int i = 0; i < digest_length; ++i)
        snprintf(host_hex + 2 * i, 3, "%02X", digest[i]);

    char * argv[] = {"host_deps_probe"};
    chdb_connection * conn = chdb_connect(1, argv);
    if (!conn)
        return fail("chdb_connect returned NULL");

    int failures = 0;
    /* chDB's zlib-ng reads what the host's zlib wrote. */
    snprintf(query, sizeof(query), "SELECT sum(c1), arrayStringConcat(groupArray(c2), '-') FROM file('%s', 'CSV')", gz_path);
    failures += check_query(conn, query, "6,\"alpha-beta-gamma\"\n");
#ifdef HOST_DEPS_ZSTD
    /* chDB's zstd reads what the host's zstd wrote. */
    snprintf(query, sizeof(query), "SELECT sum(c1), arrayStringConcat(groupArray(c2), '-') FROM file('%s', 'CSV')", zst_path);
    failures += check_query(conn, query, "6,\"alpha-beta-gamma\"\n");
#endif
    /* chDB's OpenSSL agrees with the host's. */
    char expected[128];
    snprintf(expected, sizeof(expected), "\"%s\"\n", host_hex);
    failures += check_query(conn, "SELECT hex(SHA256('chdb'))", expected);
    /* A failing query is an error result, not a crash. */
    chdb_result * result = chdb_query(*conn, "SELECT * FROM host_deps_probe_missing_table", "CSV");
    if (!result || !chdb_result_error(result))
        failures += fail("a query of a missing table did not fail");
    chdb_destroy_query_result(result);
    chdb_close_conn(conn);

    /* And the host's copies still work after chDB has run. */
    gz = gzopen(gz_path, "rb");
    char back[64] = {0};
    if (!gz || gzread(gz, back, sizeof(back) - 1) != (int)strlen(CSV) || strcmp(back, CSV) != 0)
        failures += fail("host zlib could not read back its own file after chDB ran");
    if (gz)
        gzclose(gz);

    unlink(gz_path);
    unlink(zst_path);
    rmdir(dir);
    if (failures)
        return 1;
    printf("host_deps_probe: host zlib %s, OpenSSL %s%s coexist with chDB %s\n", zlibVersion(),
           OpenSSL_version(OPENSSL_VERSION),
#ifdef HOST_DEPS_ZSTD
           ", zstd " ZSTD_VERSION_STRING,
#else
           "",
#endif
           chdb_version());
    return 0;
}

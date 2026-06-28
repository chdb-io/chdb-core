#pragma once

#include "config.h"

#if defined(OS_WASM)

#include <TableFunctions/ITableFunctionFileLike.h>
#include <IO/HTTPHeaderEntries.h>

namespace DB
{

class Context;
class TableFunctionFactory;

/* s3(source, [access_key_id, secret_access_key, [session_token]], [format, [structure, [compression]]])
 *
 * WASM-only, AWS-SDK-free implementation. Reads an S3 object over the host JS
 * runtime (synchronous XHR + Range, via StorageURL -> ReadBufferFromWebFetch).
 * For private buckets the request is signed with AWS Signature V4 (computed in
 * C++ with OpenSSL); for public buckets (no keys, or NOSIGN) no auth is sent.
 * s3:// URLs are rewritten to virtual-hosted https URLs.
 */
class TableFunctionS3 : public ITableFunctionFileLike
{
public:
    static constexpr auto name = "s3";
    static constexpr auto signature = " - url\n"
                                      " - url, format\n"
                                      " - url, format, structure\n"
                                      " - url, format, structure, compression_method\n"
                                      " - url, access_key_id, secret_access_key\n"
                                      " - url, access_key_id, secret_access_key, format\n"
                                      " - url, access_key_id, secret_access_key, format, structure\n"
                                      " - url, access_key_id, secret_access_key, format, structure, compression_method\n"
                                      " - url, access_key_id, secret_access_key, session_token, format[, structure[, compression_method]]\n"
                                      " - url, NOSIGN[, format[, structure[, compression_method]]]\n";

    String getName() const override { return name; }
    String getSignature() const override { return signature; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

protected:
    void parseArgumentsImpl(ASTs & args, const ContextPtr & context) override;
    std::optional<String> tryGetFormatFromFirstArgument() override;

private:
    StoragePtr getStorage(
        const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
        const std::string & table_name, const String & compression_method_, bool is_insert_query) const override;

    /// The underlying storage is StorageURL (we read S3 over https via ReadBufferFromWebFetch),
    /// so report "URL": it is what StorageFactory knows and what the access check expects.
    const char * getStorageEngineName() const override { return "URL"; }

    /// Returns the SigV4 auth headers for a GET on `filename`, or an empty list
    /// for public / NOSIGN access (no credentials).
    HTTPHeaderEntries computeAuthHeaders() const;

    String access_key_id;
    String secret_access_key;
    String session_token;
    bool no_sign = false;
    String region = "us-east-1";
};

void registerTableFunctionS3(TableFunctionFactory & factory);

}

#endif

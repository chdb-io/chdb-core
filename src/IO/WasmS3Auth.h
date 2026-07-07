#pragma once

#if defined(OS_WASM)

#include <IO/HTTPHeaderEntries.h>
#include <base/types.h>

namespace DB::WasmS3
{

/// AWS auth helpers for the WASM build, where the AWS SDK is not compiled and
/// S3 access rides plain HTTPS through the host JavaScript environment.
/// Shared by the wasm s3() table function and the wasm data-lake object storage.

/// Extract the AWS region from an S3 host, e.g.
///   bucket.s3.eu-west-3.amazonaws.com -> eu-west-3
///   bucket.s3.amazonaws.com           -> us-east-1
///   s3.us-west-2.amazonaws.com        -> us-west-2
String extractRegionFromHost(const String & host);

/// s3://bucket/key -> https://bucket.s3.<region>.amazonaws.com/key (virtual-hosted).
/// http(s):// URLs are passed through unchanged.
void rewriteS3Source(const String & source, String & out_url, String & out_region);

/// AWS Signature V4 headers for a request with UNSIGNED-PAYLOAD (read-only use:
/// GET / HEAD). The Range header is deliberately not signed, so one signature
/// covers every range read of an object within the SigV4 clock-skew window.
HTTPHeaderEntries signV4Request(
    const String & method,
    const String & url,
    const String & region,
    const String & access_key_id,
    const String & secret_access_key,
    const String & session_token);

}

#endif

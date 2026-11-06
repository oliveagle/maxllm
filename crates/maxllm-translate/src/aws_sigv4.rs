// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! AWS Signature Version 4 (SigV4) request signing.
//!
//! Implements the SigV4 algorithm defined in the AWS documentation:
//! <https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html>
//!
//! The signing flow:
//!
//! 1. Build a **canonical request** from the HTTP method, canonical URI,
//!    canonical query string, canonical headers and payload hash.
//! 2. Build a **string to sign** containing the algorithm identifier, the
//!    request timestamp, the credential scope and the canonical-request hash.
//! 3. Derive a **signing key** by chaining HMAC-SHA256 over the secret,
//!    date, region, service and the `aws4_request` terminator.
//! 4. Compute the **signature** as HMAC-SHA256 of the string to sign.
//!
//! Credentials are read from the environment (`AWS_ACCESS_KEY_ID` /
//! `AWS_SECRET_ACCESS_KEY`) — no STS assume-role chain is supported.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// Lowercase hex encoding of a byte slice.
pub fn hex(data: &[u8]) -> String {
    const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(data.len() * 2);
    for &b in data {
        out.push(HEX_DIGITS[(b >> 4) as usize] as char);
        out.push(HEX_DIGITS[(b & 0x0f) as usize] as char);
    }
    out
}

/// SHA-256 of `data`, as lowercase hex.
pub fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

/// HMAC-SHA256, raw bytes.
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts keys of any length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Derive the SigV4 signing key for a given date/region/service scope.
///
/// `date` is the 8-digit date portion of the X-Amz-Date header (YYYYMMDD).
pub fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// One header entry to include in the signed header set.
pub struct SignHeader<'a> {
    pub name: &'a str,
    pub value: &'a str,
}

/// The output of a successful SigV4 sign operation.
pub struct SignedRequest {
    /// Value of the `Authorization` request header.
    pub authorization: String,
    /// Value of the `X-Amz-Date` request header.
    pub amz_date: String,
    /// Lowercase-hex SHA-256 of the request payload.
    pub content_sha256: String,
    /// Lowercase-hex SigV4 signature (for tests / debugging).
    pub signature: String,
}

/// The inputs of a SigV4 signing operation.
pub struct SigV4Params<'a> {
    /// HTTP verb, upper-case ("POST", "GET", …).
    pub method: &'a str,
    /// URI-encoded request path ("/model/x/invoke"); the caller supplies the
    /// path exactly as it goes on the wire.
    pub canonical_uri: &'a str,
    /// URI-encoded, sorted query string ("" when there is none).
    pub canonical_query: &'a str,
    /// The Host header value; always part of the signed header set.
    pub host: &'a str,
    /// Extra headers to sign (e.g. `x-amz-content-sha256`); `host` and
    /// `x-amz-date` are always included.
    pub headers: &'a [SignHeader<'a>],
    /// The exact request body bytes; hashed into the signature.
    pub payload: &'a [u8],
    /// AWS access key id.
    pub access_key: &'a str,
    /// AWS secret access key.
    pub secret_key: &'a str,
    /// AWS region ("us-east-1").
    pub region: &'a str,
    /// AWS service name ("bedrock").
    pub service: &'a str,
    /// Full X-Amz-Date timestamp (YYYYMMDDTHHMMSSZ).
    pub amz_date: &'a str,
}

/// AWS credentials plus the region they are scoped to.
pub struct AwsCredentials<'a> {
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub region: &'a str,
}

/// Sign an HTTP request using AWS Signature Version 4.
///
/// The headers are sorted by name before canonicalisation; duplicate header
/// names are not supported (each name must appear at most once).
pub fn sign_request(params: &SigV4Params<'_>) -> SignedRequest {
    let payload_hash = sha256_hex(params.payload);

    // Canonical headers: host and x-amz-date are always signed; the caller
    // headers (x-amz-content-sha256, …) are merged in and the whole set is
    // sorted by lowercase header name.
    let mut signed: Vec<(&str, &str)> = vec![
        ("host", params.host),
        ("x-amz-date", params.amz_date),
    ];
    for h in params.headers {
        signed.push((h.name, h.value));
    }
    signed.sort_by(|a, b| a.0.cmp(b.0));

    let mut canonical_headers = String::new();
    let mut signed_headers = String::new();
    for (name, value) in &signed {
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        canonical_headers.push_str(value);
        canonical_headers.push('\n');
        if !signed_headers.is_empty() {
            signed_headers.push(';');
        }
        signed_headers.push_str(name);
    }

    let canonical_request = format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
        method = params.method,
        canonical_uri = params.canonical_uri,
        canonical_query = params.canonical_query,
    );

    let date = &params.amz_date[..8];
    let scope = format!("{date}/{}/{}/aws4_request", params.region, params.service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
        params.amz_date,
        sha256_hex(canonical_request.as_bytes())
    );

    let key = signing_key(params.secret_key, date, params.region, params.service);
    let signature = hex(&hmac_sha256(&key, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, \
         SignedHeaders={signed_headers}, Signature={signature}",
        params.access_key
    );

    SignedRequest {
        authorization,
        amz_date: params.amz_date.to_string(),
        content_sha256: payload_hash,
        signature,
    }
}

/// Sign a Bedrock Runtime API request (POST with a JSON payload).
///
/// Adds `x-amz-content-sha256` to the signed-header set — required by
/// Bedrock — and returns the three headers to set on the wire.
pub fn sign_bedrock(
    method: &str,
    canonical_uri: &str,
    host: &str,
    payload: &[u8],
    credentials: &AwsCredentials<'_>,
    amz_date: &str,
) -> SignedRequest {
    let content_sha256 = sha256_hex(payload);
    let headers = [SignHeader {
        name: "x-amz-content-sha256",
        value: &content_sha256,
    }];
    sign_request(&SigV4Params {
        method,
        canonical_uri,
        canonical_query: "",
        host,
        headers: &headers,
        payload,
        access_key: credentials.access_key,
        secret_key: credentials.secret_key,
        region: credentials.region,
        service: "bedrock",
        amz_date,
    })
}

/// The current time formatted as an X-Amz-Date header value
/// (YYYYMMDDTHHMMSSZ, UTC).
pub fn amz_date_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format_amz_date(secs)
}

/// Format a Unix timestamp (seconds) as an X-Amz-Date string.
///
/// Uses Howard Hinnant's days-from-civil algorithm — no chrono dependency.
pub fn format_amz_date(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };
    format!("{year:04}{month:02}{d:02}T{h:02}{m:02}{s:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AWS official SigV4 test-suite credentials.
    const AKID: &str = "AKIDEXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

    /// AWS IAM documentation example — GET ListUsers with query string.
    /// The expected canonical-request hash AND signature are both published
    /// verbatim in the docs, so this validates the full chain.
    ///
    /// <https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html>
    #[test]
    fn test_iam_official_vector() {
        let amz_date = "20150830T123600Z";
        let payload_hash = sha256_hex(b"");
        let content_type = "application/x-www-form-urlencoded; charset=utf-8";
        let headers = [SignHeader {
            name: "content-type",
            value: content_type,
        }];
        let signed = sign_request(&SigV4Params {
            method: "GET",
            canonical_uri: "/",
            canonical_query: "Action=ListUsers&Version=2010-05-08",
            host: "iam.amazonaws.com",
            headers: &headers,
            payload: b"",
            access_key: AKID,
            secret_key: SECRET,
            region: "us-east-1",
            service: "iam",
            amz_date,
        });
        assert_eq!(
            signed.signature,
            "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        );
        assert_eq!(signed.content_sha256, payload_hash);
        assert_eq!(signed.amz_date, amz_date);
        assert!(signed.authorization.starts_with(
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request"
        ));
        assert!(signed
            .authorization
            .contains("SignedHeaders=content-type;host;x-amz-date"));
    }

    /// The canonical-request construction must be byte-exact: signing the
    /// same request twice yields the same signature (deterministic).
    #[test]
    fn test_sign_is_deterministic() {
        let creds = AwsCredentials {
            access_key: AKID,
            secret_key: SECRET,
            region: "us-east-1",
        };
        let a = sign_bedrock(
            "POST",
            "/model/anthropic.claude-sonnet-4-20250514-v1:0/invoke",
            "bedrock-runtime.us-east-1.amazonaws.com",
            br#"{"max_tokens":1024,"messages":[{"role":"user","content":"hi"}]}"#,
            &creds,
            "20260101T000000Z",
        );
        let b = sign_bedrock(
            "POST",
            "/model/anthropic.claude-sonnet-4-20250514-v1:0/invoke",
            "bedrock-runtime.us-east-1.amazonaws.com",
            br#"{"max_tokens":1024,"messages":[{"role":"user","content":"hi"}]}"#,
            &creds,
            "20260101T000000Z",
        );
        assert_eq!(a.signature, b.signature);
        assert_eq!(a.authorization, b.authorization);
    }

    /// The Bedrock signing path includes x-amz-content-sha256 in the signed
    /// header set — required by the Bedrock Runtime API.
    #[test]
    fn test_bedrock_signs_content_sha256() {
        let payload = br#"{"max_tokens":1,"messages":[]}"#;
        let creds = AwsCredentials {
            access_key: AKID,
            secret_key: SECRET,
            region: "eu-west-1",
        };
        let signed = sign_bedrock(
            "POST",
            "/model/m/invoke",
            "bedrock-runtime.eu-west-1.amazonaws.com",
            payload,
            &creds,
            "20260202T120000Z",
        );
        assert!(signed
            .authorization
            .contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"));
        assert_eq!(signed.content_sha256, sha256_hex(payload));
        assert!(signed
            .authorization
            .contains("/20260202/eu-west-1/bedrock/aws4_request"));
        assert_eq!(signed.signature.len(), 64);
    }

    /// Changing any signed input must change the signature.
    #[test]
    fn test_signature_sensitive_to_payload_and_host() {
        let base = ("POST", "/model/m/invoke", "host.example.com", b"{}".as_slice());
        let creds = AwsCredentials {
            access_key: AKID,
            secret_key: SECRET,
            region: "us-east-1",
        };
        let sign = |m: &str, u: &str, h: &str, p: &[u8]| {
            sign_bedrock(m, u, h, p, &creds, "20260303T030303Z").signature
        };
        let baseline = sign(base.0, base.1, base.2, base.3);
        assert_ne!(baseline, sign("GET", base.1, base.2, base.3));
        assert_ne!(baseline, sign(base.0, "/other", base.2, base.3));
        assert_ne!(baseline, sign(base.0, base.1, "other.example.com", base.3));
        assert_ne!(baseline, sign(base.0, base.1, base.2, b"different"));
    }

    /// The date formatter must produce the canonical YYYYMMDDTHHMMSSZ shape
    /// and stay correct across a leap year boundary.
    #[test]
    fn test_format_amz_date() {
        assert_eq!(format_amz_date(0), "19700101T000000Z");
        // 2024-02-29 is a leap day.
        assert_eq!(format_amz_date(1_709_164_800), "20240229T000000Z");
        // 2024-12-31T23:59:59Z
        assert_eq!(format_amz_date(1_735_689_599), "20241231T235959Z");
        // 2025-01-01T00:00:00Z
        assert_eq!(format_amz_date(1_735_689_600), "20250101T000000Z");
    }

    /// The empty-string SHA-256 is a well-known constant.
    #[test]
    fn test_empty_sha256() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}

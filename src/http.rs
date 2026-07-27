//! Shared HTTP transport for SDK ⇆ Gateway REST. Owns auth headers, error
//! mapping, retries/backoff, idempotency keys, and timeouts. Mirrors `http.ts`.
//!
//! Retry policy: idempotent requests (GET, DELETE, or any request carrying an
//! Idempotency-Key) retry on network errors, HTTP 5xx, and bodies flagged
//! `retryable`, with exponential backoff + jitter. 429 is NOT retried (org at
//! its session cap). Non-idempotent writes are never silently retried.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::Method;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::{map_gateway_error, SolariError};
use crate::types::GatewayErrorBody;

/// JS `encodeURIComponent`: leaves `A-Za-z0-9 - _ . ! ~ * ' ( )` unescaped,
/// percent-encodes every other byte as `%XX` over its UTF-8 bytes.
///
/// USE THIS FOR EVERY ID INTERPOLATED INTO A PATH. Session ids are
/// `<poolId>:<vmId>:<orgId>.<sig>`, and this crate previously built paths with a
/// bare `format!("/sandboxes/{sandbox_id}")` — no escaping at all. That happened
/// to work (colons are legal path characters, and the id alphabet is otherwise
/// URL-safe), but it left the id's bytes at the mercy of whatever the gateway
/// hands back, and it made Rust's URLs differ byte-for-byte from the TypeScript
/// reference, which the cross-language wire contract test cannot look away from.
pub(crate) fn encode_uri_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        let c = b as char;
        if c.is_ascii_alphanumeric()
            || matches!(c, '-' | '_' | '.' | '!' | '~' | '*' | '\'' | '(' | ')')
        {
            out.push(c);
        } else {
            out.push('%');
            out.push_str(&format!("{b:02X}"));
        }
    }
    out
}

/// Options for constructing an [`HttpTransport`].
#[derive(Debug, Clone)]
pub struct HttpOptions {
    pub api_key: String,
    pub base_url: String,
    /// Max retry attempts for idempotent requests. Default 5.
    pub max_retries: u32,
    /// Per-request timeout. Default 300s.
    pub request_timeout: Duration,
    /// Fixed retry delay. When `None`, uses exponential backoff + jitter
    /// (mainly for tests: pass `Some(0)` to make retries instant/deterministic).
    pub retry_delay_ms: Option<u64>,
}

impl HttpOptions {
    pub fn new(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        HttpOptions {
            api_key: api_key.into(),
            base_url: base_url.into(),
            max_retries: 5,
            request_timeout: Duration::from_millis(300_000),
            retry_delay_ms: None,
        }
    }
}

pub struct HttpTransport {
    api_key: String,
    base_url: String,
    client: reqwest::Client,
    max_retries: u32,
    retry_delay_ms: Option<u64>,
}

impl HttpTransport {
    pub fn new(opts: HttpOptions) -> Result<Self, SolariError> {
        if opts.api_key.is_empty() {
            return Err(SolariError::Other("HttpTransport requires an apiKey".into()));
        }
        if opts.base_url.is_empty() {
            return Err(SolariError::Other("HttpTransport requires a baseUrl".into()));
        }
        let base_url = opts.base_url.trim_end_matches('/').to_string();
        let client = reqwest::Client::builder()
            .timeout(opts.request_timeout)
            .build()
            .map_err(|e| SolariError::Other(format!("failed to build HTTP client: {e}")))?;
        Ok(HttpTransport {
            api_key: opts.api_key,
            base_url,
            client,
            max_retries: opts.max_retries,
            retry_delay_ms: opts.retry_delay_ms,
        })
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// `Authorization` header for handles + WS upgrades.
    pub fn auth_headers(&self) -> Vec<(String, String)> {
        vec![("Authorization".into(), format!("Bearer {}", self.api_key))]
    }

    /// ws:// (or wss://) origin derived from the gateway base URL.
    pub fn ws_origin(&self) -> String {
        // Mirrors TS `.replace(/^http/, "ws")`: http→ws, https→wss.
        self.base_url.replacen("http", "ws", 1)
    }

    pub async fn request<T: DeserializeOwned>(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        idempotency_key: Option<String>,
    ) -> Result<T, SolariError> {
        let idempotent =
            method == "GET" || method == "DELETE" || idempotency_key.is_some();
        let m = Method::from_bytes(method.as_bytes())
            .map_err(|e| SolariError::Other(format!("bad method {method}: {e}")))?;
        let url = format!("{}{}", self.base_url, path);
        let body_str = body.as_ref().map(|b| serde_json::to_string(b).unwrap());

        let mut attempt: u32 = 0;
        loop {
            let mut rb = self
                .client
                .request(m.clone(), &url)
                .header("Authorization", format!("Bearer {}", self.api_key))
                .header("Accept", "application/json");
            if let Some(k) = &idempotency_key {
                rb = rb.header("Idempotency-Key", k);
            }
            if let Some(b) = &body_str {
                rb = rb.header("Content-Type", "application/json").body(b.clone());
            }

            let resp = match rb.send().await {
                Ok(r) => r,
                Err(err) => {
                    if idempotent && attempt < self.max_retries {
                        tokio::time::sleep(self.backoff(attempt)).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(SolariError::connection(format!(
                        "{method} {path} failed: {err}"
                    )));
                }
            };

            let status = resp.status().as_u16();
            if !(200..300).contains(&status) {
                let text = resp.text().await.unwrap_or_default();
                let err_body: Option<GatewayErrorBody> =
                    if text.is_empty() { None } else { serde_json::from_str(&text).ok() };
                // 5xx or an explicit `retryable` hint. NOT 429.
                let retryable = status >= 500
                    || err_body.as_ref().and_then(|b| b.retryable).unwrap_or(false);
                if idempotent && retryable && attempt < self.max_retries {
                    tokio::time::sleep(self.backoff(attempt)).await;
                    attempt += 1;
                    continue;
                }
                return Err(map_gateway_error(status, err_body));
            }

            let text = resp.text().await.unwrap_or_default();
            if text.is_empty() {
                return serde_json::from_str("null")
                    .map_err(|e| SolariError::Other(format!("empty body decode: {e}")));
            }
            return serde_json::from_str(&text)
                .map_err(|e| SolariError::Other(format!("bad response body: {e}")));
        }
    }

    fn backoff(&self, attempt: u32) -> Duration {
        if let Some(ms) = self.retry_delay_ms {
            return Duration::from_millis(ms);
        }
        // 150·2^n capped at 8s, plus rand(0..250)ms jitter. Mirrors http.ts.
        let base = (150u64.saturating_mul(1u64 << attempt.min(20))).min(8000);
        Duration::from_millis(base + jitter_ms())
    }
}

/// A fresh idempotency key. Not a real UUID (no `uuid` dep) but unique + opaque,
/// which is all the gateway requires. Format resembles a v4 UUID.
pub fn new_idempotency_key() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let a = t ^ n.rotate_left(17) ^ 0x9E37_79B9_7F4A_7C15;
    let b = t.rotate_left(31) ^ n.wrapping_mul(0x2545_F491_4F6C_DD1D);
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        (a >> 32) as u32,
        (a >> 16) as u16,
        (a & 0x0fff) as u16,
        ((b >> 48) as u16 & 0x3fff) | 0x8000,
        b & 0xffff_ffff_ffff
    )
}

fn jitter_ms() -> u64 {
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    (t ^ t.rotate_left(13)) % 250
}

//! Typed error hierarchy for the Solari SDK.
//!
//! Mirrors the reference `errors.ts`: gateway HTTP status codes and control-WS
//! RPC failures are mapped onto a single enum so callers can `match` broadly.
//! `SolariError` is the base type carrying every failure this crate raises.

use crate::types::GatewayErrorBody;

/// Every error raised by the SDK. Corresponds to the `SolariError` hierarchy in
/// the TypeScript reference (AuthError / PlanError / ConcurrencyLimitError /
/// NoCapacityError / GatewayError / ActionError / TimeoutError / ConnectionError).
#[derive(Debug, thiserror::Error)]
// Non-exhaustive so a future variant is a non-breaking addition for downstream
// crates that match on this type. It does NOT constrain matches inside this
// crate (Rust only enforces the wildcard arm on out-of-crate matches), so keep
// it: its whole purpose is to make the next variant free rather than breaking.
#[non_exhaustive]
pub enum SolariError {
    /// HTTP 401 — the API key was missing, malformed, or rejected.
    #[error("{message}")]
    Auth { status: u16, message: String, code: Option<String> },

    /// The plan does not allow this: HTTP 402 (`FeatureRequiresPlan`, `InsufficientCredit`) or 403 `NotEntitled` (the plan
    /// does not include sandboxes/desktops). `status` is the status the gateway really sent.
    #[error("{message}")]
    Plan { status: u16, message: String, code: Option<String> },

    /// HTTP 429 `ConcurrencyLimitExceeded` (or a 429 with no code) — the org is at its session cap. NOT retryable. Other 429s
    /// (`TooManyExecutions`, a per-sandbox limit, `ObjectLimit`, the template caps) are `Gateway` errors with the gateway's `code`.
    #[error("{message}")]
    ConcurrencyLimit { status: u16, message: String, code: Option<String> },

    /// HTTP 503 `NoCapacity` / `StartTimeout` (or a 503 with no code) — no host available (retryable). Other 503s
    /// (`SandboxUnavailable`, `StoreUnavailable`, `AdmissionUnavailable`, `DependencyUnavailable`) are `Gateway` errors: the
    /// sandbox, store or admission service is unavailable, which is not a capacity problem.
    #[error("{message}")]
    NoCapacity { status: u16, message: String, code: Option<String> },

    /// Any other non-2xx gateway response (incl. 404/405/409/501).
    #[error("{message}")]
    Gateway { status: u16, message: String, code: Option<String> },

    /// A control-WS RPC replied `{ok:false}`.
    #[error("{message}")]
    Action { method: String, message: String, code: Option<String> },

    /// A call (or connect) did not complete within its timeout.
    #[error("Action \"{method}\" timed out after {timeout_ms}ms")]
    Timeout { method: String, timeout_ms: u64 },

    /// The control WebSocket is not open (never connected, or dropped).
    #[error("{message}")]
    Connection { message: String },

    /// A git subcommand exited non-zero (message includes sub/exit/detail).
    #[error("{message}")]
    Git { message: String },

    /// Anything else (bad response body, internal invariant).
    #[error("{0}")]
    Other(String),
    /// A caller passed an invalid argument (e.g. a blank session id). Distinct
    /// from `Other`: this is the caller's input to fix, not an unexpected
    /// server response.
    #[error("{message}")]
    Validation { message: String },
}

impl SolariError {
    pub(crate) fn connection(msg: impl Into<String>) -> Self {
        SolariError::Connection { message: msg.into() }
    }

    /// The gateway HTTP status this error carries, when it is a gateway error.
    pub fn status(&self) -> Option<u16> {
        match self {
            SolariError::Auth { status, .. }
            | SolariError::Plan { status, .. }
            | SolariError::ConcurrencyLimit { status, .. }
            | SolariError::NoCapacity { status, .. }
            | SolariError::Gateway { status, .. } => Some(*status),
            _ => None,
        }
    }
}

/// Map a gateway HTTP response onto the appropriate typed error. Falls back to a
/// generic `Gateway` for unrecognized statuses. Mirrors `mapGatewayError`.
pub(crate) fn map_gateway_error(status: u16, body: Option<GatewayErrorBody>) -> SolariError {
    let code = body.as_ref().and_then(|b| b.code.clone());
    let message = body
        .as_ref()
        .and_then(|b| b.message.clone().or_else(|| b.error.clone()).or_else(|| b.code.clone()))
        .unwrap_or_else(|| format!("Gateway request failed with status {status}"));
    // `code` is the discriminator (PROTOCOL.md section 4); the status alone is not enough because one status carries
    // several meanings. A response with no code keeps the type its status has always had.
    match (status, code.as_deref()) {
        (401, _) => SolariError::Auth { status, message, code },
        (402, _) => SolariError::Plan { status, message, code },
        // The gateway's only JSON 403 is NotEntitled (the plan does not include sandboxes/desktops).
        (403, Some("NotEntitled")) => SolariError::Plan { status, message, code },
        (429, None | Some("ConcurrencyLimitExceeded")) => SolariError::ConcurrencyLimit { status, message, code },
        (503, None | Some("NoCapacity" | "StartTimeout")) => SolariError::NoCapacity { status, message, code },
        _ => SolariError::Gateway { status, message, code },
    }
}

/// True when an error means the one-shot `/exec` route is not served (an older
/// gateway) — the only case where `run` retries over the control WS. A real
/// host/command failure is NOT route-unavailable and must propagate. Mirrors
/// `isRouteUnavailable` (GatewayError with status 404/405/501).
pub(crate) fn is_route_unavailable(err: &SolariError) -> bool {
    matches!(err.status(), Some(404) | Some(405) | Some(501))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    enum Kind {
        Auth,
        Plan,
        Concurrency,
        NoCapacity,
        Plain,
    }

    fn kind(e: &SolariError) -> Kind {
        match e {
            SolariError::Auth { .. } => Kind::Auth,
            SolariError::Plan { .. } => Kind::Plan,
            SolariError::ConcurrencyLimit { .. } => Kind::Concurrency,
            SolariError::NoCapacity { .. } => Kind::NoCapacity,
            SolariError::Gateway { .. } => Kind::Plain,
            other => panic!("not a gateway error: {other:?}"),
        }
    }

    /// Gateway status + body code -> typed error (PROTOCOL.md section 4).
    #[test]
    fn maps_by_code_first() {
        let cases: &[(&str, u16, Option<&str>, Kind)] = &[
            ("401", 401, Some("Unauthorized"), Kind::Auth),
            ("402-plan", 402, Some("FeatureRequiresPlan"), Kind::Plan),
            ("402-credit", 402, Some("InsufficientCredit"), Kind::Plan),
            ("403-not-entitled", 403, Some("NotEntitled"), Kind::Plan),
            ("403-other", 403, Some("Forbidden"), Kind::Plain),
            ("404", 404, Some("NotFound"), Kind::Plain),
            ("409-sandbox-stopped", 409, Some("SandboxStopped"), Kind::Plain),
            ("410-gone", 410, Some("SandboxGone"), Kind::Plain),
            ("429-concurrency-cap", 429, Some("ConcurrencyLimitExceeded"), Kind::Concurrency),
            ("429-no-code", 429, None, Kind::Concurrency),
            ("429-too-many-executions", 429, Some("TooManyExecutions"), Kind::Plain),
            ("429-object-limit", 429, Some("ObjectLimit"), Kind::Plain),
            ("500-internal", 500, Some("InternalError"), Kind::Plain),
            ("501-not-supported", 501, Some("NotSupported"), Kind::Plain),
            ("503-no-capacity", 503, Some("NoCapacity"), Kind::NoCapacity),
            ("503-start-timeout", 503, Some("StartTimeout"), Kind::NoCapacity),
            ("503-no-code", 503, None, Kind::NoCapacity),
            ("503-sandbox-unavailable", 503, Some("SandboxUnavailable"), Kind::Plain),
            ("503-store-unavailable", 503, Some("StoreUnavailable"), Kind::Plain),
            ("503-admission-unavailable", 503, Some("AdmissionUnavailable"), Kind::Plain),
            ("503-dependency-unavailable", 503, Some("DependencyUnavailable"), Kind::Plain),
            ("507-insufficient-capacity", 507, Some("InsufficientCapacity"), Kind::Plain),
        ];
        for (id, status, code, want) in cases {
            let body = GatewayErrorBody { code: code.map(str::to_string), ..Default::default() };
            let e = map_gateway_error(*status, Some(body));
            assert_eq!(&kind(&e), want, "errors/{id}");
            assert_eq!(e.status(), Some(*status), "errors/{id}: the status the gateway sent is kept");
            let kept = match &e {
                SolariError::Auth { code, .. }
                | SolariError::Plan { code, .. }
                | SolariError::ConcurrencyLimit { code, .. }
                | SolariError::NoCapacity { code, .. }
                | SolariError::Gateway { code, .. } => code.clone(),
                _ => unreachable!(),
            };
            assert_eq!(kept.as_deref(), *code, "errors/{id}: the gateway's code is kept");
        }
    }
}

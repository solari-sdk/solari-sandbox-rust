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
pub enum SolariError {
    /// HTTP 401/403 — the API key was missing, malformed, or rejected.
    #[error("{message}")]
    Auth { status: u16, message: String, code: Option<String> },

    /// HTTP 402 (or a `plan_*` body) — the plan does not allow this.
    #[error("{message}")]
    Plan { status: u16, message: String, code: Option<String> },

    /// HTTP 429 — the org is at its session cap. NOT retryable.
    #[error("{message}")]
    ConcurrencyLimit { status: u16, message: String, code: Option<String> },

    /// HTTP 503 / body `no_capacity` — no host available (retryable).
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
    match status {
        401 | 403 => SolariError::Auth { status, message, code },
        402 => SolariError::Plan { status, message, code },
        429 => SolariError::ConcurrencyLimit { status, message, code },
        503 => SolariError::NoCapacity { status, message, code },
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

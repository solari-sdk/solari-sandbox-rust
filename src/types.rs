//! Wire types for the Solari SDK core surface. serde field names match the JSON
//! wire EXACTLY (camelCase where the gateway/control-WS is camelCase).

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// REST — session lifecycle
// ---------------------------------------------------------------------------

/// Idle lifecycle policy (pause/kill on timeout; optional auto-resume).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Lifecycle {
    pub on_timeout: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_resume: Option<bool>,
    /// Auto-park (deschedule) the guest after this many milliseconds with no
    /// activity. Parking freezes vCPUs but keeps the slot, ports and control
    /// channel resident, so the next request wakes it in milliseconds. Floored
    /// by the gateway and capped at the session's idle window.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub park_after_ms: Option<u64>,
    /// Bounded park: the most a session may stay parked before the gateway
    /// wakes it unconditionally, even with no incoming traffic. Floored at the
    /// gateway's park floor and capped at the idle window.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub park_max_ms: Option<u64>,
    /// Escalate a session parked this long to a full hibernate (durable
    /// checkpoint, slot freed) rather than staying frozen indefinitely.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hibernate_after_parked_ms: Option<u64>,
    /// Take a live durable checkpoint at most this often while the session
    /// runs. Floored server-side.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_every_ms: Option<u64>,
}

/// `Lifecycle::default()` is the gateway's own default: pause on timeout, every other knob unset.
impl Default for Lifecycle {
    fn default() -> Self {
        Lifecycle {
            on_timeout: "pause".to_string(),
            auto_resume: None,
            park_after_ms: None,
            park_max_ms: None,
            hibernate_after_parked_ms: None,
            checkpoint_every_ms: None,
        }
    }
}

/// One attach-at-create instruction: a persistent volume and the absolute
/// in-guest path to mount it at. The mount happens host-side before the guest
/// starts, and survives pause/resume and recreate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeAttachment {
    /// A `vol_…` id belonging to your org.
    pub volume_id: String,
    /// Absolute in-guest mount point, e.g. `/data`.
    pub path: String,
}

/// Wire body for `POST /sandboxes`. Null/unset fields are omitted.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSandboxRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mem_mb: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_gb: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub envs: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    /// Legacy, gateway-side FALLBACK idle window in seconds, only used when
    /// `timeout_ms` is unset — prefer `timeout_ms`. Kept for parity with the
    /// reference SDKs, which have always accepted it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_snapshot: Option<String>,
    /// VM isolation tier: `"shared"` (sandbox-only), `"dedicated"` (default)
    /// or `"hardened"` (own VM, no outbound network — verified 2026-09-23 on
    /// staging, not yet on prod; refused with 409 `HostCapabilityMissing`
    /// where a host doesn't advertise it).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub isolation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<Lifecycle>,
    /// Initial display resolution, e.g. `"1280x720"`. Desktops only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution: Option<String>,
    /// Record the session server-side; the response carries a presigned playback
    /// URL. Desktops only — `record` on a headless sandbox is rejected (400
    /// `RecordingRequiresDesktop`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<bool>,
    /// Persistent volumes to mount before the session starts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volumes: Option<Vec<VolumeAttachment>>,
}

/// `POST /sandboxes/:id/resume` reply. The gateway may hand back a fresh
/// `controlUrl` (the session lands on a new slot, so the old one is stale);
/// `Client::resume` derives one when it does not.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResumeResponse {
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub control_url: Option<String>,
}

/// `POST /sandboxes/:id/start` reply. `control_url` is a fresh URL (the sandbox was
/// cold-booted); `already_running` is set when it was already running.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartResponse {
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub control_url: Option<String>,
    #[serde(default)]
    pub already_running: Option<bool>,
}

/// `201` response from `POST /sandboxes`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSandboxResponse {
    pub sandbox_id: String,
    #[serde(default)]
    pub kind: String,
    pub control_url: String,
    #[serde(default)]
    pub expires_at: String,
    /// Present for desktops only; ignored on the sandbox core surface.
    #[serde(default)]
    pub stream_url: Option<String>,
    /// The session recording link, when the create asked for a recording.
    #[serde(default)]
    pub recording_url: Option<String>,
}

/// `GET /sandboxes/:id` response (subset used by `connect`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxView {
    pub sandbox_id: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub expires_at: String,
    /// Some gateways include a control URL directly; derived otherwise.
    #[serde(default)]
    pub control_url: Option<String>,
}

/// Shape gateway error bodies may take.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GatewayErrorBody {
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub retryable: Option<bool>,
    /// The sandbox's current state, sent with a 409 `NotStopped`.
    #[serde(default)]
    pub state: Option<String>,
}

// ---------------------------------------------------------------------------
// commands
// ---------------------------------------------------------------------------

/// Terminal result of `commands.run` (also the `/exec` fast-path response).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandResult {
    #[serde(default)]
    pub exit_code: i64,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    /// True when the command hit its `timeout_ms` and was killed (`exit_code` is -1 then).
    #[serde(default)]
    pub timed_out: bool,
    /// True when the guest's memory watchdog killed the command (exit 137 alone is ambiguous). Always present; `false` on
    /// every path that does not report it.
    #[serde(default)]
    pub oom_killed: bool,
    /// The exact stdout bytes. `stdout` is text (UTF-8, invalid bytes replaced by U+FFFD); a command that prints binary data is
    /// read from here. Exact on the control connection; on the one-shot `/exec` route the gateway has already turned the output
    /// into text, so these are the bytes of that text (not recoverable from the REST answer).
    #[serde(skip)]
    pub stdout_bytes: Vec<u8>,
    /// The gateway's exact-bytes fields on the `/exec` answer (`stdoutBase64` / `stderrBase64`, sent only when the text is lossy).
    /// Read into `stdout_bytes` / `stderr_bytes` and cleared; never serialized.
    #[serde(default, rename = "stdoutBase64", skip_serializing)]
    #[doc(hidden)]
    pub stdout_base64: Option<String>,
    #[serde(default, rename = "stderrBase64", skip_serializing)]
    #[doc(hidden)]
    pub stderr_base64: Option<String>,
    /// The exact stderr bytes; see `stdout_bytes`.
    #[serde(skip)]
    pub stderr_bytes: Vec<u8>,
}

impl CommandResult {
    /// A result carrying text only: the byte fields are that text's bytes.
    pub(crate) fn from_text(exit_code: i64, stdout: String, stderr: String, timed_out: bool) -> Self {
        CommandResult { exit_code, stdout_bytes: stdout.clone().into_bytes(), stderr_bytes: stderr.clone().into_bytes(), stdout, stderr, timed_out, oom_killed: false, ..Default::default() }
    }
}

// ---------------------------------------------------------------------------
// files
// ---------------------------------------------------------------------------

/// One directory entry (`fs.list`). Matches the reference `types.ts` FsEntry:
/// the wire field is `dir` (bool), NOT `isDir`; there is no path/mode/modTime.
#[derive(Debug, Clone, Deserialize)]
pub struct FsEntry {
    #[serde(default)]
    pub name: String,
    #[serde(default, rename = "dir")]
    pub dir: bool,
    #[serde(default)]
    pub size: i64,
}

/// File metadata (`fs.stat`). Matches the reference `types.ts` FsStat: fields
/// `dir` (bool), `mode` (int), and `modTimeMs` (unix-millis).
#[derive(Debug, Clone, Deserialize)]
pub struct FsStat {
    #[serde(default)]
    pub name: String,
    #[serde(default, rename = "dir")]
    pub dir: bool,
    #[serde(default)]
    pub size: i64,
    #[serde(default)]
    pub mode: i64,
    #[serde(default, rename = "modTimeMs")]
    pub mod_time_ms: i64,
}

// ---------------------------------------------------------------------------
// code.run
// ---------------------------------------------------------------------------

/// Kind of a structured chart extracted from a matplotlib figure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChartType {
    Line,
    Scatter,
    Bar,
    Pie,
    BoxAndWhisker,
    Composite,
    #[serde(other)]
    Unknown,
}

impl Default for ChartType {
    fn default() -> Self {
        ChartType::Unknown
    }
}

/// One axis of a 2D chart.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChartAxis {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ticks: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale: Option<String>,
}

/// Structured representation of a matplotlib figure. `elements` is kept as raw
/// JSON so new chart types don't require an SDK bump.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Chart {
    #[serde(default, rename = "type")]
    pub chart_type: ChartType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub x_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub y_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub x: Option<ChartAxis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub y: Option<ChartAxis>,
    /// Loose per-type data (points/bars/slices). Raw decoded JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elements: Option<Value>,
}

/// One rich result object from `code.run`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct CodeResultItem {
    #[serde(default, rename = "type")]
    pub result_type: String,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub png: Option<String>,
    #[serde(default)]
    pub jpeg: Option<String>,
    #[serde(default)]
    pub svg: Option<String>,
    #[serde(default)]
    pub html: Option<String>,
    #[serde(default)]
    pub latex: Option<String>,
    #[serde(default)]
    pub json: Option<Value>,
    #[serde(default)]
    pub markdown: Option<String>,
    #[serde(default)]
    pub chart: Option<Chart>,
}

/// Result of `code.run`, with the client-side `charts` convenience view.
#[derive(Debug, Clone, Default)]
pub struct RunCodeResult {
    pub results: Vec<CodeResultItem>,
    pub error: Option<Value>,
    /// All structured charts across `results` (flattened convenience view).
    pub charts: Vec<Chart>,
}

// ---------------------------------------------------------------------------
// git
// ---------------------------------------------------------------------------

/// Working-tree status (`git.status`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GitStatus {
    pub branch: String,
    /// The upstream branch (`origin/main`), when one is configured (also when it is gone).
    pub upstream: Option<String>,
    pub detached: bool,
    pub ahead: i64,
    pub behind: i64,
    /// Paths with a staged change (index column not blank); a rename is its NEW path. Not a conflicted path.
    pub staged: Vec<String>,
    /// Paths with an unstaged change (worktree column not blank). Not a conflicted path.
    pub modified: Vec<String>,
    /// Untracked files, one per file (never a bare directory).
    pub untracked: Vec<String>,
    /// Paths in an unmerged state (`DD AU UD UA DU AA UU`), each listed once and only here.
    pub conflicted: Vec<String>,
    /// Every record `git status` printed, in order, with its two status columns.
    pub entries: Vec<GitStatusEntry>,
    /// True when there are no entries at all.
    pub clean: bool,
}

/// One `git status --porcelain=v1 -z` record.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GitStatusEntry {
    /// The path, verbatim (not quoted or escaped); for a rename or copy the NEW path.
    pub path: String,
    /// The old path of a rename or copy.
    pub orig_path: Option<String>,
    /// The index (staged) column: `' '`, `M`, `A`, `D`, `R`, `C`, `U`, or `?` for an untracked file.
    pub index: char,
    /// The worktree (unstaged) column, same alphabet.
    pub worktree: char,
}

/// One branch entry (`git.branches`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GitBranch {
    pub name: String,
    pub commit: String,
    pub current: bool,
    /// The upstream branch, when one is set.
    pub upstream: Option<String>,
}

/// One commit record (`git.log`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GitCommit {
    pub hash: String,
    pub author: String,
    pub email: String,
    pub date: String,
    /// The subject line.
    pub message: String,
    /// The rest of the message, without trailing newlines; empty when there is none.
    pub body: String,
}

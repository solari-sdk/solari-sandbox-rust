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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_snapshot: Option<String>,
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
}

// ---------------------------------------------------------------------------
// commands
// ---------------------------------------------------------------------------

/// Terminal result of `commands.run` (also the `/exec` fast-path response).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandResult {
    #[serde(default)]
    pub exit_code: i64,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
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
    pub detached: bool,
    pub ahead: i64,
    pub behind: i64,
    pub staged: Vec<String>,
    pub modified: Vec<String>,
    pub untracked: Vec<String>,
    pub clean: bool,
}

/// One branch entry (`git.branches`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GitBranch {
    pub name: String,
    pub commit: String,
    pub current: bool,
}

/// One commit record (`git.log`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GitCommit {
    pub hash: String,
    pub author: String,
    pub email: String,
    pub date: String,
    pub message: String,
}

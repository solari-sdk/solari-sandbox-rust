//! `commands` namespace: run / start / stdin / kill over the control WS, plus
//! the one-shot REST `/exec` fast path. Mirrors the `commands` surface + the
//! `runCommand`/`startCommand` internals of `handle.ts`.

use std::sync::Arc;

use base64::Engine;
use serde_json::{Map, Value};
use tokio::sync::mpsc;

use crate::channel::{AsyncFrame, ControlChannel};
use crate::client::Sandbox;
use crate::error::{is_route_unavailable, SolariError};
use crate::http::encode_uri_component;
use crate::types::CommandResult;

/// A streamed-output callback (`stdout`/`stderr`). `Fn(&str)`.
pub type OutputCallback = Arc<dyn Fn(&str) + Send + Sync>;

/// Options for `commands.run` / `commands.start`.
#[derive(Default, Clone)]
pub struct RunOptions {
    /// argv tail (the guest runs `cmd` with these, NOT via a shell).
    pub args: Option<Vec<String>>,
    pub cwd: Option<String>,
    pub env: Option<std::collections::HashMap<String, String>>,
    pub user: Option<String>,
    pub timeout_ms: Option<u64>,
    /// Run detached: returns immediately, output still streams via callbacks.
    pub background: bool,
    pub on_stdout: Option<OutputCallback>,
    pub on_stderr: Option<OutputCallback>,
}

impl RunOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args = Some(args.into_iter().map(Into::into).collect());
        self
    }
    pub fn cwd(mut self, cwd: impl Into<String>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }
}

/// A started command handle (`commands.start`).
pub struct CommandHandle {
    pub cmd_id: String,
    channel: Arc<ControlChannel>,
    rx: mpsc::UnboundedReceiver<AsyncFrame>,
    on_stdout: Option<OutputCallback>,
    on_stderr: Option<OutputCallback>,
}

impl CommandHandle {
    /// Write bytes to the command's stdin.
    pub async fn stdin(&self, data: impl AsRef<[u8]>) -> Result<(), SolariError> {
        let b64 = base64::engine::general_purpose::STANDARD.encode(data.as_ref());
        let mut p = Map::new();
        p.insert("cmdId".into(), Value::String(self.cmd_id.clone()));
        p.insert("base64".into(), Value::String(b64));
        self.channel.call("cmd.stdin", Value::Object(p)).await.map(|_| ())
    }

    /// Send a signal (default SIGTERM guest-side).
    pub async fn kill(&self, signal: Option<i64>) -> Result<(), SolariError> {
        let mut p = Map::new();
        p.insert("cmdId".into(), Value::String(self.cmd_id.clone()));
        if let Some(s) = signal {
            p.insert("signal".into(), Value::from(s));
        }
        self.channel.call("cmd.kill", Value::Object(p)).await.map(|_| ())
    }

    /// Wait for the command to exit, accumulating stdout/stderr.
    pub async fn wait(mut self) -> Result<CommandResult, SolariError> {
        let mut stdout = String::new();
        let mut stderr = String::new();
        while let Some(frame) = self.rx.recv().await {
            match frame.typ.as_str() {
                "cmd.data" => {
                    let text = frame
                        .base64
                        .as_ref()
                        .map(|b| decode_text(b))
                        .unwrap_or_default();
                    let is_err = frame.stream.as_deref() == Some("stderr");
                    if is_err {
                        stderr.push_str(&text);
                        if let Some(cb) = &self.on_stderr {
                            cb(&text);
                        }
                    } else {
                        stdout.push_str(&text);
                        if let Some(cb) = &self.on_stdout {
                            cb(&text);
                        }
                    }
                }
                "cmd.exit" => {
                    let exit_code = frame.exit_code.unwrap_or(0);
                    self.channel.unregister_command(&self.cmd_id);
                    return Ok(CommandResult { exit_code, stdout, stderr });
                }
                _ => {}
            }
        }
        // Channel dropped before `cmd.exit`.
        Err(SolariError::connection("Control channel closed"))
    }
}

fn decode_text(b64: &str) -> String {
    match base64::engine::general_purpose::STANDARD.decode(b64) {
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(_) => String::new(),
    }
}

/// Build a JSON params object, inserting only present (`Some`) fields — mirrors
/// how JSON.stringify omits `undefined`.
fn cmd_start_params(cmd: &str, opts: &RunOptions) -> Value {
    let mut p = Map::new();
    p.insert("cmd".into(), Value::String(cmd.to_string()));
    if let Some(args) = &opts.args {
        p.insert("args".into(), Value::from(args.clone()));
    }
    if let Some(cwd) = &opts.cwd {
        p.insert("cwd".into(), Value::String(cwd.clone()));
    }
    if let Some(env) = &opts.env {
        p.insert("env".into(), serde_json::to_value(env).unwrap());
    }
    if let Some(user) = &opts.user {
        p.insert("user".into(), Value::String(user.clone()));
    }
    Value::Object(p)
}

/// `commands.start`: start a process and return a handle immediately.
pub(crate) async fn start_command(
    sb: &Sandbox,
    cmd: &str,
    opts: &RunOptions,
) -> Result<CommandHandle, SolariError> {
    sb.channel().connect().await?;
    let result = sb
        .channel()
        .call("cmd.start", cmd_start_params(cmd, opts))
        .await?;
    let cmd_id = result
        .get("cmdId")
        .and_then(Value::as_str)
        .ok_or_else(|| SolariError::Other("cmd.start reply missing cmdId".into()))?
        .to_string();
    let rx = sb.channel().register_command(&cmd_id);
    Ok(CommandHandle {
        cmd_id,
        channel: sb.channel_arc(),
        rx,
        on_stdout: opts.on_stdout.clone(),
        on_stderr: opts.on_stderr.clone(),
    })
}

/// `commands.run`: run a command to completion.
pub(crate) async fn run_command(
    sb: &Sandbox,
    cmd: &str,
    opts: &RunOptions,
) -> Result<CommandResult, SolariError> {
    // One-shot HTTP fast path: a plain run-to-completion (no streaming
    // callbacks, not backgrounded, no per-command env/user) over the warm REST
    // connection, ONLY when the control channel isn't already open.
    if !opts.background
        && opts.on_stdout.is_none()
        && opts.on_stderr.is_none()
        && opts.env.is_none()
        && opts.user.is_none()
        && !sb.channel().connected()
    {
        match exec_oneshot(sb, cmd, opts).await {
            Ok(r) => return Ok(r),
            // Fall back to the WS path ONLY when `/exec` is unavailable. Any
            // other failure propagates (never silently double-execute).
            Err(err) => {
                if !is_route_unavailable(&err) {
                    return Err(err);
                }
            }
        }
    }

    let handle = start_command(sb, cmd, opts).await?;
    if opts.background {
        return Ok(CommandResult {
            exit_code: 0,
            stdout: String::new(),
            stderr: String::new(),
        });
    }
    handle.wait().await
}

/// POST `/sandboxes/:id/exec` — the one-shot fast path.
async fn exec_oneshot(
    sb: &Sandbox,
    cmd: &str,
    opts: &RunOptions,
) -> Result<CommandResult, SolariError> {
    let mut body = Map::new();
    body.insert("cmd".into(), Value::String(cmd.to_string()));
    if let Some(args) = &opts.args {
        body.insert("args".into(), Value::from(args.clone()));
    }
    if let Some(cwd) = &opts.cwd {
        body.insert("cwd".into(), Value::String(cwd.clone()));
    }
    if let Some(t) = opts.timeout_ms {
        body.insert("timeoutMs".into(), Value::from(t));
    }
    let path = format!("/sandboxes/{}/exec", encode_uri_component(sb.id()));
    sb.http()
        .request::<CommandResult>("POST", &path, Some(Value::Object(body)), None)
        .await
}

/// The ergonomic `commands` accessor returned by `Sandbox::commands()`.
pub struct Commands<'a> {
    pub(crate) sb: &'a Sandbox,
}

impl<'a> Commands<'a> {
    /// Run a command to completion.
    pub async fn run(&self, cmd: &str, opts: RunOptions) -> Result<CommandResult, SolariError> {
        run_command(self.sb, cmd, &opts).await
    }

    /// Start a command and return a handle immediately.
    pub async fn start(&self, cmd: &str, opts: RunOptions) -> Result<CommandHandle, SolariError> {
        start_command(self.sb, cmd, &opts).await
    }
}

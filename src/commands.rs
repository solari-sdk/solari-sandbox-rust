//! `commands` namespace: run / start / stdin / kill over the control WS, plus
//! the one-shot REST `/exec` fast path. Mirrors the `commands` surface + the
//! `runCommand`/`startCommand` internals of `handle.ts`.

use std::sync::Arc;

use base64::Engine;
use serde_json::{Map, Value};
use tokio::sync::mpsc;

use crate::channel::{AsyncFrame, ControlChannel};
use crate::client::Sandbox;
use crate::error::{is_route_unavailable, is_shared_refusal, SolariError};
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

/// How long a cancelled command gets to exit after SIGTERM before it is SIGKILLed.
const CANCEL_KILL_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// A cloneable handle on a running command's input side: write or close stdin and send signals while another task is
/// blocked in [`CommandHandle::wait`] (which consumes the handle). Get one with [`CommandHandle::io`].
#[derive(Clone)]
pub struct CommandIo {
    cmd_id: String,
    channel: Arc<ControlChannel>,
}

impl CommandIo {
    /// The command id.
    pub fn cmd_id(&self) -> &str {
        &self.cmd_id
    }

    /// Write bytes to the command's stdin.
    pub async fn stdin(&self, data: impl AsRef<[u8]>) -> Result<(), SolariError> {
        let b64 = base64::engine::general_purpose::STANDARD.encode(data.as_ref());
        let mut p = Map::new();
        p.insert("cmdId".into(), Value::String(self.cmd_id.clone()));
        p.insert("base64".into(), Value::String(b64));
        self.channel.call("cmd.stdin", Value::Object(p)).await.map(|_| ())
    }

    /// Close the command's stdin (the guest writes nothing, then closes it).
    pub async fn close_stdin(&self) -> Result<(), SolariError> {
        let mut p = Map::new();
        p.insert("cmdId".into(), Value::String(self.cmd_id.clone()));
        p.insert("base64".into(), Value::String(String::new()));
        p.insert("eof".into(), Value::Bool(true));
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
}

/// Stops the guest process when a `commands.run` future is dropped before the command finished: SIGTERM and a closed stdin
/// at once, SIGKILL after [`CANCEL_KILL_GRACE`]. Disarmed once the run returns. Every call is best effort (a command that
/// already finished answers with an error).
struct CancelGuard {
    io: Option<CommandIo>,
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        let Some(io) = self.io.take() else { return };
        let Ok(rt) = tokio::runtime::Handle::try_current() else { return };
        rt.spawn(async move {
            let _ = io.kill(Some(15)).await;
            let _ = io.close_stdin().await;
            tokio::time::sleep(CANCEL_KILL_GRACE).await;
            let _ = io.kill(Some(9)).await;
        });
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
    /// A cloneable handle on this command's input side. [`wait`](Self::wait) consumes the handle, so take this first to
    /// keep writing stdin or signalling while another task waits.
    pub fn io(&self) -> CommandIo {
        CommandIo { cmd_id: self.cmd_id.clone(), channel: self.channel.clone() }
    }

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
    pub async fn wait(self) -> Result<CommandResult, SolariError> {
        self.wait_deadline(None).await
    }

    /// `wait` with an optional deadline. When it elapses the command is killed (SIGKILL) and the
    /// result is exit -1 with the output so far, the same result the guest's own `/exec` timeout gives.
    pub(crate) async fn wait_deadline(
        mut self,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<CommandResult, SolariError> {
        let mut stdout = String::new();
        let mut stderr = String::new();
        let mut out_dec = Utf8Stream::default();
        let mut err_dec = Utf8Stream::default();
        loop {
            let next = match deadline {
                Some(d) => match tokio::time::timeout_at(d, self.rx.recv()).await {
                    Ok(frame) => frame,
                    Err(_) => {
                        let _ = self.kill(Some(9)).await;
                        self.channel.unregister_command(&self.cmd_id);
                        return Ok(CommandResult { exit_code: -1, stdout, stderr, timed_out: true });
                    }
                },
                None => self.rx.recv().await,
            };
            let Some(frame) = next else { break };
            match frame.typ.as_str() {
                "cmd.data" => {
                    let is_err = frame.stream.as_deref() == Some("stderr");
                    // One streaming decoder per stream: a multi-byte character split across two frames must come out
                    // whole, not as U+FFFD at each end.
                    let bytes = frame.base64.as_ref().map(|b| decode_bytes(b)).unwrap_or_default();
                    let text = if is_err { err_dec.push(&bytes) } else { out_dec.push(&bytes) };
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
                    // Bytes held back at the end of a stream (a truncated character) are flushed, not dropped.
                    let rest = out_dec.finish();
                    if !rest.is_empty() {
                        stdout.push_str(&rest);
                        if let Some(cb) = &self.on_stdout {
                            cb(&rest);
                        }
                    }
                    let rest = err_dec.finish();
                    if !rest.is_empty() {
                        stderr.push_str(&rest);
                        if let Some(cb) = &self.on_stderr {
                            cb(&rest);
                        }
                    }
                    return Ok(CommandResult { exit_code, stdout, stderr, timed_out: false });
                }
                _ => {}
            }
        }
        // Channel dropped before `cmd.exit`.
        Err(SolariError::connection("Control channel closed"))
    }
}

fn decode_bytes(b64: &str) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD.decode(b64).unwrap_or_default()
}

/// Streaming UTF-8 decoder: holds back an incomplete character at the end of a chunk until the next chunk completes it.
/// Invalid bytes become U+FFFD, as `String::from_utf8_lossy` does.
#[derive(Default)]
pub(crate) struct Utf8Stream {
    pending: Vec<u8>,
}

impl Utf8Stream {
    pub(crate) fn push(&mut self, bytes: &[u8]) -> String {
        self.pending.extend_from_slice(bytes);
        let mut out = String::new();
        loop {
            match std::str::from_utf8(&self.pending) {
                Ok(s) => {
                    out.push_str(s);
                    self.pending.clear();
                    break;
                }
                Err(e) => {
                    let valid = e.valid_up_to();
                    out.push_str(std::str::from_utf8(&self.pending[..valid]).unwrap_or(""));
                    match e.error_len() {
                        Some(n) => {
                            out.push('\u{FFFD}');
                            self.pending.drain(..valid + n);
                        }
                        None => {
                            // An incomplete character at the end: wait for the next chunk.
                            self.pending.drain(..valid);
                            break;
                        }
                    }
                }
            }
        }
        out
    }

    /// End of stream: a character still incomplete is reported once as U+FFFD.
    pub(crate) fn finish(&mut self) -> String {
        if self.pending.is_empty() {
            String::new()
        } else {
            self.pending.clear();
            "\u{FFFD}".to_string()
        }
    }
}

#[cfg(test)]
mod utf8_tests {
    use super::Utf8Stream;

    #[test]
    fn characters_split_across_chunks_come_out_whole() {
        let text = "漢字🙂é".repeat(50);
        let mut dec = Utf8Stream::default();
        let mut out = String::new();
        for b in text.as_bytes() {
            out.push_str(&dec.push(&[*b]));
        }
        out.push_str(&dec.finish());
        assert_eq!(out, text);
        assert!(!out.contains('\u{FFFD}'));
    }

    #[test]
    fn a_truncated_character_at_the_end_is_reported_once_and_invalid_bytes_are_replaced() {
        let mut dec = Utf8Stream::default();
        assert_eq!(dec.push(b"ok\xe6"), "ok");
        assert_eq!(dec.finish(), "\u{FFFD}");
        let mut dec = Utf8Stream::default();
        assert_eq!(dec.push(b"a\xffb"), "a\u{FFFD}b");
        assert_eq!(dec.finish(), "");
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
    // connection, when the control channel isn't already open. A shared sandbox
    // refuses command streams but runs commands over REST, so once we know the
    // sandbox is shared the REST route is the only one that works, connected or not.
    let rest_eligible = !opts.background
        && opts.on_stdout.is_none()
        && opts.on_stderr.is_none()
        && opts.env.is_none()
        && opts.user.is_none();
    if rest_eligible && (!sb.channel().connected() || sb.is_shared()) {
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

    let handle = match start_command(sb, cmd, opts).await {
        Ok(h) => h,
        Err(err) if is_shared_refusal(&err) => {
            // The sandbox is shared: remember it, and run this command over REST if it can be (the refusal means
            // nothing started, so nothing can run twice).
            sb.mark_shared();
            if rest_eligible {
                return exec_oneshot(sb, cmd, opts).await;
            }
            return Err(err);
        }
        Err(err) => return Err(err),
    };
    if opts.background {
        // Nobody awaits a background command, so something has to drive its output to the callbacks: they were never called.
        if opts.on_stdout.is_some() || opts.on_stderr.is_some() {
            tokio::spawn(async move {
                let _ = handle.wait().await;
            });
        }
        return Ok(CommandResult {
            exit_code: 0,
            stdout: String::new(),
            stderr: String::new(),
            timed_out: false,
        });
    }
    // `timeout_ms` is a deadline for the command here too (it used to be honoured only by the REST path).
    let deadline = opts
        .timeout_ms
        .filter(|t| *t > 0)
        .map(|t| tokio::time::Instant::now() + std::time::Duration::from_millis(t));
    let mut guard = CancelGuard { io: Some(handle.io()) };
    let result = handle.wait_deadline(deadline).await;
    guard.io = None; // finished (or failed) on its own: nothing to stop
    result
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

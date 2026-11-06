# Changelog

## 0.5.0

`commands().run` now behaves the same over the one-shot `/exec` route and the live control connection (the shared conformance rows in `tests/conformance/spec/commands.json` pin it; before this release 8 of them failed for this SDK).

### Changed
- **A deadline that fires below the guest is a result, not an error.** `400 ExecTimeout` and the guest's hidden vsock read timeout (`502 ... i/o timeout`) on `/exec` now return `CommandResult { exit_code: -1, timed_out: true, .. }`, the same result the live connection gives when `timeout_ms` elapses.
- **A command that cannot start is one error on both routes:** `SolariError::Action { method: "cmd.start", code: Some("ExecFailed"), message }`. `/exec` reports it as a `400 ExecFailed` gateway error, which used to come back as `SolariError::Gateway`; the live connection's refusal carries no code, and `fork/exec ...` / `chdir ...` now get `ExecFailed`.
- `CommandResult` gained fields, so code that builds one with a struct literal needs `..Default::default()`.

### Added
- **`user` is validated before any transport is chosen.** A `user` that is neither `root` nor a unix user name (`^[a-z_][a-z0-9_-]{0,31}$`) fails with `SolariError::Validation` before any request, the same error the gateway's `/exec` route gives, instead of reaching the guest on the live connection. `timeout_ms` is an unsigned integer here, so a negative deadline cannot be written.
- **A `409 HostCapabilityMissing` from `/exec` is retried once on the live connection.** It means the host pool lacks `exec.user` for the sandbox's default user and nothing ran; the live connection reaches the guest directly, which honours `user`.
- **`CommandResult::oom_killed`**: true when the guest's memory watchdog killed the command (exit 137 alone is ambiguous). Always present.
- **`CommandResult::stdout_bytes` / `stderr_bytes`**: the exact output bytes. `stdout` / `stderr` stay UTF-8 text (invalid bytes become U+FFFD); a command that prints binary data is read from the byte fields. They are exact on the live connection. On `/exec` they are exact when the gateway sends its `stdoutBase64` / `stderrBase64` fields (it does, once its guests have them, for output that is not valid UTF-8); until then they are the bytes of the lossy text.
- **`SolariError::ExecInterrupted`** for `409 ExecInterrupted` (the sandbox was parked while the command ran, so it may have partly run). It is never retried and never re-run on the live connection: exactly one `/exec` request.

### Fixed
- **`commands().start` output callbacks fire without `wait()`.** They ran only inside `wait()`, so a caller that kept the handle and never awaited it saw no output. A task now drives them as the output arrives; `wait()` collects the result.
- A flaky test: `read_loops_over_chunks_until_eof` and the upload test used a 2 s call timeout that a loaded machine exceeds with 4 MiB chunks in a debug build (20 failures in 25 runs under CPU load; 0 after), and `commands_run_timeout_kills_the_command` used an 80 ms deadline.
- Correction to 0.4.0: `SolariError` is `#[non_exhaustive]`, so a new variant is not a breaking change for code outside the crate; only matches written inside this crate are exhaustive.

## 0.4.0

### Added
- **`SolariError::SandboxNotStopped { status, message, code, state }`** is returned for a 409 `NotStopped` or `StartInProgress` from `Client::start()` / `Sandbox::start()`. `code` is `"NotStopped"` (the sandbox is in another state; use `resume()` for a paused one) or `"StartInProgress"` (a start is already under way: poll until the sandbox is running), and `state` holds the sandbox's current state when the gateway sends it. Other 409s (`StoppedDiskMissing`, `GoldenMismatch`, `HostCapabilityMissing`, `SnapshotNotRestorable`, ...) stay `SolariError::Gateway`. This is the same type the TypeScript and Python SDKs expose as `SandboxNotStoppedError`.

### Changed
- A new enum variant is a breaking change for code that matches `SolariError` exhaustively, hence 0.4.0. `SolariError::status()` returns `Some(409)` for the new variant; a catch-all arm or a `Gateway` arm that matched these two codes needs the new arm.

## 0.3.4

### Fixed
- **`commands().run` on a shared-isolation sandbox works when the control channel is connected.** A shared sandbox refuses command streams (`cmd.start`, error `NotAvailableOnSharedIsolation`) but runs commands over REST; `run` on a connected channel sent `cmd.start` and returned the refusal. It now runs that command over REST `/exec` and remembers that the sandbox is shared, so later runs go straight to REST (the same behaviour as the TypeScript and Python SDKs). A run that needs a stream (an output callback, `env`, `user`, `background`) still returns the refusal.

## 0.3.3

### Added
- **`files().upload` and `files().download`** move a file in 4 MiB chunks (`fs.upload` / `fs.download`), so a large file never travels as one frame. A 48 MiB file used to be a 64 MiB frame that closed the control channel and failed every call in flight on it; reads failed from 12 MiB (the websocket's 16 MiB frame limit after base64). `files().read` now reads through the same chunked loop (a guest image that predates `fs.download` is read with `fs.read`).
- **`CommandHandle::io()`** returns a cloneable `CommandIo` (`stdin`, `close_stdin`, `kill`). `wait` consumes the handle, so a command could not be written to while another task waited on it; take the `CommandIo` first.

### Changed
- `files().write` refuses more than 32 MiB before sending anything (`file too large for a single call (N bytes, max 33554432): use files.upload()/files.download() or uploadUrl()`), so one big call fails alone and the channel stays up. Use `upload` for larger files.

### Fixed
- **Dropping a `Sandbox` closes its socket.** `ControlChannel` had no `Drop`: the reader task kept the channel state alive, the state kept the writer's sender alive, and the writer task waited on it, so the socket and both tasks ran until the server hung up.
- **`background: true` delivers output to `on_stdout` / `on_stderr`.** Nothing drove the command's output once `run` returned, so the callbacks were never called.
- **Dropping a `commands().run` future (or a `tokio::time::timeout` around it) stops the guest process.** It gets SIGTERM and a closed stdin at once and SIGKILL 2 s later; before, it ran on. Dropping a `code().run` future asks the guest to interrupt the kernel (`code.interrupt`; a guest that does not know it is ignored). A command on the one-shot `/exec` route is not stopped by dropping the call (that route has no command id to signal).

## 0.3.2

### Fixed
- **A deleted sandbox no longer reports "not connected".** A `404` on the one-shot `/exec` route is only read as "route not served" (and retried over the control channel) when it is a plain-text 404; a JSON `404` is the gateway saying the sandbox is gone, and now reaches you as that error.
- **`CommandResult::timed_out`** says a command hit its `timeout_ms` and was killed (`exit_code` -1). The one-shot route reported it and it was dropped; the control-channel path now sets it too.
- **`Sandbox::recording_url()`** (and `CreateSandboxResponse::recording_url`) carry the recording link from a create that asked for a recording (it was discarded).
- **Security: `git().clone` / `push` / `pull` credentials no longer reach the clone URL, `.git/config` or the process list.** With a username/password, `clone` used to put them in the URL, so git saved them as `remote.origin.url` (and from there into snapshots and templates) and showed them in `/proc/*/cmdline`; push and pull put them in a `-c url.….insteadOf=` argument. They now travel as environment variables of that one command and git reads them through an inline credential helper bound to the remote's origin, so nothing is left in the repository, in a config file or on a command line, and git never offers the password to another host (a redirect to a different host gets nothing). If you cloned a private repo with credentials before this release, check `.git/config` in existing snapshots and templates and rotate the token if it is there. A username or password with a line break is refused, and the password is masked in error messages. Authenticated git needs the live control connection, so on a `shared` sandbox only public remotes work. `GitRunner` gains `run_git_env` (default: refuses an environment, so credentials are never silently dropped); existing implementors keep compiling.
- **git commands no longer read user text as options.** `clone`, `push`, `pull` and `checkout` put `--` before the repository, remote and ref, and refuse a remote, branch or ref that starts with `-` (`clone("--upload-pack=cmd", …)` ran `cmd` in the sandbox; `checkout("--orphan=x")` created a branch).
- **Streamed command output no longer corrupts non-ASCII text.** Each output chunk was decoded on its own, so a multi-byte character split across two chunks became U+FFFD. Each stream now has its own streaming decoder, and bytes held back at the end of a command are flushed on exit.

## 0.3.1

### Added
- `ClientOptions::with_api_key(key)` and `DEFAULT_BASE_URL` (`https://api.getsolari.com`).

### Changed
- An empty `base_url` now means the hosted Solari API, matching the TypeScript and Python SDKs.
  `ClientOptions::new(key, base_url)` with an explicit URL behaves as before.

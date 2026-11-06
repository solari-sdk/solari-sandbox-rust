//! `git` namespace: safe, non-shell `git` invocations composed over the command
//! runner, with client-side parsers. Ported EXACTLY from the `git` namespace in
//! `handle.ts` (arg construction + status/branches/log parsers + auth-URL).
//!
//! The runner is abstracted behind [`GitRunner`] so the parsers/args can be
//! unit-tested with a stub that returns canned [`CommandResult`]s (no live WS),
//! exactly like the TS `git.test.mjs` stubs `runCommand`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use crate::error::SolariError;
use crate::types::{CommandResult, GitBranch, GitCommit, GitStatus, GitStatusEntry};

/// The seam the git namespace runs commands through. `Sandbox` implements it by
/// delegating to `commands.run("git", …)`; tests implement a recording stub.
pub trait GitRunner: Send + Sync {
    fn run_git<'a>(
        &'a self,
        args: Vec<String>,
        cwd: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<CommandResult, SolariError>> + Send + 'a>>;

    /// Like [`run_git`](GitRunner::run_git) with environment variables for the command (used to hand credentials to
    /// git). The default refuses rather than dropping them: a runner that cannot pass an environment must not run an
    /// authenticated command as an unauthenticated one.
    fn run_git_env<'a>(
        &'a self,
        args: Vec<String>,
        cwd: Option<String>,
        env: Option<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = Result<CommandResult, SolariError>> + Send + 'a>> {
        match env {
            None => self.run_git(args, cwd),
            Some(_) => Box::pin(async {
                Err(SolariError::Git {
                    message: "this git runner cannot pass environment variables".to_string(),
                })
            }),
        }
    }
}

/// Options for `git.clone`.
#[derive(Default, Clone)]
pub struct GitCloneOptions {
    pub path: Option<String>,
    pub branch: Option<String>,
    pub depth: Option<i64>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub cwd: Option<String>,
}

/// Options for `git.commit`.
#[derive(Default, Clone)]
pub struct GitCommitOptions {
    pub cwd: Option<String>,
    pub author: Option<String>,
    pub email: Option<String>,
    pub all: bool,
}

/// Options for `git.push` / `git.pull`.
#[derive(Default, Clone)]
pub struct GitRemoteOptions {
    pub cwd: Option<String>,
    pub remote: Option<String>,
    pub branch: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

/// Options for `git.log`.
#[derive(Default, Clone)]
pub struct GitLogOptions {
    pub cwd: Option<String>,
    pub max_count: Option<i64>,
}

/// The ergonomic `git` accessor returned by `Sandbox::git()`.
pub struct Git<'a> {
    runner: &'a dyn GitRunner,
}

impl<'a> Git<'a> {
    pub fn new(runner: &'a dyn GitRunner) -> Self {
        Git { runner }
    }

    /// Run `git <args>`; return stdout, or an error with stderr on non-zero exit.
    async fn must_git(&self, args: Vec<String>, cwd: Option<String>) -> Result<String, SolariError> {
        self.must_git_env(args, cwd, None, None).await
    }

    /// As [`must_git`](Self::must_git) with an environment for the command; a `secret` is masked in the message.
    async fn must_git_env(
        &self,
        args: Vec<String>,
        cwd: Option<String>,
        env: Option<HashMap<String, String>>,
        secret: Option<&str>,
    ) -> Result<String, SolariError> {
        let sub = git_subcommand(&args);
        let r = self.runner.run_git_env(args, cwd, env).await?;
        if r.exit_code != 0 {
            let mut detail = {
                let s = r.stderr.trim();
                if !s.is_empty() {
                    s.to_string()
                } else {
                    r.stdout.trim().to_string()
                }
            };
            if let Some(sec) = secret.filter(|s| !s.is_empty()) {
                detail = detail.replace(sec, "***");
            }
            return Err(SolariError::Git {
                message: format!("git {} failed (exit {}): {}", sub, r.exit_code, detail),
            });
        }
        Ok(r.stdout)
    }

    /// Clone `url` into `opts.path` under `opts.cwd`.
    pub async fn clone(&self, url: &str, opts: GitCloneOptions) -> Result<(), SolariError> {
        let auth = git_auth(url, opts.username.as_deref(), opts.password.as_deref())?;
        let mut args: Vec<String> = auth.as_ref().map(|a| a.0.clone()).unwrap_or_default();
        args.push("clone".into());
        if let Some(d) = opts.depth {
            if d > 0 {
                args.push("--depth".into());
                args.push(d.to_string());
            }
        }
        if let Some(b) = &opts.branch {
            args.push("--branch".into());
            args.push(b.clone());
        }
        // `--` ends options: a url such as "--upload-pack=cmd" is then a (bad) repository, not an option.
        args.push("--".into());
        args.push(url.to_string());
        if let Some(p) = &opts.path {
            args.push(p.clone());
        }
        self.must_git_env(args, opts.cwd, auth.map(|a| a.1), opts.password.as_deref())
            .await
            .map(|_| ())
    }

    /// Parsed working-tree status.
    pub async fn status(&self, cwd: Option<&str>) -> Result<GitStatus, SolariError> {
        let out = self
            .must_git(
                vec![
                    "status".into(),
                    "--porcelain=v1".into(),
                    "-z".into(),
                    "--branch".into(),
                    "--untracked-files=all".into(),
                ],
                cwd.map(String::from),
            )
            .await?;
        Ok(parse_status(&out))
    }

    /// Stage `paths` (no-op on empty; uses `--` to guard flag-like paths).
    pub async fn add(&self, paths: &[String], cwd: Option<&str>) -> Result<(), SolariError> {
        if paths.is_empty() {
            return Ok(());
        }
        let mut args = vec!["add".to_string(), "--".to_string()];
        args.extend(paths.iter().cloned());
        self.must_git(args, cwd.map(String::from)).await.map(|_| ())
    }

    /// Commit staged changes; returns the new commit hash.
    pub async fn commit(
        &self,
        message: &str,
        opts: GitCommitOptions,
    ) -> Result<String, SolariError> {
        let mut args: Vec<String> = Vec::new();
        if let Some(a) = &opts.author {
            args.push("-c".into());
            args.push(format!("user.name={a}"));
        }
        if let Some(e) = &opts.email {
            args.push("-c".into());
            args.push(format!("user.email={e}"));
        }
        args.push("commit".into());
        args.push("-m".into());
        args.push(message.to_string());
        if opts.all {
            args.push("-a".into());
        }
        self.must_git(args, opts.cwd.clone()).await?;
        let hash = self
            .must_git(vec!["rev-parse".into(), "HEAD".into()], opts.cwd)
            .await?
            .trim()
            .to_string();
        Ok(hash)
    }

    /// Push to a remote (default `origin`).
    pub async fn push(&self, opts: GitRemoteOptions) -> Result<(), SolariError> {
        self.push_pull("push", opts).await
    }

    /// Pull from a remote (default `origin`).
    pub async fn pull(&self, opts: GitRemoteOptions) -> Result<(), SolariError> {
        self.push_pull("pull", opts).await
    }

    async fn push_pull(&self, op: &str, opts: GitRemoteOptions) -> Result<(), SolariError> {
        let remote = opts.remote.clone().unwrap_or_else(|| "origin".to_string());
        reject_option_like("remote", &remote)?;
        if let Some(b) = &opts.branch {
            reject_option_like("branch", b)?;
        }
        let mut auth = None;
        if opts.username.is_some() || opts.password.is_some() {
            // The helper is bound to the remote's origin, so resolve the remote's URL first.
            let plain = self
                .must_git(
                    vec!["remote".into(), "get-url".into(), "--".into(), remote.clone()],
                    opts.cwd.clone(),
                )
                .await?;
            auth = git_auth(plain.trim(), opts.username.as_deref(), opts.password.as_deref())?;
        }
        let mut args: Vec<String> = auth.as_ref().map(|a| a.0.clone()).unwrap_or_default();
        args.push(op.to_string());
        args.push("--".into());
        args.push(remote);
        if let Some(b) = &opts.branch {
            args.push(b.clone());
        }
        self.must_git_env(args, opts.cwd, auth.map(|a| a.1), opts.password.as_deref())
            .await
            .map(|_| ())
    }

    /// Check out an existing ref, or create a branch with `create: true`.
    pub async fn checkout(
        &self,
        r#ref: &str,
        cwd: Option<&str>,
        create: bool,
    ) -> Result<(), SolariError> {
        // A ref that starts with "-" would be read as an option ("--orphan=x" created a branch). Git never allows one
        // as a branch name, so refuse it rather than guess.
        reject_option_like("ref", r#ref)?;
        let mut args = vec!["checkout".to_string()];
        if create {
            args.push("-b".into());
        }
        args.push(r#ref.to_string());
        args.push("--".into());
        self.must_git(args, cwd.map(String::from)).await.map(|_| ())
    }

    /// List local branches.
    pub async fn branches(&self, cwd: Option<&str>) -> Result<Vec<GitBranch>, SolariError> {
        let out = self
            .must_git(
                vec![
                    "for-each-ref".into(),
                    "--format=%(HEAD)%1f%(refname:short)%1f%(objectname:short)%1f%(upstream:short)".into(),
                    "refs/heads".into(),
                ],
                cwd.map(String::from),
            )
            .await?;
        Ok(parse_branches(&out))
    }

    /// Recent commits, newest first.
    pub async fn log(&self, opts: GitLogOptions) -> Result<Vec<GitCommit>, SolariError> {
        let mut args = vec![
            "log".to_string(),
            "--format=%H%x1f%an%x1f%ae%x1f%aI%x1f%s%x1f%b%x1e".to_string(),
        ];
        if let Some(n) = opts.max_count {
            if n > 0 {
                args.push(format!("--max-count={n}"));
            }
        }
        // A repository with no commit yet is an empty history, not an error.
        let out = match self.must_git(args, opts.cwd).await {
            Ok(out) => out,
            Err(SolariError::Git { message }) if message.contains("does not have any commits yet") => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        Ok(parse_log(&out))
    }
}

// --- pure parsers + arg helpers (directly unit-testable) --------------------

/// Inline git credential helper: answers `get` from the command's environment, ignores `store` / `erase`, and holds no
/// secret.
const GIT_CREDENTIAL_HELPER: &str = r#"!f() { test "$1" = get && printf "username=%s\npassword=%s\n" "$SOLARI_GIT_USERNAME" "$SOLARI_GIT_PASSWORD"; }; f"#;

/// The `-c` options and environment for ONE git command that must authenticate to a private https remote. The
/// credentials travel as environment variables of that command and git reads them back through the inline helper, so
/// they are never in the URL (and so not in `.git/config`, a snapshot or a template), never in argv
/// (`/proc/<pid>/cmdline`) and never in a config file. `None` when there are no credentials.
pub(crate) fn git_auth(
    remote_url: &str,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<Option<(Vec<String>, HashMap<String, String>)>, SolariError> {
    let u = username.unwrap_or("");
    let p = password.unwrap_or("");
    if u.is_empty() && p.is_empty() {
        return Ok(None);
    }
    if u.contains(['\r', '\n', '\0']) || p.contains(['\r', '\n', '\0']) {
        return Err(SolariError::Git {
            message: "git credentials must not contain line breaks or NUL".to_string(),
        });
    }
    // The helper is bound to the remote's origin, so git never offers the password to another host: a redirect to a
    // different host, or a downgrade to http, gets nothing. Not http(s) (ssh, scp-style): no credentials apply.
    let parsed = match url::Url::parse(remote_url) {
        Ok(x) if x.scheme() == "http" || x.scheme() == "https" => x,
        _ => return Ok(None),
    };
    let Some(host) = parsed.host_str() else {
        return Ok(None);
    };
    let host = if host.contains(':') { format!("[{host}]") } else { host.to_string() };
    let origin = match parsed.port() {
        Some(port) => format!("{}://{}:{}", parsed.scheme(), host, port),
        None => format!("{}://{}", parsed.scheme(), host),
    };
    if origin.contains(['=', ' ', '\t', '\r', '\n']) {
        return Ok(None);
    }
    let cfg = vec![
        // The empty value first clears any helper configured on the guest, so only ours answers...
        "-c".to_string(),
        "credential.helper=".to_string(),
        // ...and only for this origin.
        "-c".to_string(),
        format!("credential.{origin}.helper={GIT_CREDENTIAL_HELPER}"),
    ];
    let mut env = HashMap::new();
    env.insert("GIT_TERMINAL_PROMPT".to_string(), "0".to_string());
    env.insert("SOLARI_GIT_USERNAME".to_string(), u.to_string());
    env.insert("SOLARI_GIT_PASSWORD".to_string(), p.to_string());
    Ok(Some((cfg, env)))
}

/// The git subcommand in an argv that may start with `-c k=v` pairs.
fn git_subcommand(args: &[String]) -> String {
    let mut i = 0;
    while i + 1 < args.len() && args[i] == "-c" {
        i += 2;
    }
    args.get(i).cloned().unwrap_or_default()
}

/// Refuse user text that git would parse as an option.
fn reject_option_like(what: &str, v: &str) -> Result<(), SolariError> {
    if v.starts_with('-') {
        return Err(SolariError::Git {
            message: format!("git {what} must not start with \"-\": {v:?}"),
        });
    }
    Ok(())
}

/// Parse `git status --porcelain=v1 -z --branch --untracked-files=all` output: NUL-terminated records, the first
/// `## branch...upstream [ahead N, behind M]`, then `XY path`, where a rename or copy (X or Y is R/C) is followed by one more
/// record holding the ORIGINAL path. Paths are verbatim (`-z` turns quoting off).
pub(crate) fn parse_status(out: &str) -> GitStatus {
    let mut st = GitStatus { clean: true, ..Default::default() };
    let mut records = out.split('\0').filter(|r| !r.is_empty());
    while let Some(rec) = records.next() {
        if let Some(head) = rec.strip_prefix("## ") {
            parse_status_header(head, &mut st);
            continue;
        }
        let mut chars = rec.chars();
        let (Some(x), Some(y)) = (chars.next(), chars.next()) else { continue };
        let path = rec.get(3..).unwrap_or("").to_string();
        let orig_path = if matches!(x, 'R' | 'C') || matches!(y, 'R' | 'C') { records.next().map(String::from) } else { None };
        if x == '!' && y == '!' {
            continue; // ignored files are not part of the status
        }
        st.entries.push(GitStatusEntry { path: path.clone(), orig_path, index: x, worktree: y });
        if x == '?' && y == '?' {
            st.untracked.push(path);
        } else if matches!((x, y), ('D', 'D') | ('A', 'U') | ('U', 'D') | ('U', 'A') | ('D', 'U') | ('A', 'A') | ('U', 'U')) {
            st.conflicted.push(path);
        } else {
            if x != ' ' {
                st.staged.push(path.clone());
            }
            if y != ' ' {
                st.modified.push(path);
            }
        }
    }
    st.clean = st.entries.is_empty();
    st
}

/// `main...origin/main [ahead 1, behind 2]` | `main...origin/main [gone]` | `main` | `No commits yet on main` | `HEAD (no branch)`.
fn parse_status_header(head: &str, st: &mut GitStatus) {
    if head.starts_with("HEAD (no branch)") {
        st.detached = true;
        return;
    }
    let head = head.strip_prefix("No commits yet on ").or_else(|| head.strip_prefix("Initial commit on ")).unwrap_or(head);
    let (refs, counts) = match head.find(" [") {
        Some(at) => (&head[..at], Some(&head[at + 2..])),
        None => (head, None),
    };
    match refs.split_once("...") {
        Some((branch, upstream)) => {
            st.branch = branch.to_string();
            st.upstream = Some(upstream.to_string());
        }
        None => st.branch = refs.to_string(),
    }
    if let Some(counts) = counts {
        st.ahead = extract_after(counts, "ahead ").unwrap_or(0);
        st.behind = extract_after(counts, "behind ").unwrap_or(0);
    }
}

/// Read the integer immediately following `marker` inside `s` (e.g. "ahead 2").
fn extract_after(s: &str, marker: &str) -> Option<i64> {
    let idx = s.find(marker)? + marker.len();
    let digits: String = s[idx..].chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// Parse the `%(HEAD)%1f%(refname:short)%1f%(objectname:short)%1f%(upstream:short)` for-each-ref format, one branch per line.
/// A detached HEAD is not a branch (`refs/heads` has no entry for it), so nothing is current then.
pub(crate) fn parse_branches(out: &str) -> Vec<GitBranch> {
    let mut branches = Vec::new();
    for line in out.split('\n') {
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split('\x1f').collect();
        let upstream = fields.get(3).copied().unwrap_or("");
        branches.push(GitBranch {
            name: fields.get(1).copied().unwrap_or("").to_string(),
            commit: fields.get(2).copied().unwrap_or("").to_string(),
            current: fields.first().copied() == Some("*"),
            upstream: if upstream.is_empty() { None } else { Some(upstream.to_string()) },
        });
    }
    branches
}

/// Parse the `%H%x1f%an%x1f%ae%x1f%aI%x1f%s%x1f%b%x1e` log format: records end with `\x1e`, fields with `\x1f`, so a multi-line
/// body belongs to its own commit.
pub(crate) fn parse_log(out: &str) -> Vec<GitCommit> {
    let mut commits = Vec::new();
    for rec in out.split('\x1e') {
        let rec = rec.trim_start_matches('\n');
        if rec.is_empty() {
            continue;
        }
        let f: Vec<&str> = rec.splitn(6, '\x1f').collect();
        commits.push(GitCommit {
            hash: f.first().copied().unwrap_or("").to_string(),
            author: f.get(1).copied().unwrap_or("").to_string(),
            email: f.get(2).copied().unwrap_or("").to_string(),
            date: f.get(3).copied().unwrap_or("").to_string(),
            message: f.get(4).copied().unwrap_or("").to_string(),
            body: f.get(5).copied().unwrap_or("").trim_end_matches('\n').to_string(),
        });
    }
    commits
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct RecordedCall {
        args: Vec<String>,
        cwd: Option<String>,
        env: Option<HashMap<String, String>>,
    }

    /// Stub runner mirroring `stubGit` in git.test.mjs: records calls and maps
    /// `git <args>` → a canned CommandResult via a responder closure.
    struct StubGit {
        calls: Mutex<Vec<RecordedCall>>,
        responder: Box<dyn Fn(&[String]) -> CommandResult + Send + Sync>,
    }

    impl StubGit {
        fn new<F>(f: F) -> Self
        where
            F: Fn(&[String]) -> CommandResult + Send + Sync + 'static,
        {
            StubGit {
                calls: Mutex::new(Vec::new()),
                responder: Box::new(f),
            }
        }
        fn ok(stdout: &str) -> CommandResult {
            CommandResult {
                exit_code: 0,
                stdout: stdout.to_string(),
                stderr: String::new(),
                timed_out: false, oom_killed: false, ..Default::default()
            }
        }
    }

    impl GitRunner for StubGit {
        fn run_git<'a>(
            &'a self,
            args: Vec<String>,
            cwd: Option<String>,
        ) -> Pin<Box<dyn Future<Output = Result<CommandResult, SolariError>> + Send + 'a>> {
            self.run_git_env(args, cwd, None)
        }

        fn run_git_env<'a>(
            &'a self,
            args: Vec<String>,
            cwd: Option<String>,
            env: Option<HashMap<String, String>>,
        ) -> Pin<Box<dyn Future<Output = Result<CommandResult, SolariError>> + Send + 'a>> {
            self.calls.lock().unwrap().push(RecordedCall {
                args: args.clone(),
                cwd,
                env,
            });
            let r = (self.responder)(&args);
            Box::pin(async move { Ok(r) })
        }
    }

    #[tokio::test]
    async fn status_parses_branch_ahead_behind_staged_modified_untracked() {
        // NOTE: leading spaces in porcelain lines are significant, so this is
        // built by concatenation (a `\`-newline continuation would eat them).
        let fixture = [
            "## main...origin/main [ahead 2, behind 1]",
            "M  staged.ts",
            " M modified.ts",
            "MM both.ts",
            "?? new.txt",
            "",
        ]
        .join("\0");
        let stub = StubGit::new(move |args| {
            if args[0] == "status" {
                return StubGit::ok(&fixture);
            }
            StubGit::ok("")
        });
        let git = Git::new(&stub);
        let st = git.status(Some("/repo")).await.unwrap();
        assert_eq!(st.branch, "main");
        assert_eq!(st.upstream.as_deref(), Some("origin/main"));
        assert_eq!(st.ahead, 2);
        assert_eq!(st.behind, 1);
        assert!(!st.detached);
        assert_eq!(st.staged, vec!["staged.ts", "both.ts"]);
        assert_eq!(st.modified, vec!["modified.ts", "both.ts"]);
        assert_eq!(st.untracked, vec!["new.txt"]);
        assert!(!st.clean);
    }

    #[tokio::test]
    async fn status_clean_detached_head() {
        let stub = StubGit::new(|args| {
            if args[0] == "status" {
                return StubGit::ok("## HEAD (no branch)\0");
            }
            StubGit::ok("")
        });
        let git = Git::new(&stub);
        let st = git.status(None).await.unwrap();
        assert!(st.detached);
        assert!(st.clean);
        assert_eq!(st.branch, "");
    }

    #[tokio::test]
    async fn status_fresh_repo_no_commits() {
        let stub = StubGit::new(|args| {
            if args[0] == "status" {
                return StubGit::ok("## No commits yet on trunk\0?? a.txt\0");
            }
            StubGit::ok("")
        });
        let git = Git::new(&stub);
        let st = git.status(None).await.unwrap();
        assert_eq!(st.branch, "trunk");
        assert_eq!(st.untracked, vec!["a.txt"]);
    }

    #[tokio::test]
    async fn branches_parses_name_commit_current() {
        let stub = StubGit::new(|args| {
            if args[0] == "for-each-ref" {
                return StubGit::ok("*\x1fmain\x1fabc1234\x1forigin/main\n \x1fdev\x1fdef5678\x1f\n");
            }
            StubGit::ok("")
        });
        let git = Git::new(&stub);
        let branches = git.branches(Some("/repo")).await.unwrap();
        assert_eq!(
            branches,
            vec![
                GitBranch { name: "main".into(), commit: "abc1234".into(), current: true, upstream: Some("origin/main".into()) },
                GitBranch { name: "dev".into(), commit: "def5678".into(), current: false, upstream: None },
            ]
        );
        let calls = stub.calls.lock().unwrap();
        assert!(calls[0].args.iter().any(|a| a.contains("%(refname:short)")));
    }

    #[tokio::test]
    async fn log_parses_unit_separated_records() {
        let stub = StubGit::new(|args| {
            if args[0] == "log" {
                return StubGit::ok(
                    "h1\x1fAda\x1fada@x\x1f2026-01-01T00:00:00+00:00\x1ffirst\x1fbody 1\nline 2\n\x1e\n\
                     h2\x1fLin\x1flin@x\x1f2026-01-02T00:00:00+00:00\x1fsecond\x1f\n\x1e\n",
                );
            }
            StubGit::ok("")
        });
        let git = Git::new(&stub);
        let log = git.log(GitLogOptions { cwd: Some("/repo".into()), max_count: Some(2) }).await.unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(
            log[0],
            GitCommit {
                hash: "h1".into(),
                author: "Ada".into(),
                email: "ada@x".into(),
                date: "2026-01-01T00:00:00+00:00".into(),
                message: "first".into(),
                body: "body 1\nline 2".into(),
            }
        );
        assert_eq!(log[1].message, "second");
        assert_eq!(log[1].body, "");
    }

    #[tokio::test]
    async fn commit_sets_identity_then_resolves_hash() {
        let stub = StubGit::new(|args| {
            if args.iter().any(|a| a == "rev-parse") {
                return StubGit::ok("deadbeef\n");
            }
            StubGit::ok("")
        });
        let git = Git::new(&stub);
        let hash = git
            .commit(
                "msg",
                GitCommitOptions {
                    cwd: Some("/repo".into()),
                    author: Some("Ada".into()),
                    email: Some("ada@x".into()),
                    all: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(hash, "deadbeef");
        let calls = stub.calls.lock().unwrap();
        let commit = calls.iter().find(|c| c.args.iter().any(|a| a == "commit")).unwrap();
        assert_eq!(
            commit.args,
            vec![
                "-c", "user.name=Ada", "-c", "user.email=ada@x", "commit", "-m", "msg", "-a"
            ]
        );
    }

    #[tokio::test]
    async fn clone_sends_credentials_by_env_and_origin_bound_helper_never_in_url_or_argv() {
        let stub = StubGit::new(|_| StubGit::ok(""));
        let git = Git::new(&stub);
        git.clone(
            "https://github.com/acme/repo.git",
            GitCloneOptions {
                branch: Some("main".into()),
                depth: Some(1),
                username: Some("u".into()),
                password: Some("p@ss word".into()),
                path: Some("dest".into()),
                cwd: Some("/work".into()),
            },
        )
        .await
        .unwrap();
        let calls = stub.calls.lock().unwrap();
        let c = &calls[0];
        assert_eq!(c.cwd.as_deref(), Some("/work"));
        assert_eq!(&c.args[0..3], &["-c", "credential.helper=", "-c"]);
        assert!(c.args[3].starts_with("credential.https://github.com.helper=!f()"), "got {}", c.args[3]);
        assert_eq!(
            &c.args[4..],
            &["clone", "--depth", "1", "--branch", "main", "--", "https://github.com/acme/repo.git", "dest"]
        );
        let env = c.env.as_ref().unwrap();
        assert_eq!(env["SOLARI_GIT_USERNAME"], "u");
        assert_eq!(env["SOLARI_GIT_PASSWORD"], "p@ss word");
        assert_eq!(env["GIT_TERMINAL_PROMPT"], "0");
        let argv = c.args.join(" ");
        assert!(!argv.contains("p@ss") && !argv.contains("p%40ss") && !argv.contains("u:"), "credential in argv: {argv}");
    }

    #[tokio::test]
    async fn clone_puts_dashdash_before_a_user_url_and_sends_no_helper_without_creds() {
        let stub = StubGit::new(|_| StubGit::ok(""));
        let git = Git::new(&stub);
        git.clone(
            "--upload-pack=touch /tmp/pwned",
            GitCloneOptions { path: Some("dest".into()), ..Default::default() },
        )
        .await
        .unwrap();
        let calls = stub.calls.lock().unwrap();
        assert_eq!(calls[0].args, vec!["clone", "--", "--upload-pack=touch /tmp/pwned", "dest"]);
        assert!(calls[0].env.is_none());
    }

    #[tokio::test]
    async fn credentials_with_line_breaks_are_refused_and_the_password_is_masked_in_errors() {
        let stub = StubGit::new(|_| StubGit::ok(""));
        let git = Git::new(&stub);
        let err = git
            .clone(
                "https://h/r.git",
                GitCloneOptions { username: Some("u".into()), password: Some("a\nurl=https://evil".into()), ..Default::default() },
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("line breaks"), "got {err}");
        let stub2 = StubGit::new(|_| CommandResult {
            exit_code: 128,
            stdout: String::new(),
            stderr: "fatal: unable to access: hunter2 rejected".into(),
            timed_out: false, oom_killed: false, ..Default::default()
        });
        let err = Git::new(&stub2)
            .clone(
                "https://h/r.git",
                GitCloneOptions { username: Some("u".into()), password: Some("hunter2".into()), ..Default::default() },
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("git clone failed") && !err.contains("hunter2"), "got {err}");
    }

    #[tokio::test]
    async fn push_resolves_the_remote_url_binds_the_helper_to_its_origin_and_keeps_the_secret_out_of_argv() {
        let stub = StubGit::new(|args| {
            if args[0] == "remote" {
                return StubGit::ok("https://git.example.com:8443/acme/repo.git\n");
            }
            StubGit::ok("")
        });
        let git = Git::new(&stub);
        git.push(GitRemoteOptions {
            cwd: Some("/repo".into()),
            username: Some("u".into()),
            password: Some("p@ss".into()),
            branch: Some("main".into()),
            ..Default::default()
        })
        .await
        .unwrap();
        let calls = stub.calls.lock().unwrap();
        assert_eq!(calls[0].args, vec!["remote", "get-url", "--", "origin"]);
        assert!(calls[0].env.is_none());
        let push = &calls[1].args;
        assert_eq!(&push[0..2], &["-c", "credential.helper="]);
        assert!(push[3].starts_with("credential.https://git.example.com:8443.helper=!f()"), "got {}", push[3]);
        assert_eq!(&push[4..], &["push", "--", "origin", "main"]);
        assert_eq!(calls[1].env.as_ref().unwrap()["SOLARI_GIT_PASSWORD"], "p@ss");
        assert!(!push.join(" ").contains("p@ss"));
    }

    #[tokio::test]
    async fn credentials_are_not_used_for_an_ssh_remote_and_a_plain_push_is_plain() {
        let stub = StubGit::new(|args| {
            if args[0] == "remote" {
                return StubGit::ok("git@github.com:acme/repo.git\n");
            }
            StubGit::ok("")
        });
        let git = Git::new(&stub);
        git.pull(GitRemoteOptions {
            username: Some("u".into()),
            password: Some("p".into()),
            remote: Some("upstream".into()),
            ..Default::default()
        })
        .await
        .unwrap();
        git.push(GitRemoteOptions::default()).await.unwrap();
        let calls = stub.calls.lock().unwrap();
        assert_eq!(calls[0].args, vec!["remote", "get-url", "--", "upstream"]);
        assert_eq!(calls[1].args, vec!["pull", "--", "upstream"]);
        assert!(calls[1].env.is_none());
        assert_eq!(calls[2].args, vec!["push", "--", "origin"]);
    }

    #[tokio::test]
    async fn option_like_remotes_branches_and_refs_are_refused_before_anything_runs() {
        let stub = StubGit::new(|_| StubGit::ok(""));
        let git = Git::new(&stub);
        assert!(git.push(GitRemoteOptions { remote: Some("--receive-pack=x".into()), ..Default::default() }).await.is_err());
        assert!(git.pull(GitRemoteOptions { branch: Some("--rebase".into()), ..Default::default() }).await.is_err());
        assert!(git.checkout("--orphan=x", None, false).await.is_err());
        git.checkout("dev", Some("/repo"), true).await.unwrap();
        let calls = stub.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "only the valid checkout reached the runner");
        assert_eq!(calls[0].args, vec!["checkout", "-b", "dev", "--"]);
    }

    #[tokio::test]
    async fn add_noop_on_empty_and_dashdash_separator() {
        let stub = StubGit::new(|_| StubGit::ok(""));
        let git = Git::new(&stub);
        git.add(&[], None).await.unwrap();
        assert_eq!(stub.calls.lock().unwrap().len(), 0);
        git.add(&["a.ts".into(), "-weird.ts".into()], Some("/repo")).await.unwrap();
        let calls = stub.calls.lock().unwrap();
        assert_eq!(calls[0].args, vec!["add", "--", "a.ts", "-weird.ts"]);
    }

    #[tokio::test]
    async fn nonzero_exit_surfaces_error_with_stderr() {
        let stub = StubGit::new(|_| CommandResult {
            exit_code: 128,
            stdout: String::new(),
            stderr: "fatal: not a git repository".into(),
            timed_out: false, oom_killed: false, ..Default::default()
        });
        let git = Git::new(&stub);
        let err = git.status(Some("/nope")).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not a git repository"), "got {msg}");
        assert!(msg.contains("git status failed (exit 128)"), "got {msg}");
    }
}

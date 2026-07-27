//! `git` namespace: safe, non-shell `git` invocations composed over the command
//! runner, with client-side parsers. Ported EXACTLY from the `git` namespace in
//! `handle.ts` (arg construction + status/branches/log parsers + auth-URL).
//!
//! The runner is abstracted behind [`GitRunner`] so the parsers/args can be
//! unit-tested with a stub that returns canned [`CommandResult`]s (no live WS),
//! exactly like the TS `git.test.mjs` stubs `runCommand`.

use std::future::Future;
use std::pin::Pin;

use crate::error::SolariError;
use crate::http::encode_uri_component;
use crate::types::{CommandResult, GitBranch, GitCommit, GitStatus};

/// The seam the git namespace runs commands through. `Sandbox` implements it by
/// delegating to `commands.run("git", …)`; tests implement a recording stub.
pub trait GitRunner: Send + Sync {
    fn run_git<'a>(
        &'a self,
        args: Vec<String>,
        cwd: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<CommandResult, SolariError>> + Send + 'a>>;
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
        let sub = args.first().cloned().unwrap_or_default();
        let r = self.runner.run_git(args, cwd).await?;
        if r.exit_code != 0 {
            let detail = {
                let s = r.stderr.trim();
                if !s.is_empty() {
                    s.to_string()
                } else {
                    r.stdout.trim().to_string()
                }
            };
            return Err(SolariError::Git {
                message: format!("git {} failed (exit {}): {}", sub, r.exit_code, detail),
            });
        }
        Ok(r.stdout)
    }

    /// Clone `url` into `opts.path` under `opts.cwd`.
    pub async fn clone(&self, url: &str, opts: GitCloneOptions) -> Result<(), SolariError> {
        let mut args = vec!["clone".to_string()];
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
        args.push(auth_url(url, opts.username.as_deref(), opts.password.as_deref()));
        if let Some(p) = &opts.path {
            args.push(p.clone());
        }
        self.must_git(args, opts.cwd).await.map(|_| ())
    }

    /// Parsed working-tree status.
    pub async fn status(&self, cwd: Option<&str>) -> Result<GitStatus, SolariError> {
        let out = self
            .must_git(
                vec![
                    "status".into(),
                    "--porcelain=v1".into(),
                    "--branch".into(),
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
        let mut cfg: Vec<String> = Vec::new();
        if opts.username.is_some() || opts.password.is_some() {
            // One-off HTTPS auth without persisting a credential.
            let plain = self
                .runner
                .run_git(
                    vec!["remote".into(), "get-url".into(), remote.clone()],
                    opts.cwd.clone(),
                )
                .await?
                .stdout
                .trim()
                .to_string();
            if !plain.is_empty() {
                let authed = auth_url(&plain, opts.username.as_deref(), opts.password.as_deref());
                if authed != plain {
                    cfg.push("-c".into());
                    cfg.push(format!("url.{authed}.insteadOf={plain}"));
                }
            }
        }
        let mut args = cfg;
        args.push(op.to_string());
        args.push(remote);
        if let Some(b) = &opts.branch {
            args.push(b.clone());
        }
        self.must_git(args, opts.cwd).await.map(|_| ())
    }

    /// Check out an existing ref, or create a branch with `create: true`.
    pub async fn checkout(
        &self,
        r#ref: &str,
        cwd: Option<&str>,
        create: bool,
    ) -> Result<(), SolariError> {
        let mut args = vec!["checkout".to_string()];
        if create {
            args.push("-b".into());
        }
        args.push(r#ref.to_string());
        self.must_git(args, cwd.map(String::from)).await.map(|_| ())
    }

    /// List local branches.
    pub async fn branches(&self, cwd: Option<&str>) -> Result<Vec<GitBranch>, SolariError> {
        let out = self
            .must_git(
                vec![
                    "branch".into(),
                    "--format=%(HEAD)%1f%(refname:short)%1f%(objectname:short)".into(),
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
            "--format=%H%x1f%an%x1f%ae%x1f%aI%x1f%s".to_string(),
        ];
        if let Some(n) = opts.max_count {
            if n > 0 {
                args.push(format!("--max-count={n}"));
            }
        }
        let out = self.must_git(args, opts.cwd).await?;
        Ok(parse_log(&out))
    }
}

// --- pure parsers + arg helpers (directly unit-testable) --------------------

/// Splice basic-auth creds into an https remote URL. On any parse failure,
/// returns the URL unchanged. `p@ss word` → `p%40ss%20word`.
pub(crate) fn auth_url(url: &str, username: Option<&str>, password: Option<&str>) -> String {
    let has_user = username.map(|u| !u.is_empty()).unwrap_or(false);
    let has_pass = password.map(|p| !p.is_empty()).unwrap_or(false);
    if !has_user && !has_pass {
        return url.to_string();
    }
    let mut parsed = match url::Url::parse(url) {
        Ok(u) => u,
        Err(_) => return url.to_string(),
    };
    if let Some(u) = username {
        // encodeURIComponent, then set — mirrors the TS `u.username = encodeURIComponent(...)`.
        if parsed.set_username(&encode_uri_component(u)).is_err() {
            return url.to_string();
        }
    }
    if let Some(p) = password {
        if parsed.set_password(Some(&encode_uri_component(p))).is_err() {
            return url.to_string();
        }
    }
    parsed.to_string()
}

/// Parse `git status --porcelain=v1 --branch` output.
pub(crate) fn parse_status(out: &str) -> GitStatus {
    let mut st = GitStatus {
        branch: String::new(),
        detached: false,
        ahead: 0,
        behind: 0,
        staged: Vec::new(),
        modified: Vec::new(),
        untracked: Vec::new(),
        clean: true,
    };
    for line in out.split('\n').filter(|l| !l.is_empty()) {
        if let Some(head) = line.strip_prefix("## ") {
            if head.starts_with("HEAD (no branch)") {
                st.detached = true;
            } else {
                // "main...origin/main [ahead 1, behind 2]" | "main" | "No commits yet on main"
                let body = head
                    .strip_prefix("No commits yet on ")
                    .unwrap_or(head)
                    .to_string();
                let before_dots = body.split("...").next().unwrap_or("");
                st.branch = before_dots.split(' ').next().unwrap_or("").to_string();
                if let (Some(open), Some(close)) = (head.find('['), head.find(']')) {
                    if close > open {
                        let inner = &head[open + 1..close];
                        st.ahead = extract_after(inner, "ahead ").unwrap_or(0);
                        st.behind = extract_after(inner, "behind ").unwrap_or(0);
                    }
                }
            }
            continue;
        }
        if line.len() < 3 {
            continue;
        }
        let xy: Vec<char> = line.chars().take(2).collect();
        let path = &line[3..];
        if xy[0] == '?' && xy[1] == '?' {
            st.untracked.push(path.to_string());
        } else {
            let index = xy[0];
            let work = xy[1];
            if index != ' ' && index != '?' {
                st.staged.push(path.to_string());
            }
            if work != ' ' && work != '?' {
                st.modified.push(path.to_string());
            }
        }
    }
    st.clean = st.staged.is_empty() && st.modified.is_empty() && st.untracked.is_empty();
    st
}

/// Read the integer immediately following `marker` inside `s` (e.g. "ahead 2").
fn extract_after(s: &str, marker: &str) -> Option<i64> {
    let idx = s.find(marker)? + marker.len();
    let digits: String = s[idx..].chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// Parse the `%(HEAD)%1f%(refname:short)%1f%(objectname:short)` branch format.
pub(crate) fn parse_branches(out: &str) -> Vec<GitBranch> {
    let mut branches = Vec::new();
    for line in out.split('\n') {
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split('\x1f').collect();
        let head = fields.first().copied().unwrap_or("");
        let name = fields.get(1).copied().unwrap_or("");
        let commit = fields.get(2).copied().unwrap_or("");
        branches.push(GitBranch {
            name: name.to_string(),
            commit: commit.to_string(),
            current: head == "*",
        });
    }
    branches
}

/// Parse the unit-separated `git log` format.
pub(crate) fn parse_log(out: &str) -> Vec<GitCommit> {
    let mut commits = Vec::new();
    for line in out.split('\n') {
        if line.trim().is_empty() {
            continue;
        }
        let f: Vec<&str> = line.split('\x1f').collect();
        commits.push(GitCommit {
            hash: f.first().copied().unwrap_or("").to_string(),
            author: f.get(1).copied().unwrap_or("").to_string(),
            email: f.get(2).copied().unwrap_or("").to_string(),
            date: f.get(3).copied().unwrap_or("").to_string(),
            message: f.get(4).copied().unwrap_or("").to_string(),
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
            }
        }
    }

    impl GitRunner for StubGit {
        fn run_git<'a>(
            &'a self,
            args: Vec<String>,
            cwd: Option<String>,
        ) -> Pin<Box<dyn Future<Output = Result<CommandResult, SolariError>> + Send + 'a>> {
            self.calls.lock().unwrap().push(RecordedCall {
                args: args.clone(),
                cwd,
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
        .join("\n");
        let stub = StubGit::new(move |args| {
            if args[0] == "status" {
                return StubGit::ok(&fixture);
            }
            StubGit::ok("")
        });
        let git = Git::new(&stub);
        let st = git.status(Some("/repo")).await.unwrap();
        assert_eq!(st.branch, "main");
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
                return StubGit::ok("## HEAD (no branch)\n");
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
                return StubGit::ok("## No commits yet on trunk\n?? a.txt\n");
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
            if args[0] == "branch" {
                return StubGit::ok("*\x1fmain\x1fabc1234\n \x1fdev\x1fdef5678\n");
            }
            StubGit::ok("")
        });
        let git = Git::new(&stub);
        let branches = git.branches(Some("/repo")).await.unwrap();
        assert_eq!(
            branches,
            vec![
                GitBranch { name: "main".into(), commit: "abc1234".into(), current: true },
                GitBranch { name: "dev".into(), commit: "def5678".into(), current: false },
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
                    "h1\x1fAda\x1fada@x\x1f2026-01-01T00:00:00+00:00\x1ffirst\n\
                     h2\x1fLin\x1flin@x\x1f2026-01-02T00:00:00+00:00\x1fsecond\n",
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
            }
        );
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
    async fn clone_injects_basic_auth_into_https_remote() {
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
        assert_eq!(&c.args[0..5], &["clone", "--depth", "1", "--branch", "main"]);
        let url = &c.args[5];
        assert!(
            url.starts_with("https://u:p%40ss%20word@github.com/"),
            "got {url}"
        );
        assert_eq!(c.args[6], "dest");
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
        });
        let git = Git::new(&stub);
        let err = git.status(Some("/nope")).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not a git repository"), "got {msg}");
        assert!(msg.contains("git status failed (exit 128)"), "got {msg}");
    }

    #[test]
    fn auth_url_unchanged_without_creds_or_on_parse_failure() {
        assert_eq!(auth_url("https://x/y.git", None, None), "https://x/y.git");
        // scp-style / non-URL: left untouched.
        assert_eq!(
            auth_url("git@github.com:acme/repo.git", Some("u"), Some("p")),
            "git@github.com:acme/repo.git"
        );
    }
}

//! Real git against real HTTP servers: the credentials reach git through the environment and the origin-bound inline
//! credential helper, and nothing about them is left in `.git/config` or argv; a redirect to another host gets nothing.
//! Skipped where git or git-http-backend is absent.

use std::collections::HashMap;
use std::future::Future;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;

use base64::Engine;
use solari::{CommandResult, Git, GitCloneOptions, GitLogOptions, GitRemoteOptions, GitRunner, SolariError};

const USER: &str = "alice";
const PASS: &str = "p@ss w0rd/ü";

fn backend() -> Option<PathBuf> {
    if Command::new("git").arg("--version").output().is_err() {
        return None;
    }
    ["/usr/lib/git-core/git-http-backend", "/usr/libexec/git-core/git-http-backend"]
        .iter()
        .map(PathBuf::from)
        .find(|p| p.exists())
}

fn sh(args: &[&str]) -> String {
    let out = Command::new(args[0]).args(&args[1..]).output().unwrap();
    assert!(out.status.success(), "{args:?}: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Runs the real git with the env the SDK asked for, on a clean HOME, and records every argv.
struct RealGit {
    home: PathBuf,
    argvs: Mutex<Vec<Vec<String>>>,
}

impl GitRunner for RealGit {
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
        self.argvs.lock().unwrap().push(args.clone());
        let out = {
            let mut c = Command::new("git");
            c.args(&args)
                .env_clear()
                .env("PATH", std::env::var("PATH").unwrap_or_default())
                .env("HOME", &self.home)
                .env("GIT_CONFIG_NOSYSTEM", "1");
            if let Some(cwd) = cwd {
                c.current_dir(cwd);
            }
            for (k, v) in env.unwrap_or_default() {
                c.env(k, v);
            }
            c.output().unwrap()
        };
        let r = CommandResult {
            exit_code: out.status.code().unwrap_or(-1) as i64,
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            timed_out: false, oom_killed: false, ..Default::default()
        };
        Box::pin(async move { Ok(r) })
    }
}

fn read_request(s: &mut TcpStream) -> Option<(String, String, HashMap<String, String>, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let end = loop {
        let n = s.read(&mut tmp).ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break p;
        }
    };
    let head = String::from_utf8_lossy(&buf[..end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut first = lines.next()?.split(' ');
    let method = first.next()?.to_string();
    let target = first.next()?.to_string();
    let headers: HashMap<String, String> = lines
        .filter_map(|l| l.split_once(':').map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string())))
        .collect();
    let mut body = buf[end + 4..].to_vec();
    let want: usize = headers.get("content-length").and_then(|v| v.parse().ok()).unwrap_or(0);
    while body.len() < want {
        let n = s.read(&mut tmp).ok()?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    Some((method, target, headers, body))
}

fn respond(s: &mut TcpStream, status: &str, headers: &[(String, String)], body: &[u8]) {
    let mut out = format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n", body.len());
    for (k, v) in headers {
        out.push_str(&format!("{k}: {v}\r\n"));
    }
    out.push_str("\r\n");
    let _ = s.write_all(out.as_bytes());
    let _ = s.write_all(body);
}

/// A git smart-HTTP server (git http-backend as CGI) behind basic auth. Returns its base URL and the Authorization
/// headers it saw.
fn git_server(backend: PathBuf, root: PathBuf) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen2 = seen.clone();
    let want = format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(format!("{USER}:{PASS}")));
    thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(mut s) = conn else { continue };
            let Some((method, target, headers, body)) = read_request(&mut s) else { continue };
            let auth = headers.get("authorization").cloned().unwrap_or_default();
            seen2.lock().unwrap().push(auth.clone());
            if auth != want {
                respond(&mut s, "401 Unauthorized", &[("WWW-Authenticate".into(), "Basic realm=\"git\"".into())], b"");
                continue;
            }
            let (path, query) = target.split_once('?').unwrap_or((target.as_str(), ""));
            let mut child = Command::new(&backend)
                .env_clear()
                .env("PATH", std::env::var("PATH").unwrap_or_default())
                .env("GIT_PROJECT_ROOT", &root)
                .env("GIT_HTTP_EXPORT_ALL", "1")
                .env("REQUEST_METHOD", &method)
                .env("PATH_INFO", path)
                .env("QUERY_STRING", query)
                .env("CONTENT_TYPE", headers.get("content-type").cloned().unwrap_or_default())
                .env("CONTENT_LENGTH", body.len().to_string())
                .env("REMOTE_USER", USER)
                .env("REMOTE_ADDR", "127.0.0.1")
                .env("HTTP_CONTENT_ENCODING", headers.get("content-encoding").cloned().unwrap_or_default())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            child.stdin.take().unwrap().write_all(&body).unwrap();
            let out = child.wait_with_output().unwrap().stdout;
            let sep = out.windows(4).position(|w| w == b"\r\n\r\n").unwrap_or(0);
            let head = String::from_utf8_lossy(&out[..sep]).into_owned();
            let mut status = "200 OK".to_string();
            let mut hs = Vec::new();
            for line in head.split("\r\n") {
                if let Some((k, v)) = line.split_once(':') {
                    if k.eq_ignore_ascii_case("status") {
                        status = v.trim().to_string();
                    } else if !k.eq_ignore_ascii_case("content-length") {
                        hs.push((k.to_string(), v.trim().to_string()));
                    }
                }
            }
            respond(&mut s, &status, &hs, &out[(sep + 4).min(out.len())..]);
        }
    });
    (url, seen)
}

fn temp_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("solari-gitreal-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[tokio::test]
async fn private_clone_and_push_credentials_via_env_and_helper_only() {
    let Some(be) = backend() else { return };
    let dir = temp_dir("priv");
    let root = dir.join("srv");
    let bare = root.join("repo.git");
    sh(&["git", "init", "-q", "--bare", "-b", "main", bare.to_str().unwrap()]);
    sh(&["git", "-C", bare.to_str().unwrap(), "config", "http.receivepack", "true"]);
    let seed = dir.join("seed");
    sh(&["git", "init", "-q", "-b", "main", seed.to_str().unwrap()]);
    sh(&["git", "-C", seed.to_str().unwrap(), "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "first"]);
    sh(&["git", "-C", seed.to_str().unwrap(), "push", "-q", bare.to_str().unwrap(), "main"]);
    let (url, seen) = git_server(be, root);
    let home = dir.join("home");
    std::fs::create_dir_all(&home).unwrap();
    let runner = RealGit { home: home.clone(), argvs: Mutex::new(Vec::new()) };
    let git = Git::new(&runner);
    let plain = format!("{url}/repo.git");

    assert!(git.clone(&plain, GitCloneOptions { path: Some(dir.join("nocreds").to_string_lossy().into()), ..Default::default() }).await.is_err());
    let bad = git
        .clone(
            &plain,
            GitCloneOptions {
                path: Some(dir.join("bad").to_string_lossy().into()),
                username: Some(USER.into()),
                password: Some(format!("wrong-{PASS}")),
                ..Default::default()
            },
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(!bad.contains(&format!("wrong-{PASS}")), "{bad}");

    let dest = dir.join("work");
    git.clone(
        &plain,
        GitCloneOptions { path: Some(dest.to_string_lossy().into()), username: Some(USER.into()), password: Some(PASS.into()), ..Default::default() },
    )
    .await
    .unwrap();
    let want = format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(format!("{USER}:{PASS}")));
    assert!(seen.lock().unwrap().contains(&want), "the server received the credentials");
    let cfg = std::fs::read_to_string(dest.join(".git/config")).unwrap();
    assert!(cfg.contains(&format!("url = {plain}")), "{cfg}");
    for needle in [PASS, "p%40ss", "alice:", "credential"] {
        assert!(!cfg.contains(needle), ".git/config contains {needle:?}:\n{cfg}");
    }
    assert!(!Path::new(&home.join(".git-credentials")).exists());

    sh(&["git", "-C", dest.to_str().unwrap(), "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "second"]);
    git.push(GitRemoteOptions {
        cwd: Some(dest.to_string_lossy().into()),
        username: Some(USER.into()),
        password: Some(PASS.into()),
        branch: Some("main".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    assert!(sh(&["git", "-C", bare.to_str().unwrap(), "log", "--format=%s", "main"]).contains("second"));
    assert!(!std::fs::read_to_string(dest.join(".git/config")).unwrap().contains(PASS));
    for a in runner.argvs.lock().unwrap().iter() {
        let j = a.join(" ");
        assert!(!j.contains(PASS) && !j.contains("p%40ss"), "credential in argv: {j}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_redirect_to_another_host_never_receives_the_password_and_hostile_urls_are_inert() {
    if backend().is_none() {
        return;
    }
    let dir = temp_dir("redir");
    // B is where A redirects to; it demands auth and records every Authorization header it sees.
    let b = TcpListener::bind("127.0.0.1:0").unwrap();
    let b_port = b.local_addr().unwrap().port();
    let b_seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let b2 = b_seen.clone();
    thread::spawn(move || {
        for conn in b.incoming() {
            let Ok(mut s) = conn else { continue };
            let Some((_, _, headers, _)) = read_request(&mut s) else { continue };
            b2.lock().unwrap().push(headers.get("authorization").cloned().unwrap_or_default());
            respond(&mut s, "401 Unauthorized", &[("WWW-Authenticate".into(), "Basic realm=\"other\"".into())], b"");
        }
    });
    // A answers every request with a 302 to B on a different host name.
    let a = TcpListener::bind("127.0.0.1:0").unwrap();
    let a_port = a.local_addr().unwrap().port();
    thread::spawn(move || {
        for conn in a.incoming() {
            let Ok(mut s) = conn else { continue };
            let Some((_, target, _, _)) = read_request(&mut s) else { continue };
            respond(&mut s, "302 Found", &[("Location".into(), format!("http://localhost:{b_port}{target}"))], b"");
        }
    });
    let runner = RealGit { home: dir.clone(), argvs: Mutex::new(Vec::new()) };
    let git = Git::new(&runner);
    let err = git
        .clone(
            &format!("http://127.0.0.1:{a_port}/repo.git"),
            GitCloneOptions {
                path: Some(dir.join("x").to_string_lossy().into()),
                username: Some(USER.into()),
                password: Some(PASS.into()),
                ..Default::default()
            },
        )
        .await;
    assert!(err.is_err());
    let seen = b_seen.lock().unwrap();
    assert!(!seen.is_empty(), "git did not follow the redirect, so the test proves nothing");
    assert!(seen.iter().all(|h| h.is_empty()), "the redirect target received an Authorization header: {seen:?}");

    let mark = dir.join("pwned");
    let r = git
        .clone(&format!("--upload-pack=touch {}", mark.display()), GitCloneOptions { path: Some(dir.join("y").to_string_lossy().into()), ..Default::default() })
        .await;
    assert!(r.is_err() && !mark.exists(), "the --upload-pack option was executed");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- status / branches / log against REAL git (no stub output) --------------------------------------------------------------
// The parsers used to lose information: quoted paths, a rename as one "old -> new" string, an untracked directory as "dir/", a
// conflict listed as staged AND modified, a detached HEAD listed as a branch, log on a repository with no commit failing, and the
// message body dropped.

fn parse_repo(tag: &str) -> Option<(PathBuf, PathBuf)> {
    if Command::new("git").arg("--version").output().is_err() {
        return None;
    }
    let d = temp_dir(tag);
    let home = d.join("home");
    std::fs::create_dir_all(&home).unwrap();
    let repo = d.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    g(&repo, &["init", "-q", "-b", "main"]);
    g(&repo, &["config", "user.name", "T"]);
    g(&repo, &["config", "user.email", "t@e.x"]);
    Some((repo, home))
}

fn g(repo: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(repo)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn write(repo: &Path, name: &str, body: &str) {
    let p = repo.join(name);
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(p, body).unwrap();
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

#[tokio::test]
async fn real_git_empty_repository_log_is_empty_and_status_is_clean() {
    let Some((repo, home)) = parse_repo("empty") else { return };
    let runner = RealGit { home, argvs: Mutex::new(Vec::new()) };
    let git = Git::new(&runner);
    let cwd = repo.to_string_lossy().to_string();
    assert!(git.log(GitLogOptions { cwd: Some(cwd.clone()), max_count: None }).await.unwrap().is_empty(), "log on a repository with no commit is []");
    assert!(git.branches(Some(&cwd)).await.unwrap().is_empty());
    let st = git.status(Some(&cwd)).await.unwrap();
    assert_eq!(st.branch, "main");
    assert!(st.clean && st.entries.is_empty());
}

#[tokio::test]
async fn real_git_status_paths_are_verbatim_and_untracked_directories_list_their_files() {
    let Some((repo, home)) = parse_repo("paths") else { return };
    let runner = RealGit { home, argvs: Mutex::new(Vec::new()) };
    let git = Git::new(&runner);
    for (n, b) in [("a b.txt", "1"), ("é ü.txt", "2"), ("tab\there", "3"), ("nd/inner", "n"), ("nd/deep file", "n")] {
        write(&repo, n, b);
    }
    let st = git.status(Some(&repo.to_string_lossy())).await.unwrap();
    assert_eq!(
        sorted(st.untracked),
        sorted(vec!["a b.txt".into(), "é ü.txt".into(), "tab\there".into(), "nd/inner".into(), "nd/deep file".into()]),
        "verbatim paths (no quotes, no escapes) and files, not 'nd/'"
    );
}

#[tokio::test]
async fn real_git_status_rename_is_the_new_path_with_the_old_path_in_entries() {
    let Some((repo, home)) = parse_repo("rename") else { return };
    let runner = RealGit { home, argvs: Mutex::new(Vec::new()) };
    let git = Git::new(&runner);
    write(&repo, "plain", "2");
    write(&repo, "gone me", "1");
    g(&repo, &["add", "-A"]);
    g(&repo, &["commit", "-q", "-m", "first"]);
    write(&repo, "plain", "2\nmore");
    g(&repo, &["mv", "plain", "renamed file"]);
    write(&repo, "staged", "s");
    g(&repo, &["add", "staged"]);
    g(&repo, &["rm", "-q", "--cached", "gone me"]);
    let st = git.status(Some(&repo.to_string_lossy())).await.unwrap();
    assert_eq!(sorted(st.staged.clone()), vec!["gone me".to_string(), "renamed file".into(), "staged".into()]);
    assert_eq!(st.modified, vec!["renamed file".to_string()], "RM: the rename is staged and the content changed again");
    let ren = st.entries.iter().find(|e| e.path == "renamed file").unwrap();
    assert_eq!((ren.orig_path.as_deref(), ren.index, ren.worktree), (Some("plain"), 'R', 'M'));
    assert!(!st.staged.iter().any(|p| p.contains("->")), "no 'old -> new' strings");
}

#[tokio::test]
async fn real_git_status_a_merge_conflict_is_listed_once_as_conflicted() {
    let Some((repo, home)) = parse_repo("conflict") else { return };
    let runner = RealGit { home, argvs: Mutex::new(Vec::new()) };
    let git = Git::new(&runner);
    write(&repo, "base", "0");
    g(&repo, &["add", "-A"]);
    g(&repo, &["commit", "-q", "-m", "base"]);
    g(&repo, &["checkout", "-q", "-b", "c1"]);
    write(&repo, "conflict", "A");
    g(&repo, &["add", "conflict"]);
    g(&repo, &["commit", "-q", "-m", "c1"]);
    g(&repo, &["checkout", "-q", "main"]);
    write(&repo, "conflict", "B");
    g(&repo, &["add", "conflict"]);
    g(&repo, &["commit", "-q", "-m", "c2"]);
    g(&repo, &["merge", "c1"]);
    let st = git.status(Some(&repo.to_string_lossy())).await.unwrap();
    assert_eq!(st.conflicted, vec!["conflict".to_string()]);
    assert!(st.staged.is_empty() && st.modified.is_empty(), "listed once, only as conflicted");
    assert!(!st.clean);
}

#[tokio::test]
async fn real_git_status_detached_head_and_upstream_counts_and_branches() {
    let Some((repo, home)) = parse_repo("upstream") else { return };
    let runner = RealGit { home, argvs: Mutex::new(Vec::new()) };
    let git = Git::new(&runner);
    let cwd = repo.to_string_lossy().to_string();
    write(&repo, "f", "1");
    g(&repo, &["add", "-A"]);
    g(&repo, &["commit", "-q", "-m", "one"]);
    g(&repo, &["branch", "feat/x"]);
    g(&repo, &["branch", "topic"]);
    g(&repo, &["remote", "add", "origin", repo.to_str().unwrap()]);
    g(&repo, &["fetch", "-q", "origin"]);
    g(&repo, &["branch", "--set-upstream-to=origin/main", "main"]);
    write(&repo, "f", "2");
    g(&repo, &["commit", "-qam", "two"]);
    let st = git.status(Some(&cwd)).await.unwrap();
    assert_eq!((st.branch.as_str(), st.upstream.as_deref(), st.ahead, st.behind), ("main", Some("origin/main"), 1, 0));
    let bs = git.branches(Some(&cwd)).await.unwrap();
    assert_eq!(sorted(bs.iter().map(|b| b.name.clone()).collect()), vec!["feat/x".to_string(), "main".into(), "topic".into()]);
    let main = bs.iter().find(|b| b.name == "main").unwrap();
    assert!(main.current);
    assert_eq!(main.upstream.as_deref(), Some("origin/main"));
    g(&repo, &["checkout", "-q", "--detach", "HEAD~1"]);
    let st = git.status(Some(&cwd)).await.unwrap();
    assert!(st.detached && st.branch.is_empty());
    let bs = git.branches(Some(&cwd)).await.unwrap();
    assert!(!bs.iter().any(|b| b.name.starts_with('(')), "no '(HEAD detached at ...)' pseudo-branch");
    assert!(!bs.iter().any(|b| b.current), "nothing is current on a detached HEAD");
}

#[tokio::test]
async fn real_git_log_keeps_the_subject_and_the_body() {
    let Some((repo, home)) = parse_repo("log") else { return };
    let runner = RealGit { home, argvs: Mutex::new(Vec::new()) };
    let git = Git::new(&runner);
    let cwd = repo.to_string_lossy().to_string();
    write(&repo, "f", "1");
    g(&repo, &["add", "-A"]);
    g(&repo, &["commit", "-q", "-m", "subject line", "-m", "body paragraph 1\nsecond line", "-m", "paragraph 2"]);
    write(&repo, "f", "2");
    g(&repo, &["commit", "-qam", "plain"]);
    let log = git.log(GitLogOptions { cwd: Some(cwd.clone()), max_count: None }).await.unwrap();
    assert_eq!(log.len(), 2);
    assert_eq!((log[0].message.as_str(), log[0].body.as_str()), ("plain", ""));
    assert_eq!(log[1].message, "subject line");
    assert_eq!(log[1].body, "body paragraph 1\nsecond line\n\nparagraph 2");
    assert_eq!(git.log(GitLogOptions { cwd: Some(cwd), max_count: Some(1) }).await.unwrap().len(), 1);
}

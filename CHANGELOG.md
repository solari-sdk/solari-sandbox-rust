# Changelog

## 0.3.2

### Fixed
- **Security: `git().clone` / `push` / `pull` credentials no longer reach the clone URL, `.git/config` or the process list.** With a username/password, `clone` used to put them in the URL, so git saved them as `remote.origin.url` (and from there into snapshots and templates) and showed them in `/proc/*/cmdline`; push and pull put them in a `-c url.….insteadOf=` argument. They now travel as environment variables of that one command and git reads them through an inline credential helper bound to the remote's origin, so nothing is left in the repository, in a config file or on a command line, and git never offers the password to another host (a redirect to a different host gets nothing). If you cloned a private repo with credentials before this release, check `.git/config` in existing snapshots and templates and rotate the token if it is there. A username or password with a line break is refused, and the password is masked in error messages. Authenticated git needs the live control connection, so on a `shared` sandbox only public remotes work. `GitRunner` gains `run_git_env` (default: refuses an environment, so credentials are never silently dropped); existing implementors keep compiling.
- **git commands no longer read user text as options.** `clone`, `push`, `pull` and `checkout` put `--` before the repository, remote and ref, and refuse a remote, branch or ref that starts with `-` (`clone("--upload-pack=cmd", …)` ran `cmd` in the sandbox; `checkout("--orphan=x")` created a branch).
- **Streamed command output no longer corrupts non-ASCII text.** Each output chunk was decoded on its own, so a multi-byte character split across two chunks became U+FFFD. Each stream now has its own streaming decoder, and bytes held back at the end of a command are flushed on exit.

## 0.3.1

### Added
- `ClientOptions::with_api_key(key)` and `DEFAULT_BASE_URL` (`https://api.getsolari.com`).

### Changed
- An empty `base_url` now means the hosted Solari API, matching the TypeScript and Python SDKs.
  `ClientOptions::new(key, base_url)` with an explicit URL behaves as before.

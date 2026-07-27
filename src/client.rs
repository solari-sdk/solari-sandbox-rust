//! `Client` (REST create/connect/get/kill) and the `Sandbox` handle exposing
//! `.commands()`, `.files()`, `.code()`, and `.git()`. Mirrors
//! `sandbox-client.ts` (REST shapes) + `SessionHandle` (namespaces).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::channel::ControlChannel;
use crate::code::Code;
use crate::commands::{run_command, Commands, RunOptions};
use crate::error::SolariError;
use crate::files::Files;
use crate::git::{Git, GitRunner};
use crate::http::{encode_uri_component, new_idempotency_key, HttpOptions, HttpTransport};
use crate::types::{
    CommandResult, CreateSandboxRequest, CreateSandboxResponse, Lifecycle, ResumeResponse,
    SandboxView, VolumeAttachment,
};

/// Options for constructing a [`Client`].
#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub api_key: String,
    pub base_url: String,
    /// Per-call control-WS RPC timeout override (ms). Defaults to 300000.
    pub call_timeout_ms: Option<u64>,
}

impl ClientOptions {
    pub fn new(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        ClientOptions {
            api_key: api_key.into(),
            base_url: base_url.into(),
            call_timeout_ms: None,
        }
    }
}

/// Options accepted by `POST /sandboxes` (headless sandbox flavour).
#[derive(Default, Clone)]
pub struct CreateOptions {
    pub template: Option<String>,
    pub cpu: Option<u32>,
    pub mem_mb: Option<u32>,
    pub disk_gb: Option<u32>,
    pub envs: Option<HashMap<String, String>>,
    pub metadata: Option<HashMap<String, String>>,
    pub timeout_ms: Option<u64>,
    pub from_snapshot: Option<String>,
    pub lifecycle: Option<Lifecycle>,
    /// Initial display resolution, e.g. `"1280x720"`. Desktops only — a headless
    /// sandbox has no display.
    pub resolution: Option<String>,
    /// Record the session server-side; the create response carries a presigned
    /// playback URL. Desktops only.
    pub record: Option<bool>,
    /// Persistent volumes to mount before the session starts.
    pub volumes: Option<Vec<VolumeAttachment>>,
}

impl CreateOptions {
    /// Build the `POST /sandboxes` body for a given VM flavour.
    ///
    /// THE SINGLE PLACE an option becomes a wire field, and therefore the place
    /// the cross-language wire contract is pinned (see `tests/contract_wire.rs`).
    /// A field added above but not mapped here is silently invisible on the wire.
    pub(crate) fn into_request(self, kind: &str) -> CreateSandboxRequest {
        CreateSandboxRequest {
            template: self.template,
            kind: Some(kind.to_string()),
            cpu: self.cpu,
            mem_mb: self.mem_mb,
            disk_gb: self.disk_gb,
            envs: self.envs,
            metadata: self.metadata,
            timeout_ms: self.timeout_ms,
            from_snapshot: self.from_snapshot,
            lifecycle: self.lifecycle,
            resolution: self.resolution,
            record: self.record,
            volumes: self.volumes,
        }
    }
}

impl From<CreateOptions> for CreateSandboxRequest {
    fn from(o: CreateOptions) -> Self {
        o.into_request("sandbox")
    }
}

/// Talks the SDK ⇆ Gateway `/sandboxes` REST API and hands back [`Sandbox`]es.
pub struct Client {
    http: Arc<HttpTransport>,
    call_timeout_ms: Option<u64>,
}

impl Client {
    pub fn new(opts: ClientOptions) -> Result<Self, SolariError> {
        let http = HttpTransport::new(HttpOptions::new(opts.api_key, opts.base_url))?;
        Ok(Client {
            http: Arc::new(http),
            call_timeout_ms: opts.call_timeout_ms,
        })
    }

    /// Create a new headless sandbox (`POST /sandboxes`).
    pub async fn create(&self, opts: CreateOptions) -> Result<Sandbox, SolariError> {
        self.create_with_kind(opts, "sandbox").await
    }

    /// Create a GUI desktop VM (`POST /sandboxes` with `kind:"desktop"`).
    ///
    /// Mirrors TS `createDesktop()` / Python `create_desktop()`. The returned
    /// handle carries [`Sandbox::stream_url`] for the live view, and exposes the
    /// same core namespaces as a sandbox (`commands`/`files`/`code`/`git`).
    /// Driving the GUI (mouse/keyboard/screenshot) is not part of this surface.
    ///
    /// Requires the `desktop` entitlement on the org's plan; the gateway
    /// answers 402 `FeatureRequiresPlan` otherwise.
    pub async fn create_desktop(&self, opts: CreateOptions) -> Result<Sandbox, SolariError> {
        self.create_with_kind(opts, "desktop").await
    }

    async fn create_with_kind(
        &self,
        opts: CreateOptions,
        kind: &str,
    ) -> Result<Sandbox, SolariError> {
        let req = opts.into_request(kind);
        let body = serde_json::to_value(&req).unwrap();
        let resp: CreateSandboxResponse = self
            .http
            .request("POST", "/sandboxes", Some(body), Some(new_idempotency_key()))
            .await?;
        Ok(Sandbox::from_response(self.http.clone(), resp, self.call_timeout_ms))
    }

    /// Re-attach to a running sandbox by id (`GET /sandboxes/:id`).
    pub async fn connect(&self, sandbox_id: &str) -> Result<Sandbox, SolariError> {
        let view = self.get(sandbox_id).await?;
        let control_url = view.control_url.clone().unwrap_or_else(|| {
            format!(
                "{}/control/{}",
                self.http.ws_origin(),
                encode_uri_component(sandbox_id)
            )
        });
        // `GET /sandboxes/:id` carries no streamUrl (the gateway's view
        // serializer omits it), so derive it exactly as control_url is derived
        // above — both are built from the same origin. Desktops only: a
        // headless sandbox has no display to stream.
        let stream_url = if view.kind == "desktop" {
            Some(format!(
                "{}/stream/{}",
                self.http.ws_origin(),
                encode_uri_component(sandbox_id)
            ))
        } else {
            None
        };
        let resp = CreateSandboxResponse {
            sandbox_id: view.sandbox_id,
            kind: view.kind,
            control_url,
            expires_at: view.expires_at,
            stream_url,
        };
        Ok(Sandbox::from_response(self.http.clone(), resp, self.call_timeout_ms))
    }

    /// Fetch a sandbox's current view (`GET /sandboxes/:id`).
    pub async fn get(&self, sandbox_id: &str) -> Result<SandboxView, SolariError> {
        self.http
            .request(
                "GET",
                &format!("/sandboxes/{}", encode_uri_component(sandbox_id)),
                None,
                None,
            )
            .await
    }

    /// Destroy a sandbox (`DELETE /sandboxes/:id`). Idempotent.
    pub async fn kill(&self, sandbox_id: &str) -> Result<(), SolariError> {
        let _: serde_json::Value = self
            .http
            .request(
                "DELETE",
                &format!("/sandboxes/{}", encode_uri_component(sandbox_id)),
                None,
                None,
            )
            .await?;
        Ok(())
    }

    /// Pause a session (`POST /sandboxes/:id/pause`): snapshot its RAM+disk and
    /// free its host slot. The session keeps its id; bring it back with
    /// [`Client::resume`].
    pub async fn pause(&self, sandbox_id: &str) -> Result<(), SolariError> {
        let _: serde_json::Value = self
            .http
            .request(
                "POST",
                &format!("/sandboxes/{}/pause", encode_uri_component(sandbox_id)),
                None,
                None,
            )
            .await?;
        Ok(())
    }

    /// Resume a paused session (`POST /sandboxes/:id/resume`) and return the
    /// control URL to re-attach to.
    ///
    /// The session comes back on a FRESH slot, so its pre-pause control URL is
    /// stale. The gateway normally returns the new one; when it does not, derive
    /// it from the gateway origin exactly as [`Client::connect`] does.
    pub async fn resume(&self, sandbox_id: &str) -> Result<String, SolariError> {
        let r: ResumeResponse = self
            .http
            .request(
                "POST",
                &format!("/sandboxes/{}/resume", encode_uri_component(sandbox_id)),
                None,
                None,
            )
            .await?;
        Ok(r.control_url.unwrap_or_else(|| {
            format!(
                "{}/control/{}",
                self.http.ws_origin(),
                encode_uri_component(sandbox_id)
            )
        }))
    }
}

/// A live sandbox handle. Exposes the core namespaces.
pub struct Sandbox {
    id: String,
    kind: String,
    control_url: String,
    stream_url: Option<String>,
    expires_at: String,
    http: Arc<HttpTransport>,
    channel: Arc<ControlChannel>,
}

impl Sandbox {
    pub(crate) fn from_response(
        http: Arc<HttpTransport>,
        resp: CreateSandboxResponse,
        call_timeout_ms: Option<u64>,
    ) -> Self {
        let channel = ControlChannel::new(
            resp.control_url.clone(),
            http.auth_headers(),
            call_timeout_ms,
        );
        Sandbox {
            id: resp.sandbox_id,
            kind: resp.kind,
            control_url: resp.control_url,
            stream_url: resp.stream_url,
            expires_at: resp.expires_at,
            http,
            channel: Arc::new(channel),
        }
    }

    /// The session id.
    pub fn id(&self) -> &str {
        &self.id
    }
    /// The VM flavour — `"sandbox"` or `"desktop"`.
    pub fn kind(&self) -> &str {
        &self.kind
    }
    /// The control-WS URL.
    pub fn control_url(&self) -> &str {
        &self.control_url
    }
    /// The live-view stream URL. `Some` for desktops only — a headless sandbox
    /// has no display, so the gateway omits it.
    pub fn stream_url(&self) -> Option<&str> {
        self.stream_url.as_deref()
    }
    /// ISO-8601 expiry timestamp.
    pub fn expires_at(&self) -> &str {
        &self.expires_at
    }
    /// Whether the control channel is open.
    pub fn connected(&self) -> bool {
        self.channel.connected()
    }

    /// Open the control WebSocket. Idempotent.
    pub async fn connect(&self) -> Result<(), SolariError> {
        self.channel.connect().await
    }

    /// Close the control channel locally (does NOT release the remote session).
    pub fn close(&self) {
        self.channel.close();
    }

    /// Pause this session: snapshot its RAM+disk, free its host slot, and close
    /// the control channel. The session keeps its id; bring it back with
    /// [`Sandbox::resume`].
    ///
    /// Mirrors TS `handle.pause()` / Python `handle.pause()`: remote call first,
    /// then the local channel close.
    pub async fn pause(&self) -> Result<(), SolariError> {
        let _: serde_json::Value = self
            .http
            .request(
                "POST",
                &format!("/sandboxes/{}/pause", encode_uri_component(&self.id)),
                None,
                None,
            )
            .await?;
        self.close();
        Ok(())
    }

    /// Resume this paused session and re-point the control channel at the fresh
    /// slot it came back on.
    ///
    /// Mirrors TS `handle.resume()` / Python `handle.resume()`: resume, adopt the
    /// new control URL, reconnect.
    ///
    /// Takes `&mut self` where the rest of the handle takes `&self`: resuming
    /// genuinely replaces this handle's `control_url`, and [`Sandbox::control_url`]
    /// hands out a borrow of it. Interior mutability would let that borrow go
    /// stale behind the caller's back — the compiler should be the one saying
    /// this call mutates the handle.
    pub async fn resume(&mut self) -> Result<(), SolariError> {
        let r: ResumeResponse = self
            .http
            .request(
                "POST",
                &format!("/sandboxes/{}/resume", encode_uri_component(&self.id)),
                None,
                None,
            )
            .await?;
        let control_url = r.control_url.unwrap_or_else(|| {
            format!(
                "{}/control/{}",
                self.http.ws_origin(),
                encode_uri_component(&self.id)
            )
        });
        self.control_url = control_url.clone();
        self.channel.set_control_url(control_url);
        self.channel.reconnect().await
    }

    /// Destroy the remote session and close the channel. Idempotent.
    pub async fn kill(&self) -> Result<(), SolariError> {
        let _: serde_json::Value = self
            .http
            .request(
                "DELETE",
                &format!("/sandboxes/{}", encode_uri_component(&self.id)),
                None,
                None,
            )
            .await?;
        self.close();
        Ok(())
    }

    // --- namespaces ----------------------------------------------------------

    pub fn commands(&self) -> Commands<'_> {
        Commands { sb: self }
    }
    pub fn files(&self) -> Files<'_> {
        Files { sb: self }
    }
    pub fn code(&self) -> Code<'_> {
        Code { sb: self }
    }
    pub fn git(&self) -> Git<'_> {
        Git::new(self)
    }

    // --- internal accessors (used by the namespace modules) ------------------

    pub(crate) fn channel(&self) -> &ControlChannel {
        &self.channel
    }
    pub(crate) fn channel_arc(&self) -> Arc<ControlChannel> {
        self.channel.clone()
    }
    pub(crate) fn http(&self) -> &HttpTransport {
        &self.http
    }
}

/// `Sandbox` runs `git` through `commands.run("git", …)` (no shell).
impl GitRunner for Sandbox {
    fn run_git<'a>(
        &'a self,
        args: Vec<String>,
        cwd: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<CommandResult, SolariError>> + Send + 'a>> {
        Box::pin(async move {
            let opts = RunOptions {
                args: Some(args),
                cwd,
                ..Default::default()
            };
            run_command(self, "git", &opts).await
        })
    }
}

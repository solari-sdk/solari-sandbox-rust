//! `files` namespace (`fs.*` control-WS RPCs): read / write / list / stat /
//! mkdir / remove / rename. Mirrors the `files` surface of `handle.ts`.

use base64::Engine;
use serde_json::{Map, Value};

use crate::client::Sandbox;
use crate::error::SolariError;
use crate::types::{FsEntry, FsStat};

/// How much one `fs.upload` / `fs.download` call moves. A bigger frame (a 48 MiB file is a 64 MiB frame once base64'd) closes
/// the control channel and fails every call in flight on it.
const FILE_CHUNK_SIZE: usize = 4 << 20;
/// The most `write` sends in one frame; `upload` has no such limit.
const MAX_SINGLE_FRAME_BYTES: usize = 32 << 20;

fn file_too_large(n: usize) -> SolariError {
    SolariError::Other(format!(
        "file too large for a single call ({n} bytes, max {MAX_SINGLE_FRAME_BYTES}): use files.upload()/files.download() or uploadUrl()"
    ))
}

/// A control-channel reply saying the guest does not know the method (an older image).
fn is_unknown_method(err: &SolariError) -> bool {
    match err {
        SolariError::Action { message, code, .. } => {
            let m = format!("{message} {}", code.as_deref().unwrap_or("")).to_lowercase();
            m.contains("unknown method") || m.contains("method not found") || m.contains("unknown_method")
        }
        _ => false,
    }
}

/// The ergonomic `files` accessor returned by `Sandbox::files()`.
pub struct Files<'a> {
    pub(crate) sb: &'a Sandbox,
}

impl<'a> Files<'a> {
    async fn call(&self, method: &str, params: Value) -> Result<Value, SolariError> {
        self.sb.channel().connect().await?;
        self.sb.channel().call(method, params).await
    }

    /// Read a file's raw bytes. It reads in 4 MiB chunks ([`download`](Self::download)), so a large file never travels as one
    /// frame; a guest image that predates `fs.download` is read in one frame (`fs.read`).
    pub async fn read(&self, path: &str) -> Result<Vec<u8>, SolariError> {
        match self.download(path).await {
            Err(e) if is_unknown_method(&e) => self.read_single_frame(path).await,
            other => other,
        }
    }

    async fn read_single_frame(&self, path: &str) -> Result<Vec<u8>, SolariError> {
        let r = self.call("fs.read", json_obj(&[("path", Value::from(path))])).await?;
        let b64 = r.get("base64").and_then(Value::as_str).unwrap_or("");
        base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|e| SolariError::Other(format!("fs.read bad base64: {e}")))
    }

    /// Read a whole file in 4 MiB chunks (`fs.download`) until the guest reports the end.
    pub async fn download(&self, path: &str) -> Result<Vec<u8>, SolariError> {
        let mut out = Vec::new();
        let mut offset: u64 = 0;
        loop {
            let r = self
                .call(
                    "fs.download",
                    json_obj(&[
                        ("path", Value::from(path)),
                        ("offset", Value::from(offset)),
                        ("length", Value::from(FILE_CHUNK_SIZE)),
                    ]),
                )
                .await?;
            let b64 = r.get("base64").and_then(Value::as_str).unwrap_or("");
            let chunk = base64::engine::general_purpose::STANDARD
                .decode(b64)
                .map_err(|e| SolariError::Other(format!("fs.download bad base64: {e}")))?;
            out.extend_from_slice(&chunk);
            if r.get("eof").and_then(Value::as_bool).unwrap_or(false) {
                return Ok(out);
            }
            let next = r.get("nextOffset").and_then(Value::as_u64).unwrap_or(offset + chunk.len() as u64);
            if next <= offset {
                return Err(SolariError::Other(format!("fs.download made no progress at offset {offset}")));
            }
            offset = next;
        }
    }

    /// Write data to a file in 4 MiB chunks (`fs.upload`): the first call truncates, later calls append at their offset.
    /// An empty file is one zero-length call. Unlike [`write`](Self::write) there is no size limit.
    pub async fn upload(&self, path: &str, data: impl AsRef<[u8]>) -> Result<(), SolariError> {
        let data = data.as_ref();
        let mut offset = 0usize;
        loop {
            let end = (offset + FILE_CHUNK_SIZE).min(data.len());
            let mut p = Map::new();
            p.insert("path".into(), Value::from(path));
            p.insert("base64".into(), Value::String(base64::engine::general_purpose::STANDARD.encode(&data[offset..end])));
            p.insert("offset".into(), Value::from(offset));
            p.insert("truncate".into(), Value::Bool(offset == 0));
            self.call("fs.upload", Value::Object(p)).await?;
            if end >= data.len() {
                return Ok(());
            }
            offset = end;
        }
    }

    /// Read a file as UTF-8 text.
    pub async fn read_text(&self, path: &str) -> Result<String, SolariError> {
        let bytes = self.read(path).await?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    /// Write bytes to a file (`fs.write`). `mode` is an optional unix mode. The data travels as one frame, so more than
    /// 32 MiB is refused before anything is sent (a frame that large would close the control channel and fail every call
    /// on it): use [`upload`](Self::upload).
    pub async fn write(
        &self,
        path: &str,
        data: impl AsRef<[u8]>,
        mode: Option<i64>,
    ) -> Result<(), SolariError> {
        if data.as_ref().len() > MAX_SINGLE_FRAME_BYTES {
            return Err(file_too_large(data.as_ref().len()));
        }
        let b64 = base64::engine::general_purpose::STANDARD.encode(data.as_ref());
        let mut p = Map::new();
        p.insert("path".into(), Value::from(path));
        p.insert("base64".into(), Value::String(b64));
        if let Some(m) = mode {
            p.insert("mode".into(), Value::from(m));
        }
        self.call("fs.write", Value::Object(p)).await.map(|_| ())
    }

    /// List a directory (`fs.list`).
    pub async fn list(&self, path: &str) -> Result<Vec<FsEntry>, SolariError> {
        let r = self.call("fs.list", json_obj(&[("path", Value::from(path))])).await?;
        let entries = r.get("entries").cloned().unwrap_or(Value::Array(vec![]));
        serde_json::from_value(entries)
            .map_err(|e| SolariError::Other(format!("fs.list bad entries: {e}")))
    }

    /// Stat a path (`fs.stat`).
    pub async fn stat(&self, path: &str) -> Result<FsStat, SolariError> {
        let r = self.call("fs.stat", json_obj(&[("path", Value::from(path))])).await?;
        serde_json::from_value(r).map_err(|e| SolariError::Other(format!("fs.stat bad: {e}")))
    }

    /// Create a directory and parents (`fs.mkdir`).
    pub async fn mkdir(&self, path: &str) -> Result<(), SolariError> {
        self.call("fs.mkdir", json_obj(&[("path", Value::from(path))]))
            .await
            .map(|_| ())
    }

    /// Remove a path (`fs.remove`); `recursive` for directories.
    pub async fn remove(&self, path: &str, recursive: bool) -> Result<(), SolariError> {
        self.call(
            "fs.remove",
            json_obj(&[("path", Value::from(path)), ("recursive", Value::from(recursive))]),
        )
        .await
        .map(|_| ())
    }

    /// Rename/move a path (`fs.rename`).
    pub async fn rename(&self, from: &str, to: &str) -> Result<(), SolariError> {
        self.call(
            "fs.rename",
            json_obj(&[("from", Value::from(from)), ("to", Value::from(to))]),
        )
        .await
        .map(|_| ())
    }
}

fn json_obj(pairs: &[(&str, Value)]) -> Value {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert((*k).to_string(), v.clone());
    }
    Value::Object(m)
}

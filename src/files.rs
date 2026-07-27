//! `files` namespace (`fs.*` control-WS RPCs): read / write / list / stat /
//! mkdir / remove / rename. Mirrors the `files` surface of `handle.ts`.

use base64::Engine;
use serde_json::{Map, Value};

use crate::client::Sandbox;
use crate::error::SolariError;
use crate::types::{FsEntry, FsStat};

/// The ergonomic `files` accessor returned by `Sandbox::files()`.
pub struct Files<'a> {
    pub(crate) sb: &'a Sandbox,
}

impl<'a> Files<'a> {
    async fn call(&self, method: &str, params: Value) -> Result<Value, SolariError> {
        self.sb.channel().connect().await?;
        self.sb.channel().call(method, params).await
    }

    /// Read a file's raw bytes (`fs.read`).
    pub async fn read(&self, path: &str) -> Result<Vec<u8>, SolariError> {
        let r = self.call("fs.read", json_obj(&[("path", Value::from(path))])).await?;
        let b64 = r.get("base64").and_then(Value::as_str).unwrap_or("");
        base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|e| SolariError::Other(format!("fs.read bad base64: {e}")))
    }

    /// Read a file as UTF-8 text.
    pub async fn read_text(&self, path: &str) -> Result<String, SolariError> {
        let bytes = self.read(path).await?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    /// Write bytes to a file (`fs.write`). `mode` is an optional unix mode.
    pub async fn write(
        &self,
        path: &str,
        data: impl AsRef<[u8]>,
        mode: Option<i64>,
    ) -> Result<(), SolariError> {
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

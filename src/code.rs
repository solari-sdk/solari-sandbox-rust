//! `code` namespace (`code.run`): run code in a stateful kernel and flatten the
//! per-result `chart` fields into a top-level `charts` array. Mirrors `runCode`
//! in `handle.ts`.

use serde::Deserialize;
use serde_json::{Map, Value};

use crate::client::Sandbox;
use crate::error::SolariError;
use crate::types::{CodeResultItem, RunCodeResult};

/// Options for `code.run`.
#[derive(Default, Clone)]
pub struct RunCodeOptions {
    /// Language (defaults server-side to python).
    pub language: Option<String>,
    /// Stateful kernel context id (REPL persistence across calls).
    pub context_id: Option<String>,
}

#[derive(Default, Deserialize)]
struct RawRunCode {
    #[serde(default)]
    results: Option<Vec<CodeResultItem>>,
    #[serde(default)]
    error: Option<Value>,
}

/// Build a [`RunCodeResult`] from a raw `code.run` reply, flattening every
/// `results[i].chart` into the top-level `charts` array.
pub(crate) fn build_run_code_result(v: Value) -> RunCodeResult {
    let raw: RawRunCode = serde_json::from_value(v).unwrap_or_default();
    let results = raw.results.unwrap_or_default();
    let charts = results.iter().filter_map(|i| i.chart.clone()).collect();
    RunCodeResult {
        results,
        error: raw.error,
        charts,
    }
}

/// The ergonomic `code` accessor returned by `Sandbox::code()`.
pub struct Code<'a> {
    pub(crate) sb: &'a Sandbox,
}

impl<'a> Code<'a> {
    /// Run `code` in a stateful kernel and return rich results + flattened charts.
    pub async fn run(&self, code: &str, opts: RunCodeOptions) -> Result<RunCodeResult, SolariError> {
        self.sb.channel().connect().await?;
        let mut p = Map::new();
        p.insert("code".into(), Value::String(code.to_string()));
        if let Some(lang) = &opts.language {
            p.insert("language".into(), Value::String(lang.clone()));
        }
        if let Some(ctx) = &opts.context_id {
            p.insert("contextId".into(), Value::String(ctx.clone()));
        }
        let raw = self.sb.channel().call("code.run", Value::Object(p)).await?;
        Ok(build_run_code_result(raw))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ChartType;

    #[test]
    fn flattens_result_charts_into_top_level() {
        let reply = serde_json::json!({
            "results": [
                { "type": "stdout", "text": "hi" },
                { "type": "result", "png": "AAAA",
                  "chart": { "type": "line", "title": "T",
                             "elements": [{"points": [[1,2]]}] } },
                { "type": "result", "chart": { "type": "bar" } },
                { "type": "result", "html": "<b/>" }
            ],
            "error": null
        });
        let out = build_run_code_result(reply);
        assert_eq!(out.results.len(), 4);
        assert_eq!(out.charts.len(), 2);
        assert_eq!(out.charts[0].chart_type, ChartType::Line);
        assert_eq!(out.charts[0].title.as_deref(), Some("T"));
        // `elements` stays as raw JSON.
        assert!(out.charts[0].elements.is_some());
        assert_eq!(out.charts[1].chart_type, ChartType::Bar);
    }

    #[test]
    fn unknown_chart_type_does_not_fail() {
        let reply = serde_json::json!({
            "results": [ { "type": "result", "chart": { "type": "sankey" } } ]
        });
        let out = build_run_code_result(reply);
        assert_eq!(out.charts.len(), 1);
        assert_eq!(out.charts[0].chart_type, ChartType::Unknown);
    }
}

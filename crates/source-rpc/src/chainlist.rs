//! Public endpoints chainlist.org lists for a chain: keyless HTTPS ones whose
//! operators state that they keep no logs. Anyone can list an endpoint
//! there, so these only carry blocks the node checks itself.

use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};
use url::Url;

/// chainlist.org's export of every chain it lists and their endpoints.
const EXPORT: &str = "https://chainlist.org/rpcs.json";
/// How long a fetched list is used before it is fetched again.
const REFRESH: Duration = Duration::from_hours(24);
/// The export is a few megabytes; a larger one is refused.
const MAX_EXPORT_BYTES: usize = 32 * 1_024 * 1_024;

#[derive(Deserialize, Serialize)]
struct Kept {
    chain_id: u64,
    fetched_at_unix_seconds: u64,
    endpoints: Vec<String>,
}

/// The endpoints `export` lists for `chain_id` that use HTTPS, need no key,
/// and keep no logs by their operator's statement.
#[must_use]
pub fn listed(export: &Value, chain_id: u64) -> Vec<Url> {
    let Some(chain) = export
        .as_array()
        .into_iter()
        .flatten()
        .find(|chain| chain["chainId"].as_u64() == Some(chain_id))
    else {
        return Vec::new();
    };
    let mut endpoints = Vec::new();
    for rpc in chain["rpc"].as_array().into_iter().flatten() {
        let endpoint = rpc["url"]
            .as_str()
            .filter(|_| rpc["tracking"].as_str() == Some("none"))
            // Keyed endpoints carry a `${API_KEY}` placeholder.
            .filter(|url| !url.contains(['$', '{']))
            .and_then(|url| Url::parse(url).ok())
            .filter(|url| url.scheme() == "https");
        if let Some(endpoint) = endpoint
            && !endpoints.contains(&endpoint)
        {
            endpoints.push(endpoint);
        }
    }
    endpoints
}

/// The endpoints chainlist.org lists for `chain_id`, kept at `kept` and
/// fetched again after a day. When they cannot be fetched, the kept ones
/// serve however old, or none.
pub async fn endpoints(chain_id: u64, kept: &Path) -> Vec<Url> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let previous = std::fs::read(kept)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Kept>(&bytes).ok())
        .filter(|previous| previous.chain_id == chain_id);
    let parse = |kept: &Kept| {
        kept.endpoints
            .iter()
            .filter_map(|endpoint| Url::parse(endpoint).ok())
            .collect::<Vec<_>>()
    };
    if let Some(previous) = previous
        .as_ref()
        .filter(|previous| now.saturating_sub(previous.fetched_at_unix_seconds) < REFRESH.as_secs())
    {
        return parse(previous);
    }
    match fetch().await {
        Ok(export) => {
            let endpoints = listed(&export, chain_id);
            info!(
                chain_id,
                endpoints = endpoints.len(),
                "chainlist.org lists public endpoints"
            );
            let fresh = Kept {
                chain_id,
                fetched_at_unix_seconds: now,
                endpoints: endpoints.iter().map(ToString::to_string).collect(),
            };
            if let Err(error) = serde_json::to_vec(&fresh)
                .map_err(std::io::Error::other)
                .and_then(|bytes| std::fs::write(kept, bytes))
            {
                warn!(%error, "could not keep chainlist.org's endpoints");
            }
            endpoints
        }
        Err(error) => {
            warn!(%error, "could not fetch chainlist.org's endpoints; using the kept ones");
            previous.as_ref().map(parse).unwrap_or_default()
        }
    }
}

async fn fetch() -> Result<Value, String> {
    let response = reqwest::Client::builder()
        .user_agent(concat!("leani/", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|error| error.to_string())?
        .get(EXPORT)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|error| error.to_string())?;
    crate::read_json(response, MAX_EXPORT_BYTES).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_keyless_https_endpoints_that_keep_no_logs_are_listed() {
        let export = serde_json::json!([
            {"chainId": 1, "rpc": [{"url": "https://other.example/", "tracking": "none"}]},
            {"chainId": 42161, "rpc": [
                {"url": "https://a.example/arb", "tracking": "none"},
                {"url": "https://a.example/arb", "tracking": "none"},
                {"url": "https://b.example/", "tracking": "limited"},
                {"url": "https://c.example/", "tracking": "yes"},
                {"url": "https://d.example/"},
                {"url": "https://e.example/v2/${INFURA_API_KEY}", "tracking": "none"},
                {"url": "http://f.example/", "tracking": "none"},
                {"url": "wss://g.example/", "tracking": "none"},
                {"url": "https://h.example/", "tracking": "none"},
            ]},
        ]);
        assert_eq!(
            listed(&export, 42161)
                .iter()
                .map(Url::as_str)
                .collect::<Vec<_>>(),
            ["https://a.example/arb", "https://h.example/"]
        );
        assert!(listed(&export, 10).is_empty());
    }
}

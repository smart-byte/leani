//! Untrusted HTTP transport for Beacon API light-client responses.

use std::{fmt, time::Duration};

use async_trait::async_trait;
use url::Url;

use crate::BeaconApiError;

/// Largest Beacon API response body read from an endpoint.
const MAX_RESPONSE_BYTES: usize = 16 * 1_024 * 1_024;
/// Largest error response body read for diagnostics.
const MAX_ERROR_BODY_BYTES: usize = 2 * 1_024;

/// Render `url` for errors and logs as `scheme://host[:port]`, with `/…` in
/// place of a base path. Providers put API keys in userinfo, query strings,
/// and paths alike, so none of them is shown.
#[must_use]
pub fn redacted_url(url: &Url) -> String {
    let mut rendered = format!("{}://", url.scheme());
    if let Some(host) = url.host_str() {
        rendered.push_str(host);
    }
    if let Some(port) = url.port() {
        rendered.push(':');
        rendered.push_str(&port.to_string());
    }
    if !matches!(url.path(), "" | "/") {
        rendered.push_str("/…");
    }
    rendered
}

/// The URL of a request to `path_and_query` below `endpoint`. Joining a path
/// drops the endpoint's query, where providers put API keys, so it is
/// appended after the request's own query.
///
/// # Errors
///
/// Fails when `path_and_query` does not join onto `endpoint`.
pub fn request_url(endpoint: &Url, path_and_query: &str) -> Result<Url, url::ParseError> {
    let mut url = endpoint.join(path_and_query)?;
    if let Some(key) = endpoint.query().filter(|query| !query.is_empty()) {
        let query = match url.query().filter(|own| !own.is_empty()) {
            Some(own) => format!("{own}&{key}"),
            None => key.to_owned(),
        };
        url.set_query(Some(&query));
    }
    Ok(url)
}

/// `text` with each secret `endpoint` may carry replaced by `…`: every
/// non-empty base-path segment, the raw query and each query value, the
/// username, and the password. A server's error body may echo the request's
/// path and query.
pub(crate) fn redact_endpoint(text: &str, endpoint: &Url) -> String {
    let mut secrets = endpoint
        .path_segments()
        .into_iter()
        .flatten()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if let Some(query) = endpoint.query() {
        secrets.push(query.to_owned());
        secrets.extend(
            query
                .split('&')
                .filter_map(|pair| pair.split_once('=').map(|(_, value)| value.to_owned())),
        );
        secrets.extend(endpoint.query_pairs().map(|(_, value)| value.into_owned()));
    }
    secrets.push(endpoint.username().to_owned());
    secrets.extend(endpoint.password().map(str::to_owned));
    // Longest first, so no shorter secret leaves pieces of a longer one.
    secrets.retain(|secret| !secret.is_empty());
    secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
    secrets.iter().fold(text.to_owned(), |text, secret| {
        text.replace(secret.as_str(), "…")
    })
}

/// Render a request to `path_and_query` below `endpoint` for errors: the
/// redacted endpoint plus the API path, which is never secret.
#[must_use]
pub fn request_label(endpoint: &Url, path_and_query: &str) -> String {
    format!(
        "{}/{}",
        redacted_url(endpoint),
        path_and_query.trim_start_matches('/')
    )
}

/// Labels for configured endpoints in reports and errors, without their
/// secrets. Endpoints that would render the same carry their index in
/// `field`, such as `finality.endpoints[1]`.
#[must_use]
pub fn endpoint_labels(endpoints: &[Url], field: &str) -> Vec<String> {
    let rendered = endpoints.iter().map(redacted_url).collect::<Vec<_>>();
    rendered
        .iter()
        .enumerate()
        .map(|(index, label)| {
            if rendered.iter().filter(|other| *other == label).count() > 1 {
                format!("{label} ({field}[{index}])")
            } else {
                label.clone()
            }
        })
        .collect()
}

/// The HTTP client for untrusted Beacon endpoints: bounded timeouts, and no
/// redirect is ever followed to another origin.
pub(crate) fn client_builder(timeout: Duration) -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(timeout.min(Duration::from_secs(10)))
        .redirect(reqwest::redirect::Policy::none())
}

/// Treat an endpoint as a base directory, so joining a request path keeps
/// the endpoint's last path segment.
#[must_use]
pub fn normalized_endpoint(endpoint: &Url) -> Url {
    let mut endpoint = endpoint.clone();
    if !endpoint.path().ends_with('/') {
        let path = format!("{}/", endpoint.path());
        endpoint.set_path(&path);
    }
    endpoint
}

/// Untrusted transport for Beacon API light-client responses.
#[async_trait]
pub(crate) trait BeaconTransport: Send + Sync + fmt::Debug {
    /// Fetch `path_and_query`, relative to a normalized endpoint.
    async fn get(&self, endpoint: &Url, path_and_query: &str) -> Result<Vec<u8>, BeaconApiError>;
}

#[derive(Debug)]
pub(crate) struct HttpTransport {
    pub(crate) client: reqwest::Client,
}

#[async_trait]
impl BeaconTransport for HttpTransport {
    async fn get(&self, endpoint: &Url, path_and_query: &str) -> Result<Vec<u8>, BeaconApiError> {
        let url = request_url(endpoint, path_and_query).map_err(BeaconApiError::Url)?;
        let label = request_label(endpoint, path_and_query);
        let response =
            self.client
                .get(url)
                .send()
                .await
                .map_err(|source| BeaconApiError::Request {
                    url: label.clone(),
                    source: source.without_url(),
                })?;
        read_response(response, &label, MAX_RESPONSE_BYTES)
            .await
            .map_err(|error| error.redacting(endpoint))
    }
}

/// Read a response body of at most `maximum` bytes while it streams in.
///
/// # Errors
///
/// Rejects a redirect, a non-success status (with a bounded detail), or a
/// body larger than `maximum`.
pub async fn read_response(
    mut response: reqwest::Response,
    label: &str,
    maximum: usize,
) -> Result<Vec<u8>, BeaconApiError> {
    let status = response.status();
    if status.is_redirection() {
        return Err(BeaconApiError::Redirect {
            url: label.to_owned(),
            status: status.as_u16(),
        });
    }
    let too_large = || BeaconApiError::ResponseTooLarge {
        url: label.to_owned(),
        maximum,
    };
    if status.is_success()
        && response
            .content_length()
            .is_some_and(|length| length > u64::try_from(maximum).unwrap_or(u64::MAX))
    {
        return Err(too_large());
    }
    let limit = if status.is_success() {
        maximum
    } else {
        maximum.min(MAX_ERROR_BODY_BYTES)
    };
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|source| BeaconApiError::Request {
            url: label.to_owned(),
            source: source.without_url(),
        })?
    {
        let room = limit.saturating_sub(body.len());
        if chunk.len() > room {
            if status.is_success() {
                return Err(too_large());
            }
            body.extend_from_slice(&chunk[..room]);
            break;
        }
        body.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        return Err(BeaconApiError::Status {
            url: label.to_owned(),
            status: status.as_u16(),
            detail: String::from_utf8_lossy(&body).chars().take(512).collect(),
        });
    }
    Ok(body)
}

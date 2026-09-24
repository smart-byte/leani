//! Checks that keep a web page from calling JSON-RPC through the visitor's
//! browser.

use std::collections::BTreeSet;

use axum::{
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::RpcState;

/// Refuse a browser `Origin` other than a loopback or configured one with
/// HTTP 403, on calls and WebSocket upgrades alike, and a call whose body
/// is not declared as JSON with HTTP 415. A hostile page can then neither
/// post a CORS-safelisted `text/plain` or form body nor open a WebSocket,
/// and a JSON body needs a preflight the listener never grants.
pub(crate) async fn guard_browser_requests(
    State(state): State<RpcState>,
    request: Request,
    next: Next,
) -> Response {
    if !origin_allowed(request.headers(), &state.config.allowed_origins) {
        return (
            StatusCode::FORBIDDEN,
            "this Origin may not call JSON-RPC; list it in rpc.allowed_origins",
        )
            .into_response();
    }
    if request.method() == Method::POST && !is_json_content_type(request.headers()) {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "JSON-RPC requests need content-type: application/json",
        )
            .into_response();
    }
    next.run(request).await
}

/// Whether every `Origin` header is a loopback origin on any port or one of
/// `allowed`. A request without one is not a cross-origin browser request.
/// Mirrors the native API's policy.
fn origin_allowed(headers: &HeaderMap, allowed: &BTreeSet<String>) -> bool {
    headers.get_all(header::ORIGIN).iter().all(|origin| {
        origin.to_str().is_ok_and(|origin| {
            let origin = origin.to_ascii_lowercase();
            origin
                .strip_prefix("http://")
                .or_else(|| origin.strip_prefix("https://"))
                .is_some_and(|authority| is_loopback_host(authority_host(authority)))
                || allowed.contains(&origin)
        })
    })
}

/// The host of a `host[:port]` authority; an IPv6 host keeps its brackets.
fn authority_host(authority: &str) -> &str {
    authority
        .rsplit_once(':')
        .filter(|(host, port)| {
            !port.is_empty()
                && port.bytes().all(|byte| byte.is_ascii_digit())
                && (!host.starts_with('[') || host.ends_with(']'))
        })
        .map_or(authority, |(host, _)| host)
}

fn is_loopback_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "[::1]")
}

/// Whether the `Content-Type` media type is `application/json`, with any
/// parameters.
fn is_json_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

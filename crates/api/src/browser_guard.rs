//! Checks that keep a web page from reading or changing a loopback API
//! through the visitor's browser.

use std::{collections::BTreeSet, sync::Arc};

use axum::{
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::ApiError;

/// Marks a mutating request without a JSON body as sent by a Leani client.
/// A cross-site page cannot add it without a CORS preflight.
const LEANI_REQUEST_HEADER: &str = "x-leani-request";

/// Host names and browser origins, besides loopback ones, the API answers.
#[derive(Debug)]
pub(crate) struct RequestPolicy {
    pub(crate) allowed_hosts: BTreeSet<String>,
    pub(crate) allowed_origins: BTreeSet<String>,
}

pub(crate) fn lowercase_set(values: &BTreeSet<String>) -> BTreeSet<String> {
    values
        .iter()
        .map(|value| value.to_ascii_lowercase())
        .collect()
}

/// Configured origins as browsers serialize them: lowercase, without a
/// trailing slash.
pub(crate) fn normalized_origins(values: &BTreeSet<String>) -> BTreeSet<String> {
    values
        .iter()
        .map(|value| value.trim_end_matches('/').to_ascii_lowercase())
        .collect()
}

/// Checks run before routing and authentication, so a web page can neither
/// read nor change a loopback API through the visitor's browser. A `Host`
/// name other than `localhost` or an allowed one gets 421: a page that
/// rebinds its own name to 127.0.0.1 still sends that name. An IP address
/// passes, since no rebinding produces one. An `Origin` that is neither
/// loopback nor allowed gets 403, streams included. A GET or HEAD that
/// another site's page sent other than as a navigation, such as an image
/// load, carries no `Origin` and gets 403 by its Fetch Metadata. A mutating
/// request needs a JSON content type or `x-leani-request: 1`, which a
/// cross-site page can only send after a CORS preflight the API never
/// grants, so it gets 415 otherwise. Navigations, and GETs from browsers
/// without Fetch Metadata, still pass; only the bearer token keeps them off
/// GET routes with side effects, such as opening a consumer's stream.
pub(crate) async fn guard_browser_requests(
    State(policy): State<Arc<RequestPolicy>>,
    request: Request,
    next: Next,
) -> Response {
    let rejection = if !host_allowed(&request, &policy.allowed_hosts) {
        Some((
            StatusCode::MISDIRECTED_REQUEST,
            "host_not_allowed",
            "this API does not serve the requested Host; list it in api.allowed_hosts",
        ))
    } else if !origin_allowed(request.headers(), &policy.allowed_origins) {
        Some((
            StatusCode::FORBIDDEN,
            "origin_not_allowed",
            "requests from this Origin are refused; list it in api.allowed_origins",
        ))
    } else if is_cross_site_subresource(&request) {
        Some((
            StatusCode::FORBIDDEN,
            "cross_site_request",
            "browser requests from another site are refused unless they navigate",
        ))
    } else if !matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    ) && !is_json_content_type(request.headers())
        && request
            .headers()
            .get(LEANI_REQUEST_HEADER)
            .is_none_or(|value| value != "1")
    {
        Some((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "a mutating request needs content-type: application/json or x-leani-request: 1",
        ))
    } else {
        None
    };
    match rejection {
        Some((status, code, message)) => {
            ApiError::new(status, code, message, false).into_response()
        }
        None => next.run(request).await,
    }
}

/// Whether a browser sent this GET or HEAD for a page of another site, or
/// another origin of the same site, other than as a navigation: an image,
/// script, or no-cors fetch, which carries no `Origin`. Clients that send no
/// Fetch Metadata, such as the SDK and curl, pass, as do navigations and
/// same-origin requests.
fn is_cross_site_subresource(request: &Request) -> bool {
    let header_is = |name: &str, values: &[&str]| {
        request.headers().get(name).is_some_and(|value| {
            values
                .iter()
                .any(|expected| value.as_bytes().eq_ignore_ascii_case(expected.as_bytes()))
        })
    };
    matches!(*request.method(), Method::GET | Method::HEAD)
        && !request.headers().contains_key(header::ORIGIN)
        && header_is("sec-fetch-site", &["cross-site", "same-site"])
        && !header_is("sec-fetch-mode", &["navigate"])
}

/// Whether every host the request names, in `Host` headers or an absolute
/// target, is an IP address, `localhost`, or allowed. A request that names
/// none, which no browser sends, passes.
fn host_allowed(request: &Request, allowed: &BTreeSet<String>) -> bool {
    let target = request
        .uri()
        .authority()
        .map(axum::http::uri::Authority::host);
    request
        .headers()
        .get_all(header::HOST)
        .iter()
        .map(|host| host.to_str().ok().map(authority_host))
        .chain(target.map(Some))
        .all(|host| {
            host.is_some_and(|host| {
                let host = host.to_ascii_lowercase();
                host == "localhost" || is_ip_address(&host) || allowed.contains(&host)
            })
        })
}

/// A dotted IPv4 address or a bracketed IPv6 one.
fn is_ip_address(host: &str) -> bool {
    host.parse::<std::net::Ipv4Addr>().is_ok()
        || host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .is_some_and(|host| host.parse::<std::net::Ipv6Addr>().is_ok())
}

/// Whether every `Origin` header is a loopback origin on any port or one of
/// `allowed`. A request without one is not a cross-origin browser request.
/// Mirrors the JSON-RPC listeners' policy.
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

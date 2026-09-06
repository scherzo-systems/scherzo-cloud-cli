use std::io;

use reqwest::blocking::Response as BlockingResponse;
use reqwest::header::{CONTENT_TYPE, HeaderValue, LOCATION};
use reqwest::{Response, StatusCode, Url};
use url::Position;

pub(crate) const MAX_RESPONSE_BODY_BYTES: usize = 1024 * 1024;
const MAX_RESPONSE_BODY_BYTES_U64: u64 = 1024 * 1024;

pub(crate) enum BoundedBodyError {
    TooLarge,
    Transport(reqwest::Error),
}

pub(crate) struct BufferedResponse {
    pub(crate) status: StatusCode,
    pub(crate) content_type: Option<String>,
    pub(crate) idempotency_key: Option<HeaderValue>,
    pub(crate) location: Option<HeaderValue>,
    pub(crate) retry_after: Option<HeaderValue>,
    pub(crate) body: Vec<u8>,
}

pub(crate) enum BufferedResponseError {
    TooLarge {
        status: StatusCode,
    },
    Transport {
        status: StatusCode,
        source: reqwest::Error,
    },
    InvalidContentType {
        status: StatusCode,
    },
}

pub(crate) enum ApiAttemptError<E> {
    Protocol(E),
    Transport(super::UnreachableCategory),
}

pub(crate) async fn buffer_api_response<E>(
    response: Response,
    protocol: impl Fn(&'static str, bool) -> E,
) -> Result<BufferedResponse, ApiAttemptError<E>> {
    buffer_response(response)
        .await
        .map_err(|error| match error {
            BufferedResponseError::TooLarge { status } => ApiAttemptError::Protocol(protocol(
                "the response body exceeds 1 MiB",
                status == StatusCode::UNAUTHORIZED,
            )),
            BufferedResponseError::Transport { status, .. }
                if status == StatusCode::UNAUTHORIZED =>
            {
                ApiAttemptError::Protocol(protocol(
                    "the unauthorized response body could not be read",
                    true,
                ))
            }
            BufferedResponseError::Transport { source, .. } => {
                ApiAttemptError::Transport(super::classify_reqwest_error(&source))
            }
            BufferedResponseError::InvalidContentType { status } => {
                ApiAttemptError::Protocol(protocol(
                    "the Content-Type header is not valid text",
                    status == StatusCode::UNAUTHORIZED,
                ))
            }
        })
}

pub(crate) async fn buffer_response(
    response: Response,
) -> Result<BufferedResponse, BufferedResponseError> {
    let status = response.status();
    let content_type = response.headers().get(CONTENT_TYPE).cloned();
    let idempotency_key = response.headers().get("Idempotency-Key").cloned();
    let location = response.headers().get(LOCATION).cloned();
    let retry_after = response.headers().get("Retry-After").cloned();
    let body = read_bounded_body(response)
        .await
        .map_err(|error| match error {
            BoundedBodyError::TooLarge => BufferedResponseError::TooLarge { status },
            BoundedBodyError::Transport(source) => {
                BufferedResponseError::Transport { status, source }
            }
        })?;
    let content_type = content_type
        .as_ref()
        .map(media_type)
        .transpose()
        .map_err(|()| BufferedResponseError::InvalidContentType { status })?;
    Ok(BufferedResponse {
        status,
        content_type,
        idempotency_key,
        location,
        retry_after,
        body,
    })
}

pub(crate) async fn read_bounded_body(mut response: Response) -> Result<Vec<u8>, BoundedBodyError> {
    let mut body =
        bounded_body_buffer(response.content_length()).ok_or(BoundedBodyError::TooLarge)?;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(BoundedBodyError::Transport)?
    {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BODY_BYTES {
            return Err(BoundedBodyError::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

pub(crate) fn read_bounded_blocking_body(
    mut response: BlockingResponse,
) -> Result<Vec<u8>, BoundedBodyError> {
    let body = bounded_body_buffer(response.content_length()).ok_or(BoundedBodyError::TooLarge)?;
    let mut writer = BoundedBodyWriter {
        body,
        limit_exceeded: false,
    };
    if let Err(error) = response.copy_to(&mut writer) {
        return Err(if writer.limit_exceeded {
            BoundedBodyError::TooLarge
        } else {
            BoundedBodyError::Transport(error)
        });
    }
    Ok(writer.body)
}

struct BoundedBodyWriter {
    body: Vec<u8>,
    limit_exceeded: bool,
}

impl io::Write for BoundedBodyWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.len() > MAX_RESPONSE_BODY_BYTES.saturating_sub(self.body.len()) {
            self.limit_exceeded = true;
            return Err(io::Error::other("response body exceeds limit"));
        }
        self.body.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn bounded_body_buffer(content_length: Option<u64>) -> Option<Vec<u8>> {
    if content_length.is_some_and(|length| length > MAX_RESPONSE_BODY_BYTES_U64) {
        return None;
    }
    let initial_capacity = content_length
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or_default()
        .min(MAX_RESPONSE_BODY_BYTES);
    Some(Vec::with_capacity(initial_capacity))
}

pub(crate) fn endpoint(base_url: &str, path: &[&str]) -> Result<Url, ()> {
    let mut endpoint = Url::parse(base_url).map_err(|_| ())?;
    let base_segment_count = {
        let mut base_segments = endpoint.path_segments().ok_or(())?.collect::<Vec<_>>();
        if base_segments.last() == Some(&"") {
            base_segments.pop();
        }
        base_segments.len()
    };

    let mut encoded_dot_segments = Vec::new();
    let mut segments = endpoint.path_segments_mut()?;
    segments.pop_if_empty();
    for (index, segment) in path.iter().enumerate() {
        match *segment {
            "." => {
                segments.push("dot");
                encoded_dot_segments.push((base_segment_count + index, "%2E"));
            }
            ".." => {
                segments.push("dotdot");
                encoded_dot_segments.push((base_segment_count + index, "%2E%2E"));
            }
            segment => {
                segments.push(segment);
            }
        }
    }
    drop(segments);

    if encoded_dot_segments.is_empty() {
        return Ok(endpoint);
    }

    preserve_encoded_dot_segments(endpoint, &encoded_dot_segments)
}

// `url` follows the WHATWG parser and normalizes even percent-encoded dot-only
// segments. Replace equal-length placeholders in its serialized representation
// so every structural offset remains valid while reqwest receives the RFC 3986
// request target required by the API contract.
fn preserve_encoded_dot_segments(
    endpoint: Url,
    encoded_dot_segments: &[(usize, &str)],
) -> Result<Url, ()> {
    let path_start = endpoint[..Position::BeforePath].len();
    let path_end = endpoint[..Position::AfterPath].len();
    let mut path = endpoint.path().split('/').collect::<Vec<_>>();
    for (index, encoded) in encoded_dot_segments {
        let segment = path.get_mut(index + 1).ok_or(())?;
        if segment.len() != encoded.len() {
            return Err(());
        }
        *segment = encoded;
    }
    let path = path.join("/");

    let mut internal = endpoint
        .serialize_internal(serde_json::value::Serializer)
        .map_err(|_| ())?;
    let serialization = internal
        .as_array_mut()
        .and_then(|fields| fields.first_mut())
        .ok_or(())?;
    let mut rewritten = serialization.as_str().ok_or(())?.to_owned();
    rewritten.replace_range(path_start..path_end, &path);
    serialization.clone_from(&serde_json::Value::String(rewritten));

    Url::deserialize_internal(internal).map_err(|_| ())
}

pub(crate) fn append_pagination(endpoint: &mut Url, limit: Option<u16>, cursor: Option<&str>) {
    if limit.is_some() || cursor.is_some() {
        let mut query = endpoint.query_pairs_mut();
        if let Some(limit) = limit {
            query.append_pair("limit", &limit.to_string());
        }
        if let Some(cursor) = cursor {
            query.append_pair("cursor", cursor);
        }
    }
}

pub(crate) fn require_nonempty(value: &str, reason: &'static str) -> Result<(), &'static str> {
    if value.is_empty() {
        Err(reason)
    } else {
        Ok(())
    }
}

pub(crate) fn header_matches(header: Option<&HeaderValue>, expected: &str) -> bool {
    header.and_then(|value| value.to_str().ok()) == Some(expected)
}

pub(crate) fn require_media_type(actual: Option<&str>, expected: &str) -> Result<(), &'static str> {
    if actual == Some(expected) {
        Ok(())
    } else {
        Err("the response Content-Type is not valid for its HTTP status")
    }
}

pub(crate) fn media_type(value: &HeaderValue) -> Result<String, ()> {
    value
        .to_str()
        .map(|value| {
            value
                .split(';')
                .next()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
        })
        .map_err(|_| ())
}

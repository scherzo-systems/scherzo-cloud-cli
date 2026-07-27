use reqwest::header::HeaderValue;
use reqwest::{Response, Url};
use url::Position;

pub(crate) const MAX_RESPONSE_BODY_BYTES: usize = 1024 * 1024;
const MAX_RESPONSE_BODY_BYTES_U64: u64 = 1024 * 1024;

pub(crate) enum BoundedBodyError {
    TooLarge,
    Transport(reqwest::Error),
}

pub(crate) async fn read_bounded_body(mut response: Response) -> Result<Vec<u8>, BoundedBodyError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BODY_BYTES_U64)
    {
        return Err(BoundedBodyError::TooLarge);
    }

    let initial_capacity = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or_default()
        .min(MAX_RESPONSE_BODY_BYTES);
    let mut body = Vec::with_capacity(initial_capacity);
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

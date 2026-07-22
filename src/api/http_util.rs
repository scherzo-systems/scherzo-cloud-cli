use reqwest::header::HeaderValue;
use reqwest::{Response, Url};

pub(crate) const MAX_RESPONSE_BODY_BYTES: usize = 1024 * 1024;

pub(crate) enum BoundedBodyError {
    TooLarge,
    Transport(reqwest::Error),
}

pub(crate) async fn read_bounded_body(mut response: Response) -> Result<Vec<u8>, BoundedBodyError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BODY_BYTES as u64)
    {
        return Err(BoundedBodyError::TooLarge);
    }

    let initial_capacity = response
        .content_length()
        .unwrap_or_default()
        .min(MAX_RESPONSE_BODY_BYTES as u64) as usize;
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
    let mut segments = endpoint.path_segments_mut()?;
    segments.pop_if_empty();
    for segment in path {
        segments.push(segment);
    }
    drop(segments);
    Ok(endpoint)
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

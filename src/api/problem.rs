use reqwest::StatusCode;

use super::generated::models;

pub(super) const JSON_MEDIA_TYPE: &str = "application/json";
pub(super) const PROBLEM_MEDIA_TYPE: &str = "application/problem+json";
pub(super) const ACCEPTED_MEDIA_TYPES: &str = "application/json, application/problem+json";
pub(super) const BAD_REQUEST: &str = "https://api.scherzo.dev/problems/bad-request";
pub(super) const UNAUTHORIZED: &str = "https://api.scherzo.dev/problems/unauthorized";
pub(super) const FORBIDDEN: &str = "https://api.scherzo.dev/problems/forbidden";
pub(super) const NOT_FOUND: &str = "https://api.scherzo.dev/problems/not-found";

pub(super) fn decode_type(
    response: &super::http_util::BufferedResponse,
) -> Result<String, &'static str> {
    super::http_util::require_media_type(response.content_type.as_deref(), PROBLEM_MEDIA_TYPE)?;
    decode(&response.body, response.status).map(|problem| problem.r#type)
}

pub(super) fn require_type(
    response: &super::http_util::BufferedResponse,
    expected_type: &str,
) -> Result<(), &'static str> {
    if decode_type(response)? == expected_type {
        Ok(())
    } else {
        Err("the problem type is not valid for its HTTP status")
    }
}

pub(super) fn decode(
    body: &[u8],
    expected_status: StatusCode,
) -> Result<models::Problem, &'static str> {
    let problem: models::Problem =
        serde_json::from_slice(body).map_err(|_| "the problem response body is invalid")?;
    if problem.status != i32::from(expected_status.as_u16()) {
        return Err("the problem status does not match the HTTP status");
    }
    Ok(problem)
}

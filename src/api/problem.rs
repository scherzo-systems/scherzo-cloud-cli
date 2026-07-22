use reqwest::StatusCode;

use super::generated::models;

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

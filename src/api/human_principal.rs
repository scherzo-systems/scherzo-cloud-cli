use super::generated::models;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct HumanPrincipal {
    pub(crate) id: String,
    pub(crate) display_name: Option<String>,
}

pub(super) fn decode(body: &[u8]) -> Result<HumanPrincipal, &'static str> {
    let value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| "the principal response body is not valid JSON")?;
    if !value.is_object() {
        return Err("the principal response body is not a JSON object");
    }

    let principal: models::CurrentHumanPrincipal =
        serde_json::from_value(value).map_err(|_| "the principal fields are invalid")?;
    if principal.id.is_empty() {
        return Err("the principal id is empty");
    }
    if principal
        .display_name
        .as_ref()
        .is_some_and(String::is_empty)
    {
        return Err("the principal display name is empty");
    }

    Ok(HumanPrincipal {
        id: principal.id,
        display_name: principal.display_name,
    })
}

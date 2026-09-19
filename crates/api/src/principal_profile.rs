use super::generated::models;

#[derive(Debug, Eq, PartialEq)]
pub struct PrincipalProfile {
    pub id: String,
    pub r#type: &'static str,
    pub display_name: Option<String>,
}

pub(super) fn from_api(principal: models::Principal) -> Result<PrincipalProfile, &'static str> {
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

    Ok(PrincipalProfile {
        id: principal.id,
        r#type: match principal.r#type {
            models::principal::Type::PrincipalTypeHuman => "human",
            models::principal::Type::PrincipalTypeService => "service",
        },
        display_name: principal.display_name,
    })
}

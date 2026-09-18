use serde::Serialize;

use crate::api::HumanPrincipal;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PrincipalResult<'a> {
    id: &'a str,
    r#type: &'static str,
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<&'a str>,
}

impl<'a> PrincipalResult<'a> {
    pub(super) fn from_principal(principal: &'a HumanPrincipal) -> Self {
        Self::new(&principal.id, "human", principal.display_name.as_deref())
    }

    pub(super) fn from_profile(principal: &'a crate::api::PrincipalProfile) -> Self {
        Self::new(
            &principal.id,
            principal.r#type,
            principal.display_name.as_deref(),
        )
    }

    fn new(id: &'a str, principal_type: &'static str, display_name: Option<&'a str>) -> Self {
        Self {
            id,
            r#type: principal_type,
            state: "active",
            display_name,
        }
    }
}

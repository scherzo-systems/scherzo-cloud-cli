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
        Self {
            id: &principal.id,
            r#type: "human",
            state: "active",
            display_name: principal.display_name.as_deref(),
        }
    }
}

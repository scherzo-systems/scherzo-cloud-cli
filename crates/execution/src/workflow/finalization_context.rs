use std::sync::Arc;

use serde::Serialize;

use super::admission::CancellationReason;
use super::document::{FailurePolicy, FinalizationTrigger};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OrdinaryIssueDisposition {
    Failed,
    Blocked,
}

impl OrdinaryIssueDisposition {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Failed => "failed",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrdinaryIssue {
    pub(crate) step_id: String,
    pub(crate) failure_policy: FailurePolicy,
    pub(crate) disposition: OrdinaryIssueDisposition,
}

pub(crate) struct FinalizationContext<'a> {
    pub(crate) trigger: FinalizationTrigger,
    pub(crate) primary_issue_step_id: Option<&'a str>,
    pub(crate) cancellation_reason: Option<CancellationReason>,
    pub(crate) ordinary_issues: &'a [OrdinaryIssue],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SerializedContext<'a> {
    schema_version: u8,
    trigger: &'static str,
    primary_issue_step_id: Option<&'a str>,
    cancellation_reason: Option<&'static str>,
    ordinary_issues: Vec<SerializedIssue<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SerializedIssue<'a> {
    step_id: &'a str,
    failure_policy: &'static str,
    disposition: &'static str,
}

#[expect(
    clippy::expect_used,
    reason = "the closed serializer contains no fallible map keys or custom serializers"
)]
pub(crate) fn serialize(context: FinalizationContext<'_>) -> Arc<[u8]> {
    let mut ordinary_issues = context
        .ordinary_issues
        .iter()
        .map(|issue| SerializedIssue {
            step_id: &issue.step_id,
            failure_policy: match issue.failure_policy {
                FailurePolicy::Required => "required",
                FailurePolicy::Advisory => "advisory",
            },
            disposition: issue.disposition.as_str(),
        })
        .collect::<Vec<_>>();
    ordinary_issues.sort_by(|left, right| left.step_id.as_bytes().cmp(right.step_id.as_bytes()));
    let value = SerializedContext {
        schema_version: 1,
        trigger: context.trigger.as_str(),
        primary_issue_step_id: context.primary_issue_step_id,
        cancellation_reason: context.cancellation_reason.map(CancellationReason::as_str),
        ordinary_issues,
    };
    Arc::from(
        serde_json::to_vec(&value)
            .expect("the closed finalization context contains only infallible JSON values"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedicated_serializer_preserves_the_context_abi_order() {
        let issues = [
            OrdinaryIssue {
                step_id: "zeta".to_owned(),
                failure_policy: FailurePolicy::Required,
                disposition: OrdinaryIssueDisposition::Blocked,
            },
            OrdinaryIssue {
                step_id: "lint".to_owned(),
                failure_policy: FailurePolicy::Advisory,
                disposition: OrdinaryIssueDisposition::Failed,
            },
        ];

        assert_eq!(
            serialize(FinalizationContext {
                trigger: FinalizationTrigger::Succeeded,
                primary_issue_step_id: None,
                cancellation_reason: None,
                ordinary_issues: &issues,
            })
            .as_ref(),
            br#"{"schemaVersion":1,"trigger":"succeeded","primaryIssueStepId":null,"cancellationReason":null,"ordinaryIssues":[{"stepId":"lint","failurePolicy":"advisory","disposition":"failed"},{"stepId":"zeta","failurePolicy":"required","disposition":"blocked"}]}"#
        );
    }
}

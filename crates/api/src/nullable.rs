// OpenAPI Generator emits Option<Option<T>> for optional nullable values.
// Preserve omitted versus explicit null without a dependency in generated code.
pub(crate) mod double_option {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(crate) fn deserialize<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        Option::<T>::deserialize(deserializer).map(Some)
    }

    pub(crate) fn serialize<S, T>(
        value: &Option<Option<T>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        match value {
            Some(inner) => inner.serialize(serializer),
            None => serializer.serialize_none(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::generated::models::LinearEvaluationEvent;

    #[test]
    #[allow(
        clippy::disallowed_macros,
        clippy::unwrap_used,
        reason = "test assertions use fixed JSON fixtures"
    )]
    fn linear_event_distinguishes_omitted_null_and_present_actor() {
        let omitted: LinearEvaluationEvent = serde_json::from_str(r#"{"valid":true}"#).unwrap();
        let null: LinearEvaluationEvent =
            serde_json::from_str(r#"{"valid":true,"actorId":null}"#).unwrap();
        let present: LinearEvaluationEvent =
            serde_json::from_str(r#"{"valid":true,"actorId":"member"}"#).unwrap();

        assert_eq!(omitted.actor_id, None);
        assert_eq!(null.actor_id, Some(None));
        assert_eq!(present.actor_id, Some(Some("member".to_owned())));
        assert!(
            serde_json::to_value(omitted)
                .unwrap()
                .get("actorId")
                .is_none()
        );
        assert!(
            serde_json::to_value(null)
                .unwrap()
                .get("actorId")
                .unwrap()
                .is_null()
        );
        assert_eq!(serde_json::to_value(present).unwrap()["actorId"], "member");
    }
}

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::Message;

use super::Sleeper;
use super::assignment::AssignmentManager;
use super::connection::{
    ActiveEffectEvent, ConnectionCause, ConnectionDependencies, ConnectionError, FrameSource,
    OpeningHello, opening_hello, run_established,
};
use super::test_support::{
    DeterminismTranscript, healthy_wall_clock, scripted_duplex, with_watchdog,
};
use crate::runner::credential::test_credential;
use crate::runner::service::Config;
use crate::runner::telemetry::test_recorder;

const REPLAY_BOOT_ID: &str = "rbt_00000000000000000000000001";
const REPLAY_TIMESTAMP: &str = "2026-07-23T00:00:00Z";
const REPLAY_OVERRIDE: &str = "SCHERZO_RUNNER_CONVERSATION_FIXTURE";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Conversation {
    conversation_version: u64,
    name: String,
    recorded_by: String,
    entries: Vec<ConversationEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ConversationEntry {
    advance_ms: u64,
    from: ConversationPeer,
    kind: ConversationKind,
    payload: Option<Value>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum ConversationPeer {
    Gateway,
    Runner,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum ConversationKind {
    Text,
    Ping,
    Pong,
    Close,
}

struct ReplayFrameSource {
    message_ids: Mutex<VecDeque<String>>,
    message_id_references: HashMap<String, String>,
    timestamps: Mutex<VecDeque<String>>,
}

impl ReplayFrameSource {
    fn new(conversation: &Conversation) -> Self {
        let mut message_ids = VecDeque::new();
        let mut message_id_references = HashMap::new();
        let mut next_id = 1_u128;
        for entry in conversation.entries.iter().filter(|entry| {
            entry.from == ConversationPeer::Runner && entry.kind == ConversationKind::Text
        }) {
            let normalized = entry
                .payload
                .as_ref()
                .and_then(|payload| payload.get("messageId"))
                .and_then(Value::as_str)
                .expect("runner frame message ID placeholder")
                .to_owned();
            let concrete = format!(
                "rmsg_{}",
                ulid::Ulid::from(next_id).to_string().to_ascii_lowercase()
            );
            assert!(
                message_id_references
                    .insert(normalized.clone(), concrete.clone())
                    .is_none(),
                "conversation {} reuses runner message ID placeholder {normalized}",
                conversation.name
            );
            message_ids.push_back(concrete);
            next_id = next_id
                .checked_add(1)
                .expect("conversation message ID counter overflowed");
        }
        Self {
            message_ids: Mutex::new(message_ids),
            message_id_references,
            timestamps: Mutex::new(runner_timestamps(conversation)),
        }
    }

    fn concrete_message_id(&self, normalized: &str) -> String {
        self.message_id_references
            .get(normalized)
            .unwrap_or_else(|| {
                panic!("gateway acknowledgement references unknown runner frame {normalized}")
            })
            .clone()
    }
}

impl FrameSource for ReplayFrameSource {
    fn public_id(&self, prefix: &str) -> String {
        assert_eq!(
            prefix, "rmsg_",
            "conversation replay requested an unexpected ID"
        );
        self.message_ids
            .lock()
            .expect("conversation message ID mutex poisoned")
            .pop_front()
            .expect("conversation omitted a runner frame message ID")
    }

    fn utc_timestamp(&self) -> Result<String, ConnectionError> {
        Ok(self
            .timestamps
            .lock()
            .expect("conversation timestamp mutex poisoned")
            .pop_front()
            .expect("conversation omitted a timestamp for a runner frame"))
    }
}

#[derive(Clone)]
struct LogicalSleeper {
    origin: Instant,
    state: Arc<Mutex<LogicalSleepState>>,
    changed: Arc<Notify>,
}

struct LogicalSleepState {
    now: Duration,
    next_registration: u64,
    request_count: u64,
    pending: Vec<LogicalSleep>,
}

struct LogicalSleep {
    due: Duration,
    registration: u64,
    notification: Arc<Notify>,
}

impl LogicalSleeper {
    #[expect(
        clippy::disallowed_methods,
        reason = "logical sleepers need an opaque monotonic origin"
    )]
    fn new() -> Self {
        Self {
            origin: Instant::now(),
            state: Arc::new(Mutex::new(LogicalSleepState {
                now: Duration::ZERO,
                next_registration: 0,
                request_count: 0,
                pending: Vec::new(),
            })),
            changed: Arc::new(Notify::new()),
        }
    }

    fn advance(&self, duration: Duration) {
        let mut state = self
            .state
            .lock()
            .expect("conversation logical sleeper mutex poisoned");
        state.now = state
            .now
            .checked_add(duration)
            .expect("conversation logical time overflowed");
        let now = state.now;
        let mut due = Vec::new();
        state.pending.retain(|sleep| {
            if sleep.due <= now {
                due.push((sleep.registration, Arc::clone(&sleep.notification)));
                false
            } else {
                true
            }
        });
        due.sort_by_key(|(registration, _)| *registration);
        drop(state);
        for (_, notification) in due {
            notification.notify_one();
        }
    }

    async fn wait_for_requests(&self, expected: u64) {
        loop {
            let changed = self.changed.notified();
            if self
                .state
                .lock()
                .expect("conversation logical sleeper mutex poisoned")
                .request_count
                >= expected
            {
                return;
            }
            changed.await;
        }
    }
}

impl Sleeper for LogicalSleeper {
    fn now(&self) -> Instant {
        let elapsed = self
            .state
            .lock()
            .expect("conversation logical sleeper mutex poisoned")
            .now;
        self.origin + elapsed
    }

    fn sleep(&self, duration: Duration) -> super::SleepFuture<'_> {
        let notification = Arc::new(Notify::new());
        let mut state = self
            .state
            .lock()
            .expect("conversation logical sleeper mutex poisoned");
        state.next_registration += 1;
        state.request_count += 1;
        let due = state
            .now
            .checked_add(duration)
            .expect("conversation sleep deadline overflowed");
        let registration = state.next_registration;
        state.pending.push(LogicalSleep {
            due,
            registration,
            notification: Arc::clone(&notification),
        });
        drop(state);
        self.changed.notify_waiters();
        Box::pin(async move { notification.notified().await })
    }
}

#[tokio::test]
async fn replays_gateway_conversations_against_established_runner() {
    let conversations = load_conversations();
    if std::env::var_os(REPLAY_OVERRIDE).is_none() {
        let names: Vec<_> = conversations
            .iter()
            .map(|conversation| conversation.name.as_str())
            .collect();
        assert!(names.contains(&"gateway.handshake-one-effect"));
        assert!(names.contains(&"gateway.redelivery-after-reset"));
    }

    for conversation in conversations {
        let name = conversation.name.clone();
        with_watchdog(replay_conversation(conversation))
            .await
            .unwrap_or_else(|_| panic!("conversation replay timed out: {name}"))
            .unwrap_or_else(|error| {
                panic!("conversation {name} failed in established runner loop: {error}")
            });
    }
}

#[tokio::test]
async fn rejects_sequence_correct_acknowledgement_for_another_runner_frame() {
    let path = bundled_conversation_path("gateway.handshake-one-effect");
    let mut conversation = load_conversation(&path);
    let acknowledgement = gateway_acknowledgement_mut(&mut conversation, 2);
    *acknowledgement
        .pointer_mut("/payload/acknowledgedMessageId")
        .expect("gateway acknowledgement message ID") = Value::String("«mid-1»".to_owned());

    let error = with_watchdog(replay_conversation(conversation))
        .await
        .expect("corrupted conversation replay timed out")
        .expect_err("sequence-correct acknowledgement for another frame passed replay");
    assert_eq!(
        error.connection_cause(),
        ConnectionCause::MismatchedEffectAcknowledgement
    );

    with_watchdog(replay_conversation(load_conversation(&path)))
        .await
        .expect("restored conversation replay timed out")
        .expect("restored acknowledgement reference failed replay");
}

#[tokio::test]
async fn rejects_gateway_frame_recorded_after_terminal_close() {
    let path = bundled_conversation_path("gateway.handshake-one-effect");
    let mut conversation = load_conversation(&path);
    let mut trailing_acknowledgement = gateway_acknowledgement_mut(&mut conversation, 2).clone();
    *trailing_acknowledgement
        .pointer_mut("/payload/acknowledgedMessageId")
        .expect("gateway acknowledgement message ID") = Value::String("«mid-1»".to_owned());
    conversation.entries.push(ConversationEntry {
        advance_ms: 0,
        from: ConversationPeer::Gateway,
        kind: ConversationKind::Text,
        payload: Some(trailing_acknowledgement),
    });

    if std::panic::catch_unwind(|| validate_entries(&conversation)).is_err() {
        return;
    }
    let result = with_watchdog(replay_conversation(conversation))
        .await
        .expect("corrupted conversation replay timed out");
    assert!(
        result.is_err(),
        "replay accepted a gateway acknowledgement recorded after terminal close"
    );
}

fn gateway_acknowledgement_mut(
    conversation: &mut Conversation,
    acknowledged_sequence: u64,
) -> &mut Value {
    conversation
        .entries
        .iter_mut()
        .filter(|entry| entry.from == ConversationPeer::Gateway)
        .filter_map(|entry| entry.payload.as_mut())
        .find(|payload| {
            payload.get("type").and_then(Value::as_str) == Some("observation_ack")
                && payload
                    .pointer("/payload/acknowledgedSequence")
                    .and_then(Value::as_u64)
                    == Some(acknowledged_sequence)
        })
        .expect("conversation has no matching acknowledgement confirmation")
}

fn bundled_conversation_path(name: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/runner-conversations/v1")
        .join(format!("{name}.json"))
}

fn load_conversations() -> Vec<Conversation> {
    let mut paths: Vec<_> = if let Some(path) = std::env::var_os(REPLAY_OVERRIDE) {
        vec![Path::new(&path).to_path_buf()]
    } else {
        let directory =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/runner-conversations/v1");
        fs::read_dir(&directory)
            .unwrap_or_else(|error| panic!("read conversation fixture directory: {error}"))
            .map(|entry| entry.expect("read conversation fixture entry").path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "json")
            })
            .collect()
    };
    paths.sort();
    assert!(!paths.is_empty(), "no runner conversation fixtures found");
    paths
        .into_iter()
        .map(|path| load_conversation(&path))
        .collect()
}

fn load_conversation(path: &Path) -> Conversation {
    let encoded = fs::read(path)
        .unwrap_or_else(|error| panic!("read conversation {}: {error}", path.display()));
    let conversation: Conversation = serde_json::from_slice(&encoded)
        .unwrap_or_else(|error| panic!("decode conversation {}: {error}", path.display()));
    assert_eq!(conversation.conversation_version, 1);
    assert_eq!(conversation.recorded_by, "gateway");
    assert_eq!(
        path.file_stem().and_then(|name| name.to_str()),
        Some(conversation.name.as_str()),
        "conversation name does not match its file name"
    );
    validate_entries(&conversation);
    conversation
}

fn validate_entries(conversation: &Conversation) {
    assert!(
        !conversation.entries.is_empty(),
        "conversation {} has no entries",
        conversation.name
    );
    for (index, entry) in conversation.entries.iter().enumerate() {
        assert_eq!(
            entry.payload.is_some(),
            entry.kind == ConversationKind::Text,
            "conversation {} has an invalid payload for {:?}",
            conversation.name,
            entry.kind
        );
        assert!(
            entry.kind != ConversationKind::Close || index == conversation.entries.len() - 1,
            "conversation {} has an entry after a terminal close",
            conversation.name
        );
    }
}

async fn replay_conversation(conversation: Conversation) -> Result<(), ConnectionError> {
    let opening = opening_metadata(&conversation);
    let frame_source = ReplayFrameSource::new(&conversation);
    let config = Config::fixture(
        "ws://127.0.0.1:1/v1/runner/connect",
        test_credential(),
        true,
    )
    .expect("configure conversation replay gateway");
    assert_eq!(config.credential().runner_id(), opening.runner_id);
    let opening_message_id = frame_source.public_id("rmsg_");
    let encoded_opening = opening_hello(
        &frame_source,
        &opening.runner_id,
        REPLAY_BOOT_ID,
        opening_message_id.clone(),
        opening.sequence,
        "conversation-replay",
    )
    .expect("encode replay opening hello");
    let sleeper = LogicalSleeper::new();
    let (recorder, _capture) = test_recorder(REPLAY_BOOT_ID);
    let connection_event = recorder.start("runner.conversation_replay", []);
    let active_effect_event = ActiveEffectEvent::new();
    let assignment_manager = Mutex::new(AssignmentManager::new(
        &config,
        REPLAY_BOOT_ID.to_owned(),
        healthy_wall_clock(),
    ));
    let transcript = DeterminismTranscript::default();
    let (inbound, reader, writer, mut outbound) = scripted_duplex(transcript);
    let mut next_sequence = opening
        .sequence
        .checked_add(1)
        .expect("conversation opening sequence overflowed");
    let expected_final_sequence = conversation
        .entries
        .iter()
        .filter(|entry| entry.from == ConversationPeer::Runner)
        .filter_map(|entry| entry.payload.as_ref()?.get("sequence")?.as_u64())
        .max()
        .and_then(|sequence| sequence.checked_add(1))
        .expect("conversation has no valid runner sequence");
    let outcome = {
        let established = run_established(
            ConnectionDependencies::new(
                &config,
                &frame_source,
                &sleeper,
                &recorder,
                &connection_event,
                &active_effect_event,
                &assignment_manager,
                1,
            ),
            OpeningHello {
                boot_id: REPLAY_BOOT_ID,
                encoded: &encoded_opening,
                message_id: &opening_message_id,
                sequence: opening.sequence,
            },
            &mut next_sequence,
            reader,
            writer,
        );
        let peer = async {
            let mut normalizer = RunnerFrameNormalizer::default();
            let mut expected_sleep_requests = 0;
            for entry in &conversation.entries {
                sleeper.advance(Duration::from_millis(entry.advance_ms));
                match entry.from {
                    ConversationPeer::Gateway => {
                        inbound.send(gateway_message(entry, &frame_source));
                        if entry.kind != ConversationKind::Close {
                            expected_sleep_requests += 1;
                            sleeper.wait_for_requests(expected_sleep_requests).await;
                        }
                    }
                    ConversationPeer::Runner => {
                        let actual = outbound.recv().await.unwrap_or_else(|| {
                            panic!("runner stopped before entry in {}", conversation.name)
                        });
                        assert_runner_message(&conversation.name, entry, actual, &mut normalizer);
                        if expected_sleep_requests == 0 {
                            expected_sleep_requests = 1;
                            sleeper.wait_for_requests(expected_sleep_requests).await;
                        }
                    }
                }
            }
        };

        tokio::pin!(established);
        tokio::pin!(peer);
        tokio::select! {
            biased;
            outcome = &mut established => outcome,
            () = &mut peer => established.await,
        }
    };
    let progress = outcome?;
    assert!(progress.opening_acknowledged);
    assert!(progress.handshake_completed);
    assert_eq!(next_sequence, expected_final_sequence);
    assert!(
        outbound.try_recv().is_err(),
        "conversation {} left an unexpected runner frame",
        conversation.name
    );
    assert!(
        frame_source
            .message_ids
            .lock()
            .expect("conversation message ID mutex poisoned")
            .is_empty(),
        "conversation {} did not generate every expected runner frame",
        conversation.name
    );
    assert!(
        frame_source
            .timestamps
            .lock()
            .expect("conversation timestamp mutex poisoned")
            .is_empty(),
        "conversation {} did not generate every expected timestamp",
        conversation.name
    );
    Ok(())
}

struct OpeningMetadata {
    runner_id: String,
    sequence: u64,
}

fn opening_metadata(conversation: &Conversation) -> OpeningMetadata {
    let hello = conversation
        .entries
        .iter()
        .find(|entry| is_runner_text(entry, "hello"))
        .expect("conversation has no runner hello");
    let payload = hello.payload.as_ref().expect("runner hello payload");
    let sequence = payload["sequence"].as_u64().expect("runner hello sequence");
    OpeningMetadata {
        runner_id: payload["runnerId"]
            .as_str()
            .expect("runner hello runner ID")
            .to_owned(),
        sequence,
    }
}

fn runner_timestamps(conversation: &Conversation) -> VecDeque<String> {
    let mut concrete = HashMap::new();
    conversation
        .entries
        .iter()
        .filter(|entry| entry.from == ConversationPeer::Runner)
        .filter_map(|entry| entry.payload.as_ref())
        .map(|payload| {
            let placeholder = payload["sentAt"]
                .as_str()
                .expect("runner frame timestamp placeholder");
            let next = concrete.len();
            concrete
                .entry(placeholder.to_owned())
                .or_insert_with(|| {
                    if next == 0 {
                        REPLAY_TIMESTAMP.to_owned()
                    } else {
                        format!("2026-07-23T00:00:00.{next}Z")
                    }
                })
                .clone()
        })
        .collect()
}

fn is_runner_text(entry: &ConversationEntry, frame_type: &str) -> bool {
    entry.from == ConversationPeer::Runner
        && entry.kind == ConversationKind::Text
        && entry
            .payload
            .as_ref()
            .and_then(|payload| payload.get("type"))
            .and_then(Value::as_str)
            == Some(frame_type)
}

fn gateway_message(entry: &ConversationEntry, frame_source: &ReplayFrameSource) -> Message {
    match entry.kind {
        ConversationKind::Text => {
            let mut payload = entry.payload.clone().expect("gateway text payload");
            if payload.get("type").and_then(Value::as_str) == Some("observation_ack") {
                let normalized = payload
                    .pointer("/payload/acknowledgedMessageId")
                    .and_then(Value::as_str)
                    .expect("gateway acknowledgement message ID placeholder")
                    .to_owned();
                *payload
                    .pointer_mut("/payload/acknowledgedMessageId")
                    .expect("gateway acknowledgement message ID") =
                    Value::String(frame_source.concrete_message_id(&normalized));
            }
            Message::Text(
                serde_json::to_string(&payload)
                    .expect("encode gateway conversation frame")
                    .into(),
            )
        }
        ConversationKind::Ping => Message::Ping(Vec::new().into()),
        ConversationKind::Pong => Message::Pong(Vec::new().into()),
        ConversationKind::Close => Message::Close(None),
    }
}

fn assert_runner_message(
    conversation_name: &str,
    expected: &ConversationEntry,
    actual: Message,
    normalizer: &mut RunnerFrameNormalizer,
) {
    let (kind, payload) = match actual {
        Message::Text(text) => {
            let mut payload: Value = serde_json::from_str(&text)
                .unwrap_or_else(|error| panic!("decode replayed runner frame: {error}"));
            normalizer.normalize(&mut payload);
            (ConversationKind::Text, Some(payload))
        }
        Message::Ping(payload) => {
            assert!(payload.is_empty(), "runner conversation Ping has a payload");
            (ConversationKind::Ping, None)
        }
        Message::Pong(payload) => {
            assert!(payload.is_empty(), "runner conversation Pong has a payload");
            (ConversationKind::Pong, None)
        }
        Message::Close(_) => (ConversationKind::Close, None),
        Message::Binary(_) | Message::Frame(_) => {
            panic!("runner produced an unsupported conversation frame")
        }
    };
    assert_eq!(
        kind, expected.kind,
        "runner frame kind in {conversation_name}"
    );
    assert_eq!(
        payload, expected.payload,
        "normalized runner frame in {conversation_name}"
    );
}

#[derive(Default)]
struct RunnerFrameNormalizer {
    message_ids: HashMap<String, String>,
    timestamps: HashMap<String, String>,
    boot_ids: HashMap<String, String>,
}

impl RunnerFrameNormalizer {
    fn normalize(&mut self, frame: &mut Value) {
        let object = frame
            .as_object_mut()
            .expect("runner frame is not an object");
        normalize_indexed_string(object, "messageId", "mid", &mut self.message_ids);
        normalize_indexed_string(object, "sentAt", "ts", &mut self.timestamps);
        normalize_indexed_string(object, "bootId", "boot", &mut self.boot_ids);
        if let Some(version) = object
            .get_mut("payload")
            .and_then(Value::as_object_mut)
            .and_then(|payload| payload.get_mut("runnerVersion"))
        {
            assert!(version.is_string(), "runnerVersion is not a string");
            *version = Value::String("«ver»".to_owned());
        }
    }
}

fn normalize_indexed_string(
    object: &mut serde_json::Map<String, Value>,
    field: &str,
    placeholder: &str,
    replacements: &mut HashMap<String, String>,
) {
    let value = object
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("runner frame {field} is not a string"))
        .to_owned();
    let next = replacements.len() + 1;
    let replacement = replacements
        .entry(value)
        .or_insert_with(|| format!("«{placeholder}-{next}»"))
        .clone();
    object.insert(field.to_owned(), Value::String(replacement));
}

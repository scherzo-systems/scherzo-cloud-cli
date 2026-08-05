use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::io::{self, Read, Write};
use std::num::NonZeroU64;
use std::ops::Add;
use std::path::PathBuf;
use std::process::{ExitCode, Stdio};
use std::sync::Arc;

use jsonschema::error::ValidationErrorKind;
use jsonschema::{Draft, PatternOptions, Validator};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;

use super::admission::{CancellationReason, CancellationSource};
use super::agent::PositiveDuration;
use super::canonical_json::{self, CanonicalJsonError};
use super::coordinator::CoordinatorClock;

const JSON_SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";
const INTERNAL_WORKER_ENVIRONMENT: &str = "SCHERZO_INTERNAL_RESULT_VALIDATION_WORKER";
const INTERNAL_WORKER_VERSION: &str = "workflow-result-v1";
const MAXIMUM_SCHEMA_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_WORKER_CANDIDATE_BYTES: u64 = 1024 * 1024;
const MAXIMUM_WORKER_FEEDBACK_BYTES: u64 = 8 * 1024;
const MAXIMUM_REPORTED_FAILURES: usize = 16;
const MAXIMUM_JSON_ESCAPE_BYTES_PER_INPUT_BYTE: u64 = 6;
const WORKER_RESPONSE_JSON_OVERHEAD: u64 = br#"{"decision":"rejected","feedback":""}"#.len() as u64;

#[derive(Clone)]
pub(crate) struct RetainedResultSchema {
    bytes: Arc<[u8]>,
    document: Arc<Value>,
    validator: Arc<Validator>,
}

impl RetainedResultSchema {
    pub(crate) fn compile(
        bytes: Arc<[u8]>,
        document: Arc<Value>,
    ) -> Result<Self, ResultSchemaSupportFailure> {
        inspect_supported_schema(&document)?;
        let validator =
            compile_validator(&document).map_err(|_| ResultSchemaSupportFailure::Schema)?;
        Ok(Self {
            bytes,
            document,
            validator: Arc::new(validator),
        })
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn document(&self) -> &Value {
        &self.document
    }
}

impl fmt::Debug for RetainedResultSchema {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedResultSchema")
            .field("bytes", &self.bytes)
            .field("document", &self.document)
            .finish_non_exhaustive()
    }
}

impl PartialEq for RetainedResultSchema {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes && self.document == other.document
    }
}

impl Eq for RetainedResultSchema {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResultSchemaSupportFailure {
    Dialect,
    Reference,
    Schema,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundedSchemaValidAgentResult {
    value: Arc<Value>,
    canonical_json: Arc<[u8]>,
}

impl BoundedSchemaValidAgentResult {
    fn from_authoritative_validation(value: Arc<Value>, canonical_json: Arc<[u8]>) -> Self {
        Self {
            value,
            canonical_json,
        }
    }

    #[cfg(test)]
    pub(crate) fn fixture(value: Arc<Value>, canonical_json: Arc<[u8]>) -> Self {
        Self::from_authoritative_validation(value, canonical_json)
    }

    pub(crate) fn value(&self) -> &Value {
        &self.value
    }

    pub(crate) fn into_value(self) -> Arc<Value> {
        self.value
    }

    pub(crate) fn canonical_json(&self) -> &[u8] {
        &self.canonical_json
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResultValidationOutcome {
    Decided(ResultValidationDecision),
    Cancelled { reason: CancellationReason },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResultValidationDecision {
    Valid(BoundedSchemaValidAgentResult),
    Rejected { feedback: Arc<str> },
    Fatal(ResultValidationFatal),
}

impl From<ResultValidationDecision> for ResultValidationOutcome {
    fn from(decision: ResultValidationDecision) -> Self {
        Self::Decided(decision)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResultValidationFatal {
    LimitExceeded { deadline: PositiveDuration },
    WorkerFailed,
}

pub(crate) struct AuthoritativeResultValidator<Clock, Worker> {
    schema: RetainedResultSchema,
    maximum_candidate_bytes: NonZeroU64,
    maximum_feedback_bytes: NonZeroU64,
    deadline: PositiveDuration,
    clock: Clock,
    worker: Worker,
}

impl<Clock, Worker> AuthoritativeResultValidator<Clock, Worker>
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
{
    pub(crate) fn new(
        schema: RetainedResultSchema,
        maximum_candidate_bytes: NonZeroU64,
        maximum_feedback_bytes: NonZeroU64,
        deadline: PositiveDuration,
        clock: Clock,
        worker: Worker,
    ) -> Self {
        Self {
            schema,
            maximum_candidate_bytes: clamp_nonzero(
                maximum_candidate_bytes,
                MAXIMUM_WORKER_CANDIDATE_BYTES,
            ),
            maximum_feedback_bytes: clamp_nonzero(
                maximum_feedback_bytes,
                MAXIMUM_WORKER_FEEDBACK_BYTES,
            ),
            deadline,
            clock,
            worker,
        }
    }

    pub(crate) async fn validate(
        &self,
        candidate: Arc<Value>,
        cancellation: &CancellationSource,
    ) -> ResultValidationOutcome {
        if let Some(reason) = cancellation.cancellation_reason() {
            return ResultValidationOutcome::Cancelled { reason };
        }

        let canonical_json = match canonical_json::to_bounded_bytes(
            &candidate,
            self.maximum_candidate_bytes.get(),
        ) {
            Ok(bytes) => bytes,
            Err(CanonicalJsonError::SizeLimitExceeded) => {
                if let Some(reason) = cancellation.cancellation_reason() {
                    return ResultValidationOutcome::Cancelled { reason };
                }
                return ResultValidationDecision::Rejected {
                    feedback: bounded_feedback(
                        format!(
                            "Result rejected: canonical JSON exceeds the {}-byte limit.\n",
                            self.maximum_candidate_bytes
                        ),
                        self.maximum_feedback_bytes.get(),
                    ),
                }
                .into();
            }
            Err(CanonicalJsonError::SerializationFailed) => {
                return ResultValidationDecision::Fatal(ResultValidationFatal::WorkerFailed).into();
            }
        };

        if let Some(reason) = cancellation.cancellation_reason() {
            return ResultValidationOutcome::Cancelled { reason };
        }

        let mut clock = self.clock.clone();
        let deadline = clock.now().add(self.deadline.get());
        let request = ValidationWorkerRequest {
            schema: self.schema.clone(),
            candidate: Arc::clone(&candidate),
            canonical_json: Arc::clone(&canonical_json),
            maximum_feedback_bytes: self.maximum_feedback_bytes,
        };
        let mut cancellation_subscription = cancellation.subscribe();
        let mut running = match self.worker.start(request) {
            Ok(running) => running,
            Err(()) => {
                return cancellation.cancellation_reason().map_or_else(
                    || ResultValidationDecision::Fatal(ResultValidationFatal::WorkerFailed).into(),
                    |reason| ResultValidationOutcome::Cancelled { reason },
                );
            }
        };
        let cancellation_before_wait = *cancellation_subscription.borrow_and_update();
        if let Some(reason) = cancellation_before_wait {
            running.request_stop();
            running.quiesce().await;
            return ResultValidationOutcome::Cancelled { reason };
        }

        enum Completion {
            Worker(Result<ValidationWorkerDecision, ()>),
            Deadline,
            Cancellation(CancellationReason),
            CancellationSourceClosed,
        }

        let completion = {
            let worker = running.wait();
            let deadline_wait = clock.wait_until(deadline);
            tokio::pin!(worker);
            tokio::pin!(deadline_wait);
            tokio::select! {
                biased;
                changed = cancellation_subscription.changed() => {
                    match changed {
                        Ok(()) => cancellation_subscription
                            .borrow_and_update()
                            .map_or(Completion::CancellationSourceClosed, Completion::Cancellation),
                        Err(_) => Completion::CancellationSourceClosed,
                    }
                }
                () = &mut deadline_wait => Completion::Deadline,
                decision = &mut worker => Completion::Worker(decision),
            }
        };

        let decision = match completion {
            Completion::Cancellation(reason) => {
                running.request_stop();
                running.quiesce().await;
                return ResultValidationOutcome::Cancelled { reason };
            }
            Completion::Deadline => {
                running.request_stop();
                running.quiesce().await;
                if let Some(reason) = cancellation.cancellation_reason() {
                    return ResultValidationOutcome::Cancelled { reason };
                }
                return ResultValidationDecision::Fatal(ResultValidationFatal::LimitExceeded {
                    deadline: self.deadline,
                })
                .into();
            }
            Completion::CancellationSourceClosed => {
                running.request_stop();
                running.quiesce().await;
                return cancellation.cancellation_reason().map_or_else(
                    || ResultValidationDecision::Fatal(ResultValidationFatal::WorkerFailed).into(),
                    |reason| ResultValidationOutcome::Cancelled { reason },
                );
            }
            Completion::Worker(decision) => decision,
        };

        running.quiesce().await;
        if let Some(reason) = cancellation.cancellation_reason() {
            return ResultValidationOutcome::Cancelled { reason };
        }

        match decision {
            Ok(ValidationWorkerDecision::Valid) => ResultValidationDecision::Valid(
                BoundedSchemaValidAgentResult::from_authoritative_validation(
                    candidate,
                    canonical_json,
                ),
            ),
            Ok(ValidationWorkerDecision::Rejected { feedback }) => {
                ResultValidationDecision::Rejected { feedback }
            }
            Err(()) => ResultValidationDecision::Fatal(ResultValidationFatal::WorkerFailed),
        }
        .into()
    }
}

pub(crate) trait ResultValidationWorker: Clone + Send + Sync + 'static {
    type Running: RunningResultValidation;

    fn start(&self, request: ValidationWorkerRequest) -> Result<Self::Running, ()>;
}

pub(crate) trait RunningResultValidation: Send + 'static {
    fn wait(&mut self) -> impl Future<Output = Result<ValidationWorkerDecision, ()>> + Send;
    fn request_stop(&mut self);
    fn quiesce(self) -> impl Future<Output = ()> + Send;
}

#[derive(Clone)]
pub(crate) struct ValidationWorkerRequest {
    schema: RetainedResultSchema,
    candidate: Arc<Value>,
    canonical_json: Arc<[u8]>,
    maximum_feedback_bytes: NonZeroU64,
}

impl ValidationWorkerRequest {
    #[cfg(test)]
    pub(crate) fn evaluate(self) -> Result<ValidationWorkerDecision, ()> {
        evaluate_candidate(
            &self.schema,
            &self.candidate,
            self.maximum_feedback_bytes.get(),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValidationWorkerDecision {
    Valid,
    Rejected { feedback: Arc<str> },
}

#[derive(Clone, Debug)]
pub(crate) struct ProcessResultValidationWorker {
    executable: PathBuf,
}

impl ProcessResultValidationWorker {
    pub(crate) fn for_current_executable() -> io::Result<Self> {
        std::env::current_exe().map(|executable| Self { executable })
    }
}

impl ResultValidationWorker for ProcessResultValidationWorker {
    type Running = RunningProcessValidation;

    fn start(&self, request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
        let mut child = Command::new(&self.executable)
            .env(INTERNAL_WORKER_ENVIRONMENT, INTERNAL_WORKER_VERSION)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| ())?;
        let mut input = child.stdin.take().ok_or(())?;
        let output = child.stdout.take().ok_or(())?;
        let maximum_feedback_bytes = request.maximum_feedback_bytes;
        let input_task = tokio::spawn(async move {
            write_async_frame(&mut input, request.schema.bytes()).await?;
            write_async_frame(&mut input, &request.canonical_json).await?;
            input
                .write_all(&request.maximum_feedback_bytes.get().to_be_bytes())
                .await?;
            input.shutdown().await
        });
        Ok(RunningProcessValidation {
            child,
            input_task,
            input_finished: false,
            output,
            response: Vec::new(),
            process_finished: false,
            maximum_feedback_bytes,
        })
    }
}

pub(crate) struct RunningProcessValidation {
    child: Child,
    input_task: JoinHandle<io::Result<()>>,
    input_finished: bool,
    output: tokio::process::ChildStdout,
    response: Vec<u8>,
    process_finished: bool,
    maximum_feedback_bytes: NonZeroU64,
}

impl RunningResultValidation for RunningProcessValidation {
    async fn wait(&mut self) -> Result<ValidationWorkerDecision, ()> {
        if !self.input_finished {
            (&mut self.input_task)
                .await
                .map_err(|_| ())?
                .map_err(|_| ())?;
            self.input_finished = true;
        }
        let maximum_response_bytes = maximum_worker_response_bytes(self.maximum_feedback_bytes);
        (&mut self.output)
            .take(maximum_response_bytes.saturating_add(1))
            .read_to_end(&mut self.response)
            .await
            .map_err(|_| ())?;
        if u64::try_from(self.response.len()).map_or(true, |size| size > maximum_response_bytes) {
            return Err(());
        }
        if !self.process_finished {
            let status = self.child.wait().await.map_err(|_| ())?;
            self.process_finished = true;
            if !status.success() {
                return Err(());
            }
        }
        parse_worker_response(&self.response, self.maximum_feedback_bytes)
    }

    fn request_stop(&mut self) {
        if !self.process_finished {
            let _ = self.child.start_kill();
        }
    }

    async fn quiesce(mut self) {
        if !self.process_finished {
            let _ = self.child.start_kill();
            let _ = self.child.wait().await;
            self.process_finished = true;
        }
        if !self.input_finished {
            let _ = (&mut self.input_task).await;
            self.input_finished = true;
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
enum WorkerResponse {
    Valid,
    Rejected { feedback: String },
}

fn maximum_worker_response_bytes(maximum_feedback_bytes: NonZeroU64) -> u64 {
    maximum_feedback_bytes
        .get()
        .saturating_mul(MAXIMUM_JSON_ESCAPE_BYTES_PER_INPUT_BYTE)
        .saturating_add(WORKER_RESPONSE_JSON_OVERHEAD)
}

fn parse_worker_response(
    bytes: &[u8],
    maximum_feedback_bytes: NonZeroU64,
) -> Result<ValidationWorkerDecision, ()> {
    let response = serde_json::from_slice::<WorkerResponse>(bytes).map_err(|_| ())?;
    match response {
        WorkerResponse::Valid => Ok(ValidationWorkerDecision::Valid),
        WorkerResponse::Rejected { feedback }
            if u64::try_from(feedback.len())
                .is_ok_and(|size| size <= maximum_feedback_bytes.get()) =>
        {
            Ok(ValidationWorkerDecision::Rejected {
                feedback: Arc::from(feedback),
            })
        }
        WorkerResponse::Rejected { .. } => Err(()),
    }
}

async fn write_async_frame(
    destination: &mut tokio::process::ChildStdin,
    bytes: &[u8],
) -> io::Result<()> {
    let length = u64::try_from(bytes.len())
        .map_err(|_| io::Error::other("validation worker frame is too large"))?;
    destination.write_all(&length.to_be_bytes()).await?;
    destination.write_all(bytes).await
}

pub(crate) fn internal_worker_requested() -> bool {
    std::env::var(INTERNAL_WORKER_ENVIRONMENT).as_deref() == Ok(INTERNAL_WORKER_VERSION)
}

pub(crate) fn run_internal_worker() -> ExitCode {
    match run_internal_worker_io(&mut io::stdin().lock(), &mut io::stdout().lock()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(()) => ExitCode::FAILURE,
    }
}

fn run_internal_worker_io(input: &mut impl Read, output: &mut impl Write) -> Result<(), ()> {
    let schema_bytes = read_frame(input, MAXIMUM_SCHEMA_BYTES)?;
    let candidate_bytes = read_frame(input, MAXIMUM_WORKER_CANDIDATE_BYTES)?;
    let mut feedback_limit = [0_u8; 8];
    input.read_exact(&mut feedback_limit).map_err(|_| ())?;
    let feedback_limit = u64::from_be_bytes(feedback_limit);
    if feedback_limit == 0 || feedback_limit > MAXIMUM_WORKER_FEEDBACK_BYTES {
        return Err(());
    }
    let document = Arc::new(serde_json::from_slice::<Value>(&schema_bytes).map_err(|_| ())?);
    let schema =
        RetainedResultSchema::compile(Arc::from(schema_bytes), document).map_err(|_| ())?;
    let candidate = serde_json::from_slice::<Value>(&candidate_bytes).map_err(|_| ())?;
    let response = match evaluate_candidate(&schema, &candidate, feedback_limit)? {
        ValidationWorkerDecision::Valid => WorkerResponse::Valid,
        ValidationWorkerDecision::Rejected { feedback } => WorkerResponse::Rejected {
            feedback: feedback.to_string(),
        },
    };
    serde_json::to_writer(output, &response).map_err(|_| ())
}

fn read_frame(input: &mut impl Read, maximum_bytes: u64) -> Result<Vec<u8>, ()> {
    let mut length = [0_u8; 8];
    input.read_exact(&mut length).map_err(|_| ())?;
    let length = u64::from_be_bytes(length);
    if length > maximum_bytes {
        return Err(());
    }
    let length = usize::try_from(length).map_err(|_| ())?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(|_| ())?;
    bytes.resize(length, 0);
    input.read_exact(&mut bytes).map_err(|_| ())?;
    Ok(bytes)
}

fn evaluate_candidate(
    schema: &RetainedResultSchema,
    candidate: &Value,
    maximum_feedback_bytes: u64,
) -> Result<ValidationWorkerDecision, ()> {
    let mut errors = schema.validator.iter_errors(candidate);
    let Some(first) = errors.next() else {
        return Ok(ValidationWorkerDecision::Valid);
    };

    let mut feedback = FeedbackBuilder::new(maximum_feedback_bytes);
    feedback.push("Result rejected by the workflow schema:\n");
    let mut current = Some(first);
    let mut count = 0_usize;
    while count < MAXIMUM_REPORTED_FAILURES && !feedback.is_full() {
        let Some(error) = current.take().or_else(|| errors.next()) else {
            break;
        };
        if matches!(
            error.kind(),
            ValidationErrorKind::RegexEngineFailure { .. }
                | ValidationErrorKind::BacktrackLimitExceeded { .. }
                | ValidationErrorKind::Referencing(_)
        ) {
            return Err(());
        }
        count += 1;
        let instance_path = error.instance_path().to_string();
        let schema_path = error.schema_path().to_string();
        feedback.push(&format!(
            "{count}. instance {} violates `{}` at schema {}\n",
            display_pointer(&instance_path, "$"),
            error.kind().keyword(),
            display_pointer(&schema_path, "#"),
        ));
    }
    Ok(ValidationWorkerDecision::Rejected {
        feedback: feedback.finish(),
    })
}

fn display_pointer<'a>(pointer: &'a str, root: &'static str) -> &'a str {
    if pointer.is_empty() { root } else { pointer }
}

struct FeedbackBuilder {
    bytes: String,
    maximum_bytes: usize,
}

impl FeedbackBuilder {
    fn new(maximum_bytes: u64) -> Self {
        Self {
            bytes: String::new(),
            maximum_bytes: usize::try_from(maximum_bytes).unwrap_or(usize::MAX),
        }
    }

    fn push(&mut self, text: &str) {
        let remaining = self.maximum_bytes.saturating_sub(self.bytes.len());
        let end = floor_char_boundary(text, remaining.min(text.len()));
        self.bytes.push_str(&text[..end]);
    }

    fn is_full(&self) -> bool {
        self.bytes.len() >= self.maximum_bytes
    }

    fn finish(self) -> Arc<str> {
        Arc::from(self.bytes)
    }
}

fn clamp_nonzero(value: NonZeroU64, maximum: u64) -> NonZeroU64 {
    let Some(value) = NonZeroU64::new(value.get().min(maximum)) else {
        unreachable!("result-validation hard limits are positive");
    };
    value
}

fn bounded_feedback(feedback: String, maximum_bytes: u64) -> Arc<str> {
    let mut bounded = FeedbackBuilder::new(maximum_bytes);
    bounded.push(&feedback);
    bounded.finish()
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    while !value.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn compile_validator(schema: &Value) -> Result<Validator, ()> {
    Validator::options()
        .with_draft(Draft::Draft202012)
        .with_pattern_options(PatternOptions::regex())
        .build(schema)
        .map_err(|_| ())
}

fn inspect_supported_schema(schema: &Value) -> Result<(), ResultSchemaSupportFailure> {
    let Some(root) = schema.as_object() else {
        return Err(ResultSchemaSupportFailure::Dialect);
    };
    if root.get("$schema").and_then(Value::as_str) != Some(JSON_SCHEMA_DIALECT) {
        return Err(ResultSchemaSupportFailure::Dialect);
    }

    let mut inspection = SchemaInspection::default();
    inspection.walk(schema, "", true)?;
    inspection.validate_references()
}

#[derive(Default)]
struct SchemaInspection<'a> {
    schema_locations: BTreeSet<String>,
    anchors: BTreeMap<String, AnchorTarget>,
    references: Vec<&'a str>,
}

#[derive(Clone)]
enum AnchorTarget {
    Unique(String),
    Ambiguous,
}

impl<'a> SchemaInspection<'a> {
    fn walk(
        &mut self,
        schema: &'a Value,
        pointer: &str,
        root: bool,
    ) -> Result<(), ResultSchemaSupportFailure> {
        self.schema_locations.insert(pointer.to_owned());
        let Some(object) = schema.as_object() else {
            return Ok(());
        };

        if (!root && object.contains_key("$schema")) || object.contains_key("$vocabulary") {
            return Err(ResultSchemaSupportFailure::Dialect);
        }
        if !root && object.contains_key("$id") {
            return Err(ResultSchemaSupportFailure::Reference);
        }

        for keyword in ["$ref", "$dynamicRef"] {
            if let Some(reference) = object.get(keyword).and_then(Value::as_str) {
                if !reference.starts_with('#') {
                    return Err(ResultSchemaSupportFailure::Reference);
                }
                self.references.push(reference);
            }
        }
        for keyword in ["$anchor", "$dynamicAnchor"] {
            if let Some(anchor) = object.get(keyword).and_then(Value::as_str) {
                self.record_anchor(anchor, pointer);
            }
        }

        for keyword in [
            "additionalProperties",
            "contains",
            "contentSchema",
            "else",
            "if",
            "items",
            "not",
            "propertyNames",
            "then",
            "unevaluatedItems",
            "unevaluatedProperties",
        ] {
            if let Some(child) = object.get(keyword) {
                self.walk_if_schema(child, &join_pointer(pointer, keyword))?;
            }
        }
        for keyword in ["allOf", "anyOf", "oneOf", "prefixItems"] {
            if let Some(children) = object.get(keyword).and_then(Value::as_array) {
                for (index, child) in children.iter().enumerate() {
                    self.walk_if_schema(
                        child,
                        &join_pointer(&join_pointer(pointer, keyword), &index.to_string()),
                    )?;
                }
            }
        }
        for keyword in [
            "$defs",
            "dependentSchemas",
            "patternProperties",
            "properties",
        ] {
            if let Some(children) = object.get(keyword).and_then(Value::as_object) {
                for (name, child) in children {
                    self.walk_if_schema(
                        child,
                        &join_pointer(&join_pointer(pointer, keyword), name),
                    )?;
                }
            }
        }
        Ok(())
    }

    fn walk_if_schema(
        &mut self,
        value: &'a Value,
        pointer: &str,
    ) -> Result<(), ResultSchemaSupportFailure> {
        if value.is_object() || value.is_boolean() {
            self.walk(value, pointer, false)?;
        }
        Ok(())
    }

    fn record_anchor(&mut self, anchor: &str, pointer: &str) {
        match self.anchors.entry(anchor.to_owned()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(AnchorTarget::Unique(pointer.to_owned()));
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if !matches!(entry.get(), AnchorTarget::Unique(existing) if existing == pointer) {
                    entry.insert(AnchorTarget::Ambiguous);
                }
            }
        }
    }

    fn validate_references(self) -> Result<(), ResultSchemaSupportFailure> {
        if self
            .anchors
            .values()
            .any(|target| matches!(target, AnchorTarget::Ambiguous))
        {
            return Err(ResultSchemaSupportFailure::Reference);
        }
        for reference in self.references {
            let fragment = decode_uri_fragment(&reference[1..])?;
            if fragment.is_empty() {
                continue;
            }
            if fragment.starts_with('/') {
                if !self.schema_locations.contains(&fragment) {
                    return Err(ResultSchemaSupportFailure::Reference);
                }
            } else if !matches!(self.anchors.get(&fragment), Some(AnchorTarget::Unique(_))) {
                return Err(ResultSchemaSupportFailure::Reference);
            }
        }
        Ok(())
    }
}

fn decode_uri_fragment(fragment: &str) -> Result<String, ResultSchemaSupportFailure> {
    let bytes = fragment.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0_usize;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        let high = bytes
            .get(index + 1)
            .and_then(|byte| hex_value(*byte))
            .ok_or(ResultSchemaSupportFailure::Reference)?;
        let low = bytes
            .get(index + 2)
            .and_then(|byte| hex_value(*byte))
            .ok_or(ResultSchemaSupportFailure::Reference)?;
        decoded.push((high << 4) | low);
        index += 3;
    }
    String::from_utf8(decoded).map_err(|_| ResultSchemaSupportFailure::Reference)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn join_pointer(parent: &str, token: &str) -> String {
    format!("{parent}/{}", escape_pointer_token(token))
}

fn escape_pointer_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

#[cfg(test)]
mod tests;

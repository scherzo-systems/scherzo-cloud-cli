mod git;
pub(crate) mod process;

use std::collections::BTreeSet;
use std::fmt;

use git::GitCheck;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CheckDescriptor {
    pub(crate) id: &'static str,
    pub(crate) title: &'static str,
    pub(crate) default: bool,
}

pub(crate) trait DoctorCheck: Send + Sync {
    fn descriptor(&self) -> CheckDescriptor;
    fn run(&self) -> Outcome;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Status {
    Pass,
    Fail,
}

impl Status {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Outcome {
    pub(crate) status: Status,
    pub(crate) code: &'static str,
    pub(crate) message: String,
    pub(crate) details: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct CheckResult {
    pub(crate) descriptor: CheckDescriptor,
    pub(crate) outcome: Outcome,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Summary {
    pub(crate) passed: usize,
    pub(crate) failed: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Report {
    pub(crate) results: Vec<CheckResult>,
}

impl Report {
    pub(crate) fn summary(&self) -> Summary {
        self.results.iter().fold(
            Summary {
                passed: 0,
                failed: 0,
            },
            |mut summary, result| {
                match result.outcome.status {
                    Status::Pass => summary.passed += 1,
                    Status::Fail => summary.failed += 1,
                }
                summary
            },
        )
    }

    pub(crate) fn has_failures(&self) -> bool {
        self.results
            .iter()
            .any(|result| result.outcome.status == Status::Fail)
    }
}

pub(crate) struct Registry {
    checks: Vec<Box<dyn DoctorCheck>>,
}

impl Registry {
    pub(crate) fn new() -> Self {
        Self { checks: Vec::new() }
    }

    pub(crate) fn register(&mut self, check: Box<dyn DoctorCheck>) -> Result<(), RegistryError> {
        let descriptor = check.descriptor();
        validate_descriptor(descriptor)?;
        if self
            .checks
            .iter()
            .any(|registered| registered.descriptor().id == descriptor.id)
        {
            return Err(RegistryError::DuplicateId(descriptor.id));
        }
        self.checks.push(check);
        Ok(())
    }

    pub(crate) fn descriptors(&self) -> Vec<CheckDescriptor> {
        self.checks.iter().map(|check| check.descriptor()).collect()
    }

    pub(crate) fn run(&self, requested: &[String]) -> Result<Report, SelectionError> {
        for id in requested {
            if !self.checks.iter().any(|check| check.descriptor().id == id) {
                return Err(SelectionError::UnknownId(id.clone()));
            }
        }

        let requested_ids = requested
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let selected = |descriptor: CheckDescriptor| {
            if requested.is_empty() {
                descriptor.default
            } else {
                requested_ids.contains(descriptor.id)
            }
        };
        let results = self
            .checks
            .iter()
            .filter_map(|check| {
                let descriptor = check.descriptor();
                selected(descriptor).then(|| CheckResult {
                    descriptor,
                    outcome: check.run(),
                })
            })
            .collect();

        Ok(Report { results })
    }
}

pub(crate) fn built_in_registry() -> Result<Registry, RegistryError> {
    let mut registry = Registry::new();
    registry.register(Box::new(GitCheck::system()))?;
    Ok(registry)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegistryError {
    InvalidId(&'static str),
    EmptyTitle(&'static str),
    DuplicateId(&'static str),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidId(id) => write!(formatter, "runner doctor check ID is invalid: {id}"),
            Self::EmptyTitle(id) => write!(formatter, "runner doctor check title is empty: {id}"),
            Self::DuplicateId(id) => {
                write!(formatter, "runner doctor check ID is duplicated: {id}")
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum SelectionError {
    UnknownId(String),
}

impl fmt::Display for SelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownId(id) => write!(
                formatter,
                "unknown runner doctor check '{id}'; use 'scherzo-cloud runner doctor --list-checks' to list available checks"
            ),
        }
    }
}

fn validate_descriptor(descriptor: CheckDescriptor) -> Result<(), RegistryError> {
    if !is_valid_check_id(descriptor.id) {
        return Err(RegistryError::InvalidId(descriptor.id));
    }
    if descriptor.title.is_empty() {
        return Err(RegistryError::EmptyTitle(descriptor.id));
    }
    Ok(())
}

fn is_valid_check_id(id: &str) -> bool {
    let mut segments = id.split('.');
    let mut count = 0;

    for segment in &mut segments {
        count += 1;
        let bytes = segment.as_bytes();
        let Some((&first, rest)) = bytes.split_first() else {
            return false;
        };
        if !first.is_ascii_lowercase() {
            return false;
        }

        let mut previous_hyphen = false;
        for byte in rest {
            if byte.is_ascii_lowercase() || byte.is_ascii_digit() {
                previous_hyphen = false;
            } else if *byte == b'-' && !previous_hyphen {
                previous_hyphen = true;
            } else {
                return false;
            }
        }
        if previous_hyphen {
            return false;
        }
    }

    count >= 2
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{
        CheckDescriptor, DoctorCheck, Outcome, Registry, RegistryError, Report, Status,
        built_in_registry,
    };

    struct FakeCheck {
        descriptor: CheckDescriptor,
        outcome: Outcome,
        runs: Arc<AtomicUsize>,
    }

    impl DoctorCheck for FakeCheck {
        fn descriptor(&self) -> CheckDescriptor {
            self.descriptor
        }

        fn run(&self) -> Outcome {
            self.runs.fetch_add(1, Ordering::SeqCst);
            Outcome {
                status: self.outcome.status,
                code: self.outcome.code,
                message: self.outcome.message.clone(),
                details: self.outcome.details.clone(),
            }
        }
    }

    fn fake_check(
        id: &'static str,
        default: bool,
        status: Status,
        runs: Arc<AtomicUsize>,
    ) -> FakeCheck {
        FakeCheck {
            descriptor: CheckDescriptor {
                id,
                title: "Fixture check",
                default,
            },
            outcome: Outcome {
                status,
                code: "fixture",
                message: "Fixture result".to_owned(),
                details: BTreeMap::new(),
            },
            runs,
        }
    }

    #[test]
    fn registration_preserves_order_and_rejects_duplicate_ids() {
        let mut registry = Registry::new();
        registry
            .register(Box::new(fake_check(
                "extension.fixture.check",
                true,
                Status::Pass,
                Arc::new(AtomicUsize::new(0)),
            )))
            .unwrap();
        registry
            .register(Box::new(fake_check(
                "environment.command.git-lfs",
                false,
                Status::Pass,
                Arc::new(AtomicUsize::new(0)),
            )))
            .unwrap();

        assert_eq!(
            registry
                .descriptors()
                .into_iter()
                .map(|descriptor| descriptor.id)
                .collect::<Vec<_>>(),
            ["extension.fixture.check", "environment.command.git-lfs"]
        );
        assert_eq!(
            registry.register(Box::new(fake_check(
                "extension.fixture.check",
                true,
                Status::Pass,
                Arc::new(AtomicUsize::new(0)),
            ))),
            Err(RegistryError::DuplicateId("extension.fixture.check"))
        );
    }

    #[test]
    fn registration_rejects_invalid_namespaced_ids() {
        for id in [
            "git",
            "Environment.git",
            "environment..git",
            "environment.command.git-",
            "environment.command.a--b",
        ] {
            let mut registry = Registry::new();
            assert_eq!(
                registry.register(Box::new(fake_check(
                    id,
                    true,
                    Status::Pass,
                    Arc::new(AtomicUsize::new(0)),
                ))),
                Err(RegistryError::InvalidId(id))
            );
        }
    }

    #[test]
    fn empty_selection_runs_only_default_checks() {
        let default_runs = Arc::new(AtomicUsize::new(0));
        let non_default_runs = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::new();
        registry
            .register(Box::new(fake_check(
                "extension.fixture.default",
                true,
                Status::Pass,
                Arc::clone(&default_runs),
            )))
            .unwrap();
        registry
            .register(Box::new(fake_check(
                "extension.fixture.extra",
                false,
                Status::Pass,
                Arc::clone(&non_default_runs),
            )))
            .unwrap();

        let report = registry.run(&[]).unwrap();

        assert_eq!(report.results.len(), 1);
        assert_eq!(report.results[0].descriptor.id, "extension.fixture.default");
        assert_eq!(default_runs.load(Ordering::SeqCst), 1);
        assert_eq!(non_default_runs.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn explicit_selection_deduplicates_and_uses_registry_order() {
        let first_runs = Arc::new(AtomicUsize::new(0));
        let second_runs = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::new();
        registry
            .register(Box::new(fake_check(
                "extension.fixture.first",
                false,
                Status::Pass,
                Arc::clone(&first_runs),
            )))
            .unwrap();
        registry
            .register(Box::new(fake_check(
                "extension.fixture.second",
                false,
                Status::Pass,
                Arc::clone(&second_runs),
            )))
            .unwrap();

        let report = registry
            .run(&[
                "extension.fixture.second".to_owned(),
                "extension.fixture.first".to_owned(),
                "extension.fixture.second".to_owned(),
            ])
            .unwrap();

        assert_eq!(
            report
                .results
                .iter()
                .map(|result| result.descriptor.id)
                .collect::<Vec<_>>(),
            ["extension.fixture.first", "extension.fixture.second"]
        );
        assert_eq!(first_runs.load(Ordering::SeqCst), 1);
        assert_eq!(second_runs.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn unknown_selection_is_rejected_without_running_checks() {
        let runs = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::new();
        registry
            .register(Box::new(fake_check(
                "extension.fixture.check",
                true,
                Status::Pass,
                Arc::clone(&runs),
            )))
            .unwrap();

        let error = registry
            .run(&["extension.fixture.missing".to_owned()])
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "unknown runner doctor check 'extension.fixture.missing'; use 'scherzo-cloud runner doctor --list-checks' to list available checks"
        );
        assert_eq!(runs.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn summary_counts_pass_and_fail() {
        let passed_result = super::CheckResult {
            descriptor: CheckDescriptor {
                id: "extension.fixture.pass",
                title: "Pass",
                default: true,
            },
            outcome: Outcome {
                status: Status::Pass,
                code: "ok",
                message: "Passed".to_owned(),
                details: BTreeMap::new(),
            },
        };
        let report = Report {
            results: vec![
                passed_result,
                super::CheckResult {
                    descriptor: CheckDescriptor {
                        id: "extension.fixture.fail",
                        title: "Fail",
                        default: true,
                    },
                    outcome: Outcome {
                        status: Status::Fail,
                        code: "failed",
                        message: "Failed".to_owned(),
                        details: BTreeMap::new(),
                    },
                },
            ],
        };
        let passed = Report {
            results: vec![super::CheckResult {
                descriptor: CheckDescriptor {
                    id: "extension.fixture.pass-only",
                    title: "Pass only",
                    default: true,
                },
                outcome: Outcome {
                    status: Status::Pass,
                    code: "ok",
                    message: "Passed".to_owned(),
                    details: BTreeMap::new(),
                },
            }],
        };

        assert_eq!(
            report.summary(),
            super::Summary {
                passed: 1,
                failed: 1,
            }
        );
        assert!(report.has_failures());
        assert!(!passed.has_failures());
    }

    #[test]
    fn built_in_registry_contains_the_default_git_check() {
        let registry = built_in_registry().unwrap();

        assert_eq!(
            registry.descriptors(),
            vec![CheckDescriptor {
                id: "environment.command.git",
                title: "Git",
                default: true,
            }]
        );
    }
}

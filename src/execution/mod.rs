pub(crate) mod claude_code;
pub(crate) mod codex;
mod harness_installation;
pub(crate) mod pi;
pub(crate) mod workflow;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AgentHarnessInstallationFailure {
    Pi(pi::PiInstallationFailure),
    ClaudeCode(claude_code::ClaudeCodeInstallationFailure),
    Codex(codex::CodexInstallationFailure),
}

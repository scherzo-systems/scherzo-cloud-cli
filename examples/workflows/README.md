# Scherzo workflow examples

These Workflow V1 examples provide several shapes and terminal states for Scherzo Cloud
presentation and execution testing.

| Workflow | Presentation case |
| --- | --- |
| `basic.yaml` | Two parallel steps followed by a join |
| `single-step.yaml` | Minimal one-step command success |
| `agent-basic.yaml` | One low-thinking Luna agent answering basic arithmetic |
| `agent-claude-basic.yaml` | One low-effort Claude Code agent answering basic arithmetic |
| `agent-claude-structured-result.yaml` | Claude Code producing a schema-validated result |
| `agent-claude-attachment.yaml` | Claude Code summarizing an imported text attachment |
| `agent-claude-result-pipeline.yaml` | Validated JSON passed between two Claude Code agents |
| `hostile-output.yaml` | Deterministic long lines, Unicode, Markdown, controls, and no final newline |
| `large-response.yaml` | A deterministic, bounded 200-line output stream |
| `agent-parallel.yaml` | Three concurrent agents followed by an agent join |
| `agent-expected-failure.yaml` | Intentional startup failure from a nonexistent model |
| `agent-invalid-result.yaml` | Intentional rejection by an unsatisfiable result schema |
| `agent-slow-cancel.yaml` | A 120-second tool call intended for cancellation |
| `sequential.yaml` | Four short stages in a strict sequence |
| `parallel-fanout.yaml` | A wider fan-out followed by a join |
| `output-streams.yaml` | stdout, stderr, a quiet step, and an unterminated line |
| `finalizers.yaml` | Trigger filtering, ordering, context delivery, and advisory finalizer failure |
| `long-running-output.yaml` | One progress line every second for 15 minutes |
| `expected-failure.yaml` | An intentional exit 17 with downstream blocked work |

Every command step starts only `sh` and uses shell built-ins plus `sleep`. Except for
`finalizers.yaml` reading its engine-provided `finalization.context` input, the command
examples do not read inputs or files, write files, inspect the machine or environment, or
use the network. `long-running-output.yaml` intentionally takes about 15 minutes so a
client can attach and follow its output. `expected-failure.yaml` returns a nonzero status
by design. `finalizers.yaml` still succeeds overall despite its intentional advisory
finalizer failure; the other command examples succeed without an intentional failure.

`hostile-output.yaml` and `large-response.yaml` use command steps so their exact bytes
are deterministic and free of model cost. Agent examples are reserved for agent-specific
lifecycle, protocol, result, and cancellation behavior.

`agent-basic.yaml` is the smallest Pi smoke test and uses
`openai-codex/gpt-5.6-luna` with low thinking. `agent-claude-basic.yaml` is the smallest
Claude Code smoke test and uses `claude-haiku-4-5-20251001` with low effort. The other
Claude examples show schema-validated results, imported attachment delivery, and an inferred
data dependency that passes a validated JSON result to a second agent as an attachment.
Result-producing steps use low-effort Sonnet; response-only steps use low-effort Haiku.
They require a Scherzo Cloud build that admits the `claude_code` harness. Running an agent
example requires its selected harness and provider credentials, contacts the model
provider, and may consume billed tokens. Model-generated output remains probabilistic.

Run the attachment example with the included brief:

```sh
scherzo-cloud workflow run \
  --source-root . \
  --execution-root "$execution_root" \
  --run-dir "$run_dir" \
  --attachment text/plain attachments/aurora-brief.txt \
  agent-claude-attachment.yaml
```

The failure and cancellation examples are deliberately disruptive:

- `agent-expected-failure.yaml` selects a nonexistent model.
- `agent-invalid-result.yaml` uses contradictory numeric bounds so no structured result
  can pass validation.
- `agent-slow-cancel.yaml` asks the agent to run `sleep 120`; cancel it while that tool
  call is active rather than waiting for normal completion.

## Validate an agent workflow

Definition validation is offline and does not contact the model provider:

```sh
scherzo-cloud workflow validate \
  --source-root . \
  agent-basic.yaml
```

## Validate and run a workflow

From this directory, with `scherzo-cloud` on `PATH`, set `workflow` to an example that
needs no imports. Scherzo needs an execution directory and a new run directory; this
example creates both under one temporary directory and removes them when it exits.

```sh
(
  set -eu
  workflow=parallel-fanout.yaml
  tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/scherzo-example.XXXXXX")
  trap 'rm -rf -- "$tmp_dir"' EXIT HUP INT TERM

  mkdir "$tmp_dir/execution" "$tmp_dir/results"

  scherzo-cloud workflow validate \
    --source-root . \
    "$workflow"

  scherzo-cloud workflow run \
    --source-root . \
    --execution-root "$tmp_dir/execution" \
    --run-dir "$tmp_dir/results/${workflow%.yaml}" \
    --max-parallel 4 \
    --plain \
    "$workflow"
)
```

Each invocation uses a fresh run path because `workflow run` intentionally refuses to
overwrite an existing run directory.

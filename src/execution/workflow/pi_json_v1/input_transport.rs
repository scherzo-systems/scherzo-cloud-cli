use std::path::{Path, PathBuf};

use serde::Serialize;

use super::result_bridge::{
    materialize_extension_config, result_tool_name, validate_result_endpoint_directory,
    write_private_file,
};
use crate::execution::workflow::agent::AgentInvocationIdentity;

const EXTENSION_FILE_NAME: &str = "pi-json-v1-input-extension.ts";
const SYSTEM_PROMPT_FILE_NAME: &str = "system-prompt.md";
const CONFIG_MARKER: &str = "\"__SCHERZO_PI_JSON_V1_INPUT_CONFIG_JSON__\"";
const EXTENSION_TEMPLATE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/execution/workflow/pi-json-v1-extension/src/pi-json-v1-input.ts"
));

#[derive(Debug)]
pub(super) struct PreparedInputTransport {
    extension_path: PathBuf,
    message_marker: Option<String>,
    system_prompt_marker: Option<String>,
}

impl PreparedInputTransport {
    pub(super) fn prepare(
        identity: &AgentInvocationIdentity,
        staging_directory: &Path,
        system_prompt: Option<&str>,
        message_path: Option<&Path>,
    ) -> Result<Self, ()> {
        validate_result_endpoint_directory(staging_directory)?;

        let identity = result_tool_name(identity)?;
        let system_prompt_marker = system_prompt.map(|_| format!("__{identity}_system_prompt__"));
        let message_marker = message_path.map(|_| format!("__{identity}_message__"));
        let system_prompt_path = system_prompt.map(|prompt| {
            let path = staging_directory.join(SYSTEM_PROMPT_FILE_NAME);
            write_private_file(&path, prompt.as_bytes()).map(|()| path)
        });
        let system_prompt_path = system_prompt_path.transpose().map_err(|_| ())?;
        let extension_path = staging_directory.join(EXTENSION_FILE_NAME);
        let message = staged_input_config(message_path, message_marker.as_deref())?;
        let system_prompt = staged_input_config(
            system_prompt_path.as_deref(),
            system_prompt_marker.as_deref(),
        )?;
        let source = materialize_extension_config(
            EXTENSION_TEMPLATE,
            CONFIG_MARKER,
            &InputExtensionConfig {
                message,
                system_prompt,
            },
        )?;
        write_private_file(&extension_path, source.as_bytes()).map_err(|_| ())?;

        Ok(Self {
            extension_path,
            message_marker,
            system_prompt_marker,
        })
    }

    pub(super) fn extension_path(&self) -> &Path {
        &self.extension_path
    }

    pub(super) fn message_marker(&self) -> Option<&str> {
        self.message_marker.as_deref()
    }

    pub(super) fn system_prompt_marker(&self) -> Option<&str> {
        self.system_prompt_marker.as_deref()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InputExtensionConfig<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<StagedInputConfig<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system_prompt: Option<StagedInputConfig<'a>>,
}

#[derive(Serialize)]
struct StagedInputConfig<'a> {
    marker: &'a str,
    path: &'a str,
}

fn staged_input_config<'a>(
    path: Option<&'a Path>,
    marker: Option<&'a str>,
) -> Result<Option<StagedInputConfig<'a>>, ()> {
    path.zip(marker)
        .map(|(path, marker)| {
            Ok(StagedInputConfig {
                marker,
                path: path.to_str().ok_or(())?,
            })
        })
        .transpose()
}

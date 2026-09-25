use super::*;

impl ListCommand {
    pub(super) fn execute(self, deployment: Deployment) -> super::super::CommandResult {
        let context = self
            .integration_context
            .iter()
            .map(|pair| {
                pair.split_once('=')
                    .filter(|(key, _)| !key.is_empty())
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| anyhow!("invalid integration context filter; use KEY=VALUE"))?;
        let result = with_api(
            &deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| {
                api.list(
                    &self.organization,
                    RunListFilter {
                        limit: self.limit,
                        cursor: self.cursor.as_deref(),
                        project_id: self.project_id.as_deref(),
                        state_group: self.state_group.as_deref(),
                        created_after: self.created_after.as_deref(),
                        context: &context,
                    },
                )
            },
        )?;
        match result {
            Ok(list) => write_list(&list, self.options.json).map_err(Into::into),
            Err(failure) => write_failure(
                deployment.fingerprint().api_url(),
                &self.organization,
                None,
                &failure,
                self.options.authentication.kind(),
                self.options.json,
            )
            .map_err(Into::into),
        }
    }
}

fn write_list(list: &RunList, json: bool) -> anyhow::Result<ExitCode> {
    if json {
        write_json(list)?;
        return Ok(ExitCode::Success);
    }
    let mut out = io::stdout().lock();
    if list.items.is_empty() {
        writeln!(out, "No runs found.")?;
    }
    for item in &list.items {
        writeln!(
            out,
            "{}  {}  {}",
            item.id,
            format!("{:?}", item.state).to_lowercase(),
            item.created_at
        )?;
        writeln!(
            out,
            "  name: {}",
            item.display_name
                .as_deref()
                .map(visible_text)
                .unwrap_or_else(|| "—".to_owned())
        )?;
        writeln!(out, "  project: {}", item.project_id)?;
        writeln!(out, "  workflow: {}", visible_text(&item.workflow_path))?;
        writeln!(out, "  updated: {}", item.updated_at)?;
        if let Some(place) = &item.placement {
            writeln!(
                out,
                "  runner: {} ({})",
                visible_text(&place.runner_name),
                place.runner_id
            )?;
            writeln!(
                out,
                "  pool: {} ({})",
                visible_text(&place.pool_name),
                place.pool_id
            )?;
        }
        for (key, value) in {
            let mut entries = item.integration_context.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| *key);
            entries
        } {
            writeln!(
                out,
                "  context {}: {}",
                visible_text(key),
                visible_text(value)
            )?;
        }
    }
    if let Some(cursor) = &list.next_cursor {
        writeln!(out, "next cursor: {cursor}")?;
    }
    Ok(ExitCode::Success)
}

use super::*;
use base64::Engine as _;
use scherzo_cloud_runner_protocol::{
    ArtifactConfirmationOutcome, ArtifactConfirmationResponse, ArtifactResultConfirmationOutcome,
    ArtifactResultConfirmationResponse, ArtifactUploadCapability,
};
use scherzo_cloud_test_support::ScriptedHttpServer;

#[tokio::test]
async fn decoded_v2_assignment_delivers_multiple_carriers_before_the_result() {
    let workflow = r#"schemaVersion: 1
steps:
  write:
    kind: cmd
    command:
      argv: [sh, -c, 'printf first > first.txt; printf second > second.txt']
    outputs:
      first:
        kind: file
        from: path
        path: first.txt
        mediaType: text/plain
      second:
        kind: file
        from: path
        path: second.txt
        mediaType: text/plain
exports:
  first:
    ref: outputs.write.first
  second:
    ref: outputs.write.second
"#;
    let (_temporary, mut manager) = manager_fixture(workflow);
    manager.artifact_delivery = ArtifactDeliveryBroker::new(
        manager.outbox.clone(),
        Arc::clone(&manager.sleeper),
        true,
        None,
    );
    let offered = decoded_source_display_offer();
    offer_then_prepare(&mut manager, &offered).await;
    assert!(matches!(manager.slot, Some(LocalSlot::Accepted(_))));
    spawn_execution(&mut manager, &offered);

    let artifact_set_id = "ats_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned();
    let carrier_id = "acr_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned();
    let mut uploads = Vec::new();
    let mut confirmed_carriers = 0;
    let reports = with_watchdog(async {
        let notification = manager.notification();
        loop {
            let notified = notification.notified();
            tokio::pin!(notified);
            let pending = manager.pending_observations(&BTreeSet::new(), 100);
            for entry in pending {
                let terminal = entry.observation.is_terminal();
                match entry.observation {
                    AssignmentObservation::Artifact {
                        delivery_id,
                        request,
                    } => {
                        let is_result = matches!(request, ArtifactRequest::RegisterResult { .. });
                        let media_type = match &request {
                            ArtifactRequest::RegisterCarrier { media_type, .. } => {
                                media_type.clone()
                            }
                            _ => "application/json".to_owned(),
                        };
                        let response = match request {
                            ArtifactRequest::RegisterCarrier {
                                size_bytes, sha256, ..
                            }
                            | ArtifactRequest::RegisterResult {
                                size_bytes, sha256, ..
                            } => {
                                if is_result {
                                    assert_eq!(confirmed_carriers, 2);
                                }
                                let server = ScriptedHttpServer::respond(
                                    b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"
                                        .to_vec(),
                                );
                                let checksum = (0..sha256.len())
                                    .step_by(2)
                                    .map(|index| {
                                        u8::from_str_radix(&sha256[index..index + 2], 16).unwrap()
                                    })
                                    .collect::<Vec<_>>();
                                let capability = ArtifactUploadCapability {
                                    url: server.api_url.clone(),
                                    content_length: size_bytes.to_string(),
                                    content_type: media_type,
                                    if_none_match: "*".to_owned(),
                                    checksum_sha256: base64::engine::general_purpose::STANDARD
                                        .encode(checksum),
                                    expires_at: "2099-01-01T00:00:00Z".to_owned(),
                                };
                                uploads.push(server);
                                if is_result {
                                    ArtifactCloudResponse::ResultRegistration(
                                        ArtifactResultRegistrationResponse {
                                            request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc"
                                                .to_owned(),
                                            outcome: ArtifactResultRegistrationOutcome::Succeeded {
                                                artifact_set_id: artifact_set_id.clone(),
                                                finalization_deadline: "2099-01-01T00:00:00Z"
                                                    .to_owned(),
                                                upload_capability: capability,
                                            },
                                        },
                                    )
                                } else {
                                    ArtifactCloudResponse::CarrierRegistration(
                                        ArtifactRegistrationResponse {
                                            request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc"
                                                .to_owned(),
                                            outcome: ArtifactRegistrationOutcome::Succeeded {
                                                artifact_set_id: artifact_set_id.clone(),
                                                carrier_id: carrier_id.clone(),
                                                upload_capability: capability,
                                            },
                                        },
                                    )
                                }
                            }
                            ArtifactRequest::ConfirmCarrier { .. } => {
                                confirmed_carriers += 1;
                                ArtifactCloudResponse::CarrierConfirmation(
                                    ArtifactConfirmationResponse {
                                        request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abd"
                                            .to_owned(),
                                        outcome: ArtifactConfirmationOutcome::Confirmed {
                                            artifact_set_id: artifact_set_id.clone(),
                                            carrier_id: carrier_id.clone(),
                                        },
                                    },
                                )
                            }
                            ArtifactRequest::ConfirmResult { .. } => {
                                ArtifactCloudResponse::ResultConfirmation(
                                    ArtifactResultConfirmationResponse {
                                        request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abd"
                                            .to_owned(),
                                        outcome: ArtifactResultConfirmationOutcome::Confirmed {
                                            artifact_set_id: artifact_set_id.clone(),
                                        },
                                    },
                                )
                            }
                        };
                        manager
                            .handle_artifact_response(entry.id, delivery_id, response)
                            .unwrap();
                    }
                    AssignmentObservation::Execution { report, .. } if terminal => {
                        return vec![report];
                    }
                    _ => manager.acknowledge_observation(entry.id),
                }
            }
            notified.await;
        }
    })
    .await
    .unwrap();
    assert_succeeded(&reports);
    assert!(
        matches!(&reports[0], ExecutionReport::Finished { artifact_delivery, .. }
        if artifact_delivery["outcome"] == "prepared")
    );
    let requests = tokio::task::spawn_blocking(move || {
        uploads
            .into_iter()
            .map(ScriptedHttpServer::finish_one)
            .collect::<Vec<_>>()
    })
    .await
    .unwrap();
    let bodies = requests
        .iter()
        .map(|request| request.split_once("\r\n\r\n").unwrap().1)
        .collect::<Vec<_>>();
    assert_eq!(&bodies[..2], &["first", "second"]);
    let result: Value = serde_json::from_str(bodies[2]).unwrap();
    assert_eq!(
        result["workflow"]["provenance"]["sourceDisplaySnapshot"]["projectName"],
        offered
            .execution_spec
            .source_display_snapshot
            .unwrap()
            .project_name
    );
    acknowledge_terminal_and_settle(&mut manager).await;
}

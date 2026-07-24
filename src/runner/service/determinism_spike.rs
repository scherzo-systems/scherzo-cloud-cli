use std::time::Duration;

use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use super::Config;
use super::connection::{OpeningHello, opening_hello, run_established};
use super::test_support::{
    DeterminismTranscript, assignment_offer, controlled_sleeper_with_transcript,
    deterministic_frame_source, observation_acknowledgement, scripted_duplex, sleep_request,
    welcome, with_watchdog,
};
use crate::runner::credential::test_credential;

const REPETITIONS: usize = 1_000;
const BOOT_ID: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abe";
const OPENING_MESSAGE_ID: &str = "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc";

#[tokio::test]
async fn established_connection_has_a_deterministic_transcript() {
    let mut expected = None;
    for repetition in 0..REPETITIONS {
        let actual = with_watchdog(run_scenario())
            .await
            .expect("determinism spike scenario timed out");
        if let Some(expected) = &expected {
            assert_eq!(
                &actual,
                expected,
                "runner transcript diverged at repetition {}\nfirst: {expected:#?}\nactual: {actual:#?}",
                repetition + 1,
            );
        } else {
            expected = Some(actual);
        }
    }
}

async fn run_scenario() -> Vec<String> {
    let config = Config::new("ws://127.0.0.1:1/v1/connect", test_credential(), true)
        .expect("configure deterministic gateway");
    let frame_source = deterministic_frame_source();
    let opening = opening_hello(
        frame_source.as_ref(),
        config.credential().runner_id(),
        BOOT_ID,
        OPENING_MESSAGE_ID.to_owned(),
        1,
        "0.2.0",
    )
    .expect("encode deterministic opening hello");
    let transcript = DeterminismTranscript::default();
    let (sleeper, mut sleep_requests) = controlled_sleeper_with_transcript(transcript.clone());
    let (inbound, reader, writer, mut outbound) = scripted_duplex(transcript.clone());
    let mut next_sequence = 2;

    let connection = run_established(
        &config,
        frame_source.as_ref(),
        sleeper.as_ref(),
        OpeningHello {
            boot_id: BOOT_ID,
            encoded: &opening,
            message_id: OPENING_MESSAGE_ID,
            sequence: 1,
        },
        &mut next_sequence,
        reader,
        writer,
    );
    let peer = async {
        let hello = next_outbound(&mut outbound).await;
        let Message::Text(hello) = hello else {
            panic!("opening event was not text");
        };
        let hello: Value = serde_json::from_str(&hello).expect("decode opening hello");
        assert_eq!(hello["type"], "hello");
        assert_eq!(hello["messageId"], OPENING_MESSAGE_ID);

        let welcome_timer = sleep_request(&mut sleep_requests, Duration::from_secs(5)).await;
        inbound.send(welcome());
        let first_silence_timer = sleep_request(&mut sleep_requests, Duration::from_secs(2)).await;
        inbound.send(observation_acknowledgement(OPENING_MESSAGE_ID, 1));
        let second_silence_timer = sleep_request(&mut sleep_requests, Duration::from_secs(2)).await;
        inbound.send(assignment_offer());

        let acknowledgement = next_outbound(&mut outbound).await;
        let Message::Text(acknowledgement) = acknowledgement else {
            panic!("effect acknowledgement was not text");
        };
        let acknowledgement: Value =
            serde_json::from_str(&acknowledgement).expect("decode effect acknowledgement");
        assert_eq!(acknowledgement["type"], "effect_acknowledged");
        assert_eq!(acknowledgement["sequence"], 2);
        assert_eq!(
            acknowledgement["payload"]["effectId"],
            "eff_01k0z6r1w8f4jy2m7q9v3x5abg"
        );

        let stress_silence_timer = sleep_request(&mut sleep_requests, Duration::from_secs(2)).await;
        inbound.send(Message::Ping(b"spike".to_vec().into()));
        stress_silence_timer.release();
        let pong = next_outbound(&mut outbound).await;
        assert_eq!(pong, Message::Pong(b"spike".to_vec().into()));

        inbound.send(Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "spike complete".into(),
        })));
        drop(welcome_timer);
        drop(first_silence_timer);
        drop(second_silence_timer);
    };

    let (outcome, ()) = tokio::join!(connection, peer);
    let outcome = outcome.expect("run established deterministic connection");
    assert!(outcome.opening_acknowledged);
    assert!(outcome.handshake_completed);
    assert_eq!(next_sequence, 3);
    transcript.snapshot()
}

async fn next_outbound(outbound: &mut mpsc::UnboundedReceiver<Message>) -> Message {
    outbound
        .recv()
        .await
        .expect("scripted writer closed before the expected event")
}

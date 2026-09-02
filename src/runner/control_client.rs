use std::fmt;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use super::control_protocol::{
    Operation, ProtocolFailure, RESPONSE_LIMIT, Response, decode_response, encode_request,
};

const IO_TIMEOUT: Duration = Duration::from_secs(2);
const RELOAD_RESPONSE_TIMEOUT: Duration = Duration::from_secs(32);

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum RequestFailure {
    NotReachable,
    Protocol(ProtocolFailure),
}

impl fmt::Display for RequestFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotReachable => formatter.write_str("Runner Serve is not reachable"),
            Self::Protocol(failure) => failure.fmt(formatter),
        }
    }
}

impl std::error::Error for RequestFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotReachable => None,
            Self::Protocol(failure) => Some(failure),
        }
    }
}

pub(crate) fn request(
    socket_path: &Path,
    operation: Operation,
) -> Result<Response, RequestFailure> {
    let mut stream = UnixStream::connect(socket_path).map_err(|_| RequestFailure::NotReachable)?;
    let response_timeout = match operation {
        Operation::Status => IO_TIMEOUT,
        Operation::ReloadCredential => RELOAD_RESPONSE_TIMEOUT,
    };
    stream
        .set_read_timeout(Some(response_timeout))
        .and_then(|()| stream.set_write_timeout(Some(IO_TIMEOUT)))
        .map_err(|_| RequestFailure::NotReachable)?;
    stream
        .write_all(&encode_request(operation))
        .and_then(|()| stream.flush())
        .map_err(|_| RequestFailure::NotReachable)?;

    let mut response = Vec::with_capacity(512);
    let mut chunk = [0_u8; 1024];
    loop {
        let read = stream
            .read(&mut chunk)
            .map_err(|_| RequestFailure::NotReachable)?;
        if read == 0 {
            return Err(if response.is_empty() {
                RequestFailure::NotReachable
            } else {
                RequestFailure::Protocol(ProtocolFailure::Invalid)
            });
        }
        response.extend_from_slice(&chunk[..read]);
        if response.len() > RESPONSE_LIMIT {
            return Err(RequestFailure::Protocol(ProtocolFailure::Oversized));
        }
        if let Some(newline) = response.iter().position(|byte| *byte == b'\n') {
            if newline + 1 != response.len() {
                return Err(RequestFailure::Protocol(ProtocolFailure::Invalid));
            }
            break;
        }
    }
    decode_response(&response).map_err(RequestFailure::Protocol)
}

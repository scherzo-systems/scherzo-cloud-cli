use std::fmt;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use super::control_protocol::{
    Operation, RESPONSE_LIMIT, Response, decode_response, encode_request,
};

const IO_TIMEOUT: Duration = Duration::from_secs(2);
const RELOAD_RESPONSE_TIMEOUT: Duration = Duration::from_secs(32);

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct NotReachable;

impl fmt::Display for NotReachable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Runner Serve is not reachable")
    }
}

impl std::error::Error for NotReachable {}

pub(crate) fn request(socket_path: &Path, operation: Operation) -> Result<Response, NotReachable> {
    let mut stream = UnixStream::connect(socket_path).map_err(|_| NotReachable)?;
    let response_timeout = match operation {
        Operation::Status => IO_TIMEOUT,
        Operation::ReloadCredential => RELOAD_RESPONSE_TIMEOUT,
    };
    stream
        .set_read_timeout(Some(response_timeout))
        .and_then(|()| stream.set_write_timeout(Some(IO_TIMEOUT)))
        .map_err(|_| NotReachable)?;
    stream
        .write_all(&encode_request(operation))
        .and_then(|()| stream.flush())
        .map_err(|_| NotReachable)?;

    let mut response = Vec::with_capacity(512);
    let mut chunk = [0_u8; 1024];
    loop {
        let read = stream.read(&mut chunk).map_err(|_| NotReachable)?;
        if read == 0 {
            return Err(NotReachable);
        }
        response.extend_from_slice(&chunk[..read]);
        if response.len() > RESPONSE_LIMIT {
            return Err(NotReachable);
        }
        if let Some(newline) = response.iter().position(|byte| *byte == b'\n') {
            if newline + 1 != response.len() {
                return Err(NotReachable);
            }
            break;
        }
    }
    decode_response(&response).map_err(|_| NotReachable)
}

pub(crate) mod credential;
pub(crate) mod doctor;
pub(crate) mod service;
mod telemetry;

fn is_loopback(endpoint: &url::Url) -> bool {
    match endpoint.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

#[allow(dead_code)]
fn reasonless_suppression() {}

fn ordinary_warning() {}

fn panic_shortcut(value: Option<u8>) -> u8 {
    value.unwrap()
}

fn lossy_cast(value: u64) -> u8 {
    value as u8
}

fn main() {
    let _ = panic_shortcut as fn(Option<u8>) -> u8;
    let _ = lossy_cast as fn(u64) -> u8;
}

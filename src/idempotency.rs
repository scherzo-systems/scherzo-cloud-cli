const RANDOM_KEY_BYTES: usize = 32;
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

pub(crate) fn generate_idempotency_key() -> Result<String, getrandom::Error> {
    let mut random = [0_u8; RANDOM_KEY_BYTES];
    getrandom::fill(&mut random)?;
    let mut key = String::with_capacity(RANDOM_KEY_BYTES * 2);
    for byte in random {
        key.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        key.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::generate_idempotency_key;

    #[test]
    fn idempotency_keys_are_opaque_lowercase_hexadecimal() {
        let first = generate_idempotency_key().expect("random key should be available");
        let second = generate_idempotency_key().expect("random key should be available");

        assert_eq!(first.len(), 64);
        assert!(
            first
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
        assert_ne!(first, second);
    }
}

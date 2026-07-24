use nanoid::nanoid;

const OPERATION_ID_LENGTH: usize = 21;

pub fn generate(prefix: &str) -> String {
    format!("{prefix}_{}", nanoid!(OPERATION_ID_LENGTH))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_ids_are_prefixed_url_safe_and_unique() {
        let first = generate("pg");
        let second = generate("pg");
        assert!(first.starts_with("pg_"));
        assert_eq!(first.len(), 3 + OPERATION_ID_LENGTH);
        assert!(
            first
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        );
        assert_ne!(first, second);
    }
}

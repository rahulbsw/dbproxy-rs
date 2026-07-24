use sha1::Sha1;
use sha2::{Digest, Sha256};

/// Computes the `mysql_native_password` response for a plaintext password.
///
/// MySQL sends `SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))`.
pub fn native_password_response(password: &str, salt: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }

    let stage1 = Sha1::digest(password.as_bytes());
    let stage2 = Sha1::digest(stage1);

    let mut input = Vec::with_capacity(salt.len() + stage2.len());
    input.extend_from_slice(salt);
    input.extend_from_slice(&stage2);
    let scramble = Sha1::digest(input);

    stage1
        .iter()
        .zip(scramble.iter())
        .map(|(left, right)| left ^ right)
        .collect()
}

pub fn verify_native_password(password: &str, salt: &[u8], auth_data: &[u8]) -> bool {
    constant_time_eq(&native_password_response(password, salt), auth_data)
}

/// Computes the `caching_sha2_password` fast-authentication response.
pub fn caching_sha2_password_response(password: &str, salt: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }

    let stage1 = Sha256::digest(password.as_bytes());
    let stage2 = Sha256::digest(stage1);
    let mut input = Vec::with_capacity(stage2.len() + salt.len());
    input.extend_from_slice(&stage2);
    input.extend_from_slice(salt);
    let scramble = Sha256::digest(input);
    stage1
        .iter()
        .zip(scramble.iter())
        .map(|(left, right)| left ^ right)
        .collect()
}

pub fn verify_caching_sha2_password(password: &str, salt: &[u8], auth_data: &[u8]) -> bool {
    constant_time_eq(&caching_sha2_password_response(password, salt), auth_data)
}

fn constant_time_eq(expected: &[u8], actual: &[u8]) -> bool {
    if expected.len() != actual.len() {
        return false;
    }

    expected
        .iter()
        .zip(actual)
        .fold(0_u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_password_has_empty_response() {
        assert!(native_password_response("", b"salt").is_empty());
    }

    #[test]
    fn verifies_native_password_without_timing_shortcut() {
        let salt = b"01234567890123456789";
        let response = native_password_response("correct horse", salt);
        assert!(verify_native_password("correct horse", salt, &response));
        assert!(!verify_native_password("wrong", salt, &response));
    }

    #[test]
    fn verifies_caching_sha2_fast_authentication() {
        let salt = b"01234567890123456789";
        let response = caching_sha2_password_response("correct horse", salt);
        assert!(verify_caching_sha2_password(
            "correct horse",
            salt,
            &response
        ));
        assert!(!verify_caching_sha2_password("wrong", salt, &response));
    }
}

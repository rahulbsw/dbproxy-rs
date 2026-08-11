use std::{error::Error, fmt::Write as _, io};

use bigdecimal::BigDecimal;
use bytes::{BufMut, BytesMut};
use pgwire::types::{ToSqlText, format::FormatOptions};
use tokio_postgres::types::{FromSql, IsNull, ToSql, Type, to_sql_checked};

const NUMERIC_NEGATIVE: u16 = 0x4000;
const NUMERIC_SPECIAL: u16 = 0xc000;
const NUMERIC_MAX_DISPLAY_SCALE: usize = 16_383;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub(super) struct PgBigDecimal(pub BigDecimal);

impl<'a> FromSql<'a> for PgBigDecimal {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() < 8 {
            return Err(invalid_numeric("truncated numeric header"));
        }
        let groups = u16::from_be_bytes([raw[0], raw[1]]) as usize;
        let weight = i16::from_be_bytes([raw[2], raw[3]]) as isize;
        let sign = u16::from_be_bytes([raw[4], raw[5]]);
        let scale = u16::from_be_bytes([raw[6], raw[7]]) as usize;
        if sign & NUMERIC_SPECIAL == NUMERIC_SPECIAL {
            return Err(invalid_numeric("special numeric values are not supported"));
        }
        if raw.len() != 8_usize.saturating_add(groups.saturating_mul(2)) {
            return Err(invalid_numeric("invalid numeric payload length"));
        }

        let digits = raw[8..]
            .chunks_exact(2)
            .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]))
            .collect::<Vec<_>>();
        if digits.iter().any(|digit| *digit >= 10_000) {
            return Err(invalid_numeric("invalid base-10000 numeric digit"));
        }

        let integer_groups = weight + 1;
        let mut integer = String::new();
        if integer_groups > 0 {
            for position in 0..integer_groups as usize {
                let digit = digits.get(position).copied().unwrap_or_default();
                write!(&mut integer, "{digit:04}")?;
            }
            let trimmed = integer.trim_start_matches('0');
            integer = if trimmed.is_empty() {
                "0".to_owned()
            } else {
                trimmed.to_owned()
            };
        } else {
            integer.push('0');
        }

        let mut fraction = String::new();
        if integer_groups < 0 {
            fraction.extend(std::iter::repeat_n('0', (-integer_groups) as usize * 4));
        }
        let first_fraction_group = integer_groups.max(0) as usize;
        for digit in digits.iter().skip(first_fraction_group) {
            write!(&mut fraction, "{digit:04}")?;
        }
        if fraction.len() < scale {
            fraction.extend(std::iter::repeat_n('0', scale - fraction.len()));
        }
        fraction.truncate(scale);

        let mut value = String::new();
        if sign == NUMERIC_NEGATIVE && digits.iter().any(|digit| *digit != 0) {
            value.push('-');
        } else if sign != 0 {
            return Err(invalid_numeric("invalid numeric sign"));
        }
        value.push_str(&integer);
        if scale > 0 {
            value.push('.');
            value.push_str(&fraction);
        }
        value
            .parse::<BigDecimal>()
            .map(Self)
            .map_err(|_| invalid_numeric("numeric value cannot be represented"))
    }

    fn accepts(data_type: &Type) -> bool {
        *data_type == Type::NUMERIC
    }
}

impl ToSql for PgBigDecimal {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let plain = self.0.to_plain_string();
        let (negative, unsigned) = plain
            .strip_prefix('-')
            .map_or((false, plain.as_str()), |value| (true, value));
        let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
        if fraction.len() > NUMERIC_MAX_DISPLAY_SCALE {
            return Err(invalid_numeric("numeric scale exceeds PostgreSQL limits"));
        }

        let integer_groups = integer.len().max(1).div_ceil(4);
        let mut grouped = String::with_capacity((integer_groups + fraction.len().div_ceil(4)) * 4);
        grouped.extend(std::iter::repeat_n('0', integer_groups * 4 - integer.len()));
        grouped.push_str(integer);
        grouped.push_str(fraction);
        let trailing = (4 - fraction.len() % 4) % 4;
        grouped.extend(std::iter::repeat_n('0', trailing));
        let mut digits = grouped
            .as_bytes()
            .chunks_exact(4)
            .map(|chunk| {
                std::str::from_utf8(chunk)
                    .ok()
                    .and_then(|chunk| chunk.parse::<u16>().ok())
                    .ok_or_else(|| invalid_numeric("invalid decimal digit"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut weight = isize::try_from(integer_groups)? - 1;
        if let Some(first_nonzero) = digits.iter().position(|digit| *digit != 0) {
            digits.drain(..first_nonzero);
            weight -= isize::try_from(first_nonzero)?;
        } else {
            digits.clear();
        }
        while digits.last() == Some(&0) {
            digits.pop();
        }
        if digits.is_empty() {
            weight = 0;
        }
        let weight = i16::try_from(weight)
            .map_err(|_| invalid_numeric("numeric weight exceeds PostgreSQL limits"))?;
        let group_count = u16::try_from(digits.len())
            .map_err(|_| invalid_numeric("numeric precision exceeds PostgreSQL limits"))?;

        out.reserve(8 + digits.len() * 2);
        out.put_u16(group_count);
        out.put_i16(weight);
        out.put_u16(if negative && !digits.is_empty() {
            NUMERIC_NEGATIVE
        } else {
            0
        });
        out.put_u16(fraction.len() as u16);
        for digit in digits {
            out.put_u16(digit);
        }
        Ok(IsNull::No)
    }

    fn accepts(data_type: &Type) -> bool {
        *data_type == Type::NUMERIC
    }

    to_sql_checked!();
}

impl ToSqlText for PgBigDecimal {
    fn to_sql_text(
        &self,
        data_type: &Type,
        out: &mut BytesMut,
        _: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if !<Self as ToSql>::accepts(data_type) {
            return Err(invalid_numeric("PgBigDecimal requires PostgreSQL NUMERIC"));
        }
        out.put_slice(self.0.to_plain_string().as_bytes());
        Ok(IsNull::No)
    }
}

fn invalid_numeric(message: &'static str) -> Box<dyn Error + Sync + Send> {
    Box::new(io::Error::new(io::ErrorKind::InvalidData, message))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(value: &str) {
        let original = PgBigDecimal(value.parse().unwrap());
        let mut encoded = BytesMut::new();
        original.to_sql(&Type::NUMERIC, &mut encoded).unwrap();
        let decoded = PgBigDecimal::from_sql(&Type::NUMERIC, &encoded).unwrap();
        assert_eq!(decoded, original, "{value}");
    }

    #[test]
    fn postgres_numeric_codec_preserves_arbitrary_precision() {
        for value in [
            "0",
            "-0.000001",
            "3950.123456",
            "999999999999999999999999999999999999999999999999999999999999.12345678901234567890",
            "0.000000000000000000000000000000000000000000000000000000000001",
        ] {
            round_trip(value);
        }
    }
}

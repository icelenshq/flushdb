use flushdb_types::{FlushError, FlushResult};

pub fn encode_varint(value: u64, buf: &mut Vec<u8>) {
    let mut v = value;
    loop {
        let mut byte = (v & 0x7F) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if v == 0 {
            break;
        }
    }
}

pub fn decode_varint(buf: &[u8]) -> FlushResult<(u64, usize)> {
    if buf.is_empty() {
        return Err(FlushError::CorruptedData {
            message: "varint buffer is empty".into(),
        });
    }

    let mut value: u64 = 0;
    let mut shift: u32 = 0;

    for (i, &byte) in buf.iter().enumerate() {
        if i >= 10 {
            return Err(FlushError::CorruptedData {
                message: "varint exceeds 10 bytes".into(),
            });
        }
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
        shift += 7;
    }

    Err(FlushError::CorruptedData {
        message: "varint is truncated".into(),
    })
}

pub fn varint_len(value: u64) -> usize {
    if value == 0 {
        return 1;
    }
    let bits = 64 - value.leading_zeros() as usize;
    bits.div_ceil(7)
}

/// MurmurHash3 x64 128-bit variant (safe Rust implementation).
pub fn murmurhash3_x64_128(data: &[u8], seed: u32) -> (u64, u64) {
    let seed = seed as u64;
    let mut h1: u64 = seed;
    let mut h2: u64 = seed;

    let c1: u64 = 0x87c3_7b91_1142_53d5;
    let c2: u64 = 0x4cf5_ad43_2745_937f;

    let nblocks = data.len() / 16;

    for i in 0..nblocks {
        let offset = i * 16;
        let k1 = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        let k2 = u64::from_le_bytes(data[offset + 8..offset + 16].try_into().unwrap());

        let k1 = k1.wrapping_mul(c1);
        let k1 = k1.rotate_left(31);
        let k1 = k1.wrapping_mul(c2);
        h1 ^= k1;

        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dce729);

        let k2 = k2.wrapping_mul(c2);
        let k2 = k2.rotate_left(33);
        let k2 = k2.wrapping_mul(c1);
        h2 ^= k2;

        h2 = h2.rotate_left(31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x38495ab5);
    }

    let tail = &data[nblocks * 16..];
    let mut k1: u64 = 0;
    let mut k2: u64 = 0;

    match tail.len() {
        15 => {
            k2 ^= (tail[14] as u64) << 48;
            k2 ^= (tail[13] as u64) << 40;
            k2 ^= (tail[12] as u64) << 32;
            k2 ^= (tail[11] as u64) << 24;
            k2 ^= (tail[10] as u64) << 16;
            k2 ^= (tail[9] as u64) << 8;
            k2 ^= tail[8] as u64;
            k2 = k2.wrapping_mul(c2);
            k2 = k2.rotate_left(33);
            k2 = k2.wrapping_mul(c1);
            h2 ^= k2;

            k1 ^= (tail[7] as u64) << 56;
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        14 => {
            k2 ^= (tail[13] as u64) << 40;
            k2 ^= (tail[12] as u64) << 32;
            k2 ^= (tail[11] as u64) << 24;
            k2 ^= (tail[10] as u64) << 16;
            k2 ^= (tail[9] as u64) << 8;
            k2 ^= tail[8] as u64;
            k2 = k2.wrapping_mul(c2);
            k2 = k2.rotate_left(33);
            k2 = k2.wrapping_mul(c1);
            h2 ^= k2;

            k1 ^= (tail[7] as u64) << 56;
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        13 => {
            k2 ^= (tail[12] as u64) << 32;
            k2 ^= (tail[11] as u64) << 24;
            k2 ^= (tail[10] as u64) << 16;
            k2 ^= (tail[9] as u64) << 8;
            k2 ^= tail[8] as u64;
            k2 = k2.wrapping_mul(c2);
            k2 = k2.rotate_left(33);
            k2 = k2.wrapping_mul(c1);
            h2 ^= k2;

            k1 ^= (tail[7] as u64) << 56;
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        12 => {
            k2 ^= (tail[11] as u64) << 24;
            k2 ^= (tail[10] as u64) << 16;
            k2 ^= (tail[9] as u64) << 8;
            k2 ^= tail[8] as u64;
            k2 = k2.wrapping_mul(c2);
            k2 = k2.rotate_left(33);
            k2 = k2.wrapping_mul(c1);
            h2 ^= k2;

            k1 ^= (tail[7] as u64) << 56;
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        11 => {
            k2 ^= (tail[10] as u64) << 16;
            k2 ^= (tail[9] as u64) << 8;
            k2 ^= tail[8] as u64;
            k2 = k2.wrapping_mul(c2);
            k2 = k2.rotate_left(33);
            k2 = k2.wrapping_mul(c1);
            h2 ^= k2;

            k1 ^= (tail[7] as u64) << 56;
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        10 => {
            k2 ^= (tail[9] as u64) << 8;
            k2 ^= tail[8] as u64;
            k2 = k2.wrapping_mul(c2);
            k2 = k2.rotate_left(33);
            k2 = k2.wrapping_mul(c1);
            h2 ^= k2;

            k1 ^= (tail[7] as u64) << 56;
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        9 => {
            k2 ^= tail[8] as u64;
            k2 = k2.wrapping_mul(c2);
            k2 = k2.rotate_left(33);
            k2 = k2.wrapping_mul(c1);
            h2 ^= k2;

            k1 ^= (tail[7] as u64) << 56;
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        8 => {
            k1 ^= (tail[7] as u64) << 56;
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        7 => {
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        6 => {
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        5 => {
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        4 => {
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        3 => {
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        2 => {
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        1 => {
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        _ => {}
    }

    h1 ^= data.len() as u64;
    h2 ^= data.len() as u64;

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    (h1, h2)
}

fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ceb9fe1a85ec53);
    k ^= k >> 33;
    k
}

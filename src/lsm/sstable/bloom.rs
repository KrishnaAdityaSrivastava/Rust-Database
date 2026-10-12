use std::io;
use xxhash_rust::xxh3::xxh3_128;


const HASH_ALGORITHM_XXH3_128: u8 = 1;
const HEADER_SIZE: usize = 25;
// const FALSE_POSITIVE_RATE: f64 = 0.01;

pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: usize,
    num_hashes: usize,
}

impl BloomFilter {
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        assert!(
            false_positive_rate > 0.0 && false_positive_rate < 1.0,
            "false-positive rate must be between 0 and 1"
        );

        let n = expected_items.max(1) as f64;
        let p = false_positive_rate;

        let num_bits = ((-n * p.ln()) / 2.0_f64.ln().powi(2))
            .ceil()
            .max(8.0) as usize;

        let num_hashes = (((num_bits as f64 / n) * 2.0_f64.ln()).round()
            as usize)
            .max(1);

        Self {
            bits: vec![0; num_bits.div_ceil(8)],
            num_bits,
            num_hashes,
        }
    }

    pub fn insert(&mut self, key: &str) {
        let (h1, h2) = Self::hash_pair(key);

        for i in 0..self.num_hashes {
            let bit = h1
                .wrapping_add((i as u64).wrapping_mul(h2))
                % self.num_bits as u64;

            let bit = bit as usize;
            self.bits[bit / 8] |= 1 << (bit % 8);
        }
    }

    pub fn may_contain(&self, key: &str) -> bool {
        let (h1, h2) = Self::hash_pair(key);

        for i in 0..self.num_hashes {
            let bit = h1
                .wrapping_add((i as u64).wrapping_mul(h2))
                % self.num_bits as u64;

            let bit = bit as usize;

            if self.bits[bit / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }

        true
    }

    fn hash_pair(key: &str) -> (u64, u64) {
        let hash = xxh3_128(key.as_bytes());

        let h1 = hash as u64;
        let h2 = ((hash >> 64) as u64) | 1;

        (h1, h2)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HEADER_SIZE + self.bits.len());

        // Stable on-disk identifier: 1 = XXH3-128.
        bytes.push(HASH_ALGORITHM_XXH3_128);
        bytes.extend_from_slice(&(self.num_bits as u64).to_le_bytes());
        bytes.extend_from_slice(&(self.num_hashes as u64).to_le_bytes());
        bytes.extend_from_slice(&(self.bits.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&self.bits);

        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        use std::io::{Error, ErrorKind};

        let invalid_data = |message| Error::new(ErrorKind::InvalidData, message);

        if bytes.len() < HEADER_SIZE {
            return Err(invalid_data("truncated Bloom filter header"));
        }

        if bytes[0] != HASH_ALGORITHM_XXH3_128 {
            return Err(invalid_data("unsupported Bloom filter hash algorithm"));
        }

        let num_bits_u64 = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
        let num_hashes_u64 = u64::from_le_bytes(bytes[9..17].try_into().unwrap());
        let bits_len_u64 = u64::from_le_bytes(bytes[17..25].try_into().unwrap());

        let num_bits = usize::try_from(num_bits_u64)
            .map_err(|_| invalid_data("Bloom filter bit count is too large"))?;

        let num_hashes = usize::try_from(num_hashes_u64)
            .map_err(|_| invalid_data("Bloom filter hash count is too large"))?;

        let bits_len = usize::try_from(bits_len_u64)
            .map_err(|_| invalid_data("Bloom filter byte length is too large"))?;

        if num_bits == 0 || num_hashes == 0 {
            return Err(invalid_data("Bloom filter dimensions must be nonzero"));
        }

        // Prevent malformed metadata from triggering excessive CPU work.
        if num_hashes > 100 {
            return Err(invalid_data("Bloom filter hash count exceeds limit"));
        }

        if bits_len != num_bits.div_ceil(8) {
            return Err(invalid_data("Bloom filter bit and byte counts disagree"));
        }

        let expected_len = HEADER_SIZE
            .checked_add(bits_len)
            .ok_or_else(|| invalid_data("Bloom filter length overflow"))?;

        if bytes.len() != expected_len {
            return Err(invalid_data("Bloom filter payload length mismatch"));
        }

        Ok(Self {
            bits: bytes[HEADER_SIZE..].to_vec(),
            num_bits,
            num_hashes,
        })
    }
}

// pub fn build_bloom_filter(index: &index::Index) -> BloomFilter {
//     let mut filter = BloomFilter::new(index.len(), FALSE_POSITIVE_RATE);

//     for entry in index.iter() {
//         filter.insert(&entry.key);
//     }

//     filter
// }
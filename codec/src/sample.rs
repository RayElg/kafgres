//! Deterministic value generation for round-trip tests. A `Sample` impl only populates

use crate::primitives::Uuid;
use bytes::Bytes;

/// xorshift64*, seeded per test case.
pub struct Gen {
    state: u64,
    /// Depth budget: unbounded sampling on deeply nested schemas produces enormous values.
    pub budget: i32,
}

impl Gen {
    pub fn new(seed: u64) -> Self {
        Gen {
            state: seed | 1,
            budget: 24,
        }
    }

    fn next(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn bool(&mut self) -> bool {
        self.next() & 1 == 0
    }
    pub fn i8(&mut self) -> i8 {
        self.next() as i8
    }
    pub fn i16(&mut self) -> i16 {
        self.next() as i16
    }
    pub fn i32(&mut self) -> i32 {
        self.next() as i32
    }
    pub fn i64(&mut self) -> i64 {
        self.next() as i64
    }
    pub fn u16(&mut self) -> u16 {
        self.next() as u16
    }
    pub fn u32(&mut self) -> u32 {
        self.next() as u32
    }

    /// Finite only — NaN would break the `PartialEq` the round-trip test relies on.
    pub fn f64(&mut self) -> f64 {
        (self.next() % 100_000) as f64 / 1000.0
    }

    pub fn uuid(&mut self) -> Uuid {
        let mut u = [0u8; 16];
        u[..8].copy_from_slice(&self.next().to_be_bytes());
        u[8..].copy_from_slice(&self.next().to_be_bytes());
        Uuid(u)
    }

    /// Includes multi-byte UTF-8 so the length prefix is exercised in bytes, not chars.
    pub fn string(&mut self) -> String {
        const ALPHABET: &[&str] = &["a", "z", "0", "_", "-", ".", "é", "日", "🙂"];
        let n = (self.next() % 12) as usize;
        (0..n)
            .map(|_| ALPHABET[(self.next() % ALPHABET.len() as u64) as usize])
            .collect()
    }

    pub fn bytes(&mut self) -> Bytes {
        let n = (self.next() % 24) as usize;
        Bytes::from((0..n).map(|_| self.next() as u8).collect::<Vec<u8>>())
    }

    /// Array length; shrinks to zero as the depth budget runs out, bounding sample size.
    pub fn array_len(&mut self) -> usize {
        if self.budget <= 0 {
            return 0;
        }
        self.budget -= 4;
        (self.next() % 3) as usize
    }
}

pub trait Sample: Sized {
    fn sample(version: i16, g: &mut Gen) -> Self;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_is_deterministic() {
        let a: Vec<i32> = (0..8).map(|_| Gen::new(7).i32()).collect();
        assert!(a.windows(2).all(|w| w[0] == w[1]));
        let mut g1 = Gen::new(42);
        let mut g2 = Gen::new(42);
        for _ in 0..100 {
            assert_eq!(g1.i64(), g2.i64());
        }
    }

    #[test]
    fn sampled_floats_are_finite() {
        let mut g = Gen::new(1);
        for _ in 0..1000 {
            assert!(g.f64().is_finite());
        }
    }

    #[test]
    fn array_length_respects_budget() {
        let mut g = Gen::new(3);
        g.budget = 0;
        assert_eq!(g.array_len(), 0);
    }
}

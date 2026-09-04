//! Version-range algebra.

use std::fmt;

/// Inclusive range. Empty when `lo > hi`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Versions {
    pub lo: i16,
    pub hi: i16,
}

pub const NONE: Versions = Versions { lo: 0, hi: -1 };

impl Versions {
    pub const fn new(lo: i16, hi: i16) -> Self {
        Versions { lo, hi }
    }

    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        if s == "none" {
            return Ok(NONE);
        }
        if let Some(lo) = s.strip_suffix('+') {
            let lo = lo
                .trim()
                .parse::<i16>()
                .map_err(|_| format!("bad open range {s:?}"))?;
            return Ok(Versions::new(lo, i16::MAX));
        }
        if let Some((lo, hi)) = s.split_once('-') {
            let lo = lo
                .trim()
                .parse::<i16>()
                .map_err(|_| format!("bad range start in {s:?}"))?;
            let hi = hi
                .trim()
                .parse::<i16>()
                .map_err(|_| format!("bad range end in {s:?}"))?;
            if lo > hi {
                return Err(format!("inverted range {s:?}"));
            }
            return Ok(Versions::new(lo, hi));
        }
        let v = s.parse::<i16>().map_err(|_| format!("bad version {s:?}"))?;
        Ok(Versions::new(v, v))
    }

    pub fn is_empty(&self) -> bool {
        self.lo > self.hi
    }

    /// Test membership of a single version. The generator emits version conditions rather
    #[cfg(test)]
    pub fn contains(&self, v: i16) -> bool {
        !self.is_empty() && v >= self.lo && v <= self.hi
    }

    pub fn intersect(&self, other: Versions) -> Versions {
        if self.is_empty() || other.is_empty() {
            return NONE;
        }
        let r = Versions::new(self.lo.max(other.lo), self.hi.min(other.hi));
        if r.is_empty() {
            NONE
        } else {
            r
        }
    }

    /// Does this range cover all of `outer`?
    pub fn covers(&self, outer: Versions) -> bool {
        if outer.is_empty() {
            return true;
        }
        !self.is_empty() && self.lo <= outer.lo && self.hi >= outer.hi
    }

    /// A Rust boolean expression testing `var`, specialised to the case where the
    pub fn condition(&self, outer: Versions, var: &str) -> Option<String> {
        if self.is_empty() {
            return Some("false".to_string());
        }
        if self.covers(outer) {
            return None;
        }
        let lo_needed = self.lo > outer.lo;
        let hi_needed = self.hi < outer.hi;
        // No wrapping parens: callers only ever combine these with `&&`, which has the
        match (lo_needed, hi_needed) {
            (true, true) => Some(format!("{} >= {} && {} <= {}", var, self.lo, var, self.hi)),
            (true, false) => Some(format!("{} >= {}", var, self.lo)),
            (false, true) => Some(format!("{} <= {}", var, self.hi)),
            (false, false) => None,
        }
    }
}

impl fmt::Debug for Versions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            write!(f, "none")
        } else if self.hi == i16::MAX {
            write!(f, "{}+", self.lo)
        } else if self.lo == self.hi {
            write!(f, "{}", self.lo)
        } else {
            write!(f, "{}-{}", self.lo, self.hi)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_form() {
        assert!(Versions::parse("none").unwrap().is_empty());
        assert_eq!(Versions::parse("0+").unwrap(), Versions::new(0, i16::MAX));
        assert_eq!(Versions::parse("12+").unwrap(), Versions::new(12, i16::MAX));
        assert_eq!(Versions::parse("2-5").unwrap(), Versions::new(2, 5));
        assert_eq!(Versions::parse("3").unwrap(), Versions::new(3, 3));
    }

    #[test]
    fn rejects_garbage() {
        assert!(Versions::parse("5-2").is_err());
        assert!(Versions::parse("").is_err());
        assert!(Versions::parse("x+").is_err());
    }

    #[test]
    fn contains_respects_emptiness() {
        assert!(!NONE.contains(0));
        assert!(Versions::parse("2-5").unwrap().contains(2));
        assert!(Versions::parse("2-5").unwrap().contains(5));
        assert!(!Versions::parse("2-5").unwrap().contains(6));
    }

    #[test]
    fn intersect_and_covers() {
        let a = Versions::parse("0-10").unwrap();
        let b = Versions::parse("5+").unwrap();
        assert_eq!(a.intersect(b), Versions::new(5, 10));
        assert!(a.covers(Versions::new(1, 9)));
        assert!(!a.covers(Versions::new(1, 11)));
        assert_eq!(a.intersect(NONE), NONE);
    }

    #[test]
    fn condition_elides_when_trivially_true() {
        let valid = Versions::parse("3-13").unwrap();
        // "0+" covers all of 3-13, so no runtime test is needed.
        assert_eq!(
            Versions::parse("0+").unwrap().condition(valid, "version"),
            None
        );
        assert_eq!(
            Versions::parse("9+").unwrap().condition(valid, "version"),
            Some("version >= 9".to_string())
        );
        assert_eq!(
            Versions::parse("0-5").unwrap().condition(valid, "version"),
            Some("version <= 5".to_string())
        );
        assert_eq!(
            Versions::parse("5-9").unwrap().condition(valid, "version"),
            Some("version >= 5 && version <= 9".to_string())
        );
        assert_eq!(NONE.condition(valid, "version"), Some("false".to_string()));
    }
}

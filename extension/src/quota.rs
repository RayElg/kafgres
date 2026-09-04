//! Client quotas. Enforced: `producer_byte_rate` and `consumer_byte_rate`, per `user`

use std::collections::HashMap;

/// The window rates are measured over. Kafka averages eleven one-second samples; one
const WINDOW_MS: i64 = 1_000;

/// The largest delay to ask a client for, matching Kafka's `quota.window.num` *
const MAX_THROTTLE_MS: i32 = 11_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rate {
    Producer,
    Consumer,
}

impl Rate {
    pub fn key(self) -> &'static str {
        match self {
            Rate::Producer => "producer_byte_rate",
            Rate::Consumer => "consumer_byte_rate",
        }
    }
}

/// One entity's usage inside the current window.
#[derive(Default, Clone, Copy)]
struct Window {
    started_ms: i64,
    bytes: i64,
}

/// Per-broker rate state. A plain map on the worker: this broker is a singleton with one
#[derive(Default)]
pub struct Meter {
    windows: HashMap<(&'static str, String), Window>,
}

impl Meter {
    /// Record `bytes` against an entity and return how long the client should wait. The
    pub fn record(&mut self, rate: Rate, entity: &str, bytes: i64, quota: f64, now_ms: i64) -> i32 {
        if quota <= 0.0 || bytes <= 0 {
            return 0;
        }
        // Bounded before insertion: a client may send any `client.id` it likes on every
        const MAX_WINDOWS: usize = 10_000;
        let key = if self.windows.len() >= MAX_WINDOWS
            && !self.windows.contains_key(&(rate.key(), entity.to_string()))
        {
            (rate.key(), "<overflow>".to_string())
        } else {
            (rate.key(), entity.to_string())
        };
        let w = self
            .windows
            .entry(key)
            .or_insert(Window { started_ms: now_ms, bytes: 0 });
        if now_ms.saturating_sub(w.started_ms) >= WINDOW_MS {
            *w = Window { started_ms: now_ms, bytes: 0 };
        }
        w.bytes = w.bytes.saturating_add(bytes);

        let elapsed = (now_ms - w.started_ms).max(0) as f64;
        let owed_ms = (w.bytes as f64 / quota) * 1000.0 - elapsed;
        if owed_ms <= 0.0 {
            return 0;
        }
        // **Truncated, not rounded**: a generous quota still leaves a sub-millisecond debt,
        (owed_ms as i64).clamp(0, MAX_THROTTLE_MS as i64) as i32
    }

    /// Drop windows nothing has touched for a while, called from the tick: a client that
    pub fn expire(&mut self, now_ms: i64) {
        self.windows
            .retain(|_, w| now_ms.saturating_sub(w.started_ms) < 60_000);
    }
}

/// Every configured quota, cached: **the hot path does no SPI at all**. A per-request query
#[derive(Default)]
pub struct QuotaCache {
    rows: Vec<Row>,
    loaded: Option<std::time::Instant>,
}

struct Row {
    entity_type: String,
    /// `None` is the default for this entity type — Kafka's own encoding, kept rather than
    entity_name: Option<String>,
    quota_type: String,
    value: f64,
}

/// How stale a quota may be: the same second `AclCache` uses, since both are operator-speed
const MAX_STALENESS: std::time::Duration = std::time::Duration::from_secs(1);

impl QuotaCache {
    pub fn is_stale(&self) -> bool {
        match self.loaded {
            None => true,
            Some(at) => at.elapsed() > MAX_STALENESS,
        }
    }

    pub fn load() -> Result<Self, String> {
        let rows: Vec<Row> = pgrx::Spi::connect(|client| {
            let got = client.select(
                "SELECT entity_type, entity_name, quota_type, quota_value
                   FROM kafgres_client_quotas",
                None,
                &[],
            )?;
            let mut out = Vec::new();
            for r in got {
                out.push(Row {
                    entity_type: r.get::<String>(1)?.unwrap_or_default(),
                    entity_name: r.get::<String>(2)?,
                    quota_type: r.get::<String>(3)?.unwrap_or_default(),
                    value: r.get::<f64>(4)?.unwrap_or(0.0),
                });
            }
            Ok::<_, pgrx::spi::Error>(out)
        })
        .map_err(|e| e.to_string())?;
        Ok(QuotaCache { rows, loaded: Some(std::time::Instant::now()) })
    }

    /// Whether anything is configured at all. The common case is "no", and it is answered
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The quota that applies, and the entity to meter it against.
    pub fn applicable(
        &self,
        rate: Rate,
        principal: &str,
        client_id: &str,
    ) -> Option<(String, f64)> {
        if self.rows.is_empty() {
            return None;
        }
        let key = rate.key();
        let mut best: Option<(u8, &Row)> = None;
        for r in &self.rows {
            if r.quota_type != key {
                continue;
            }
            let rank = match (r.entity_type.as_str(), r.entity_name.as_deref()) {
                ("user", Some(n)) if n == principal => 0,
                ("user", None) => 1,
                ("client-id", Some(n)) if n == client_id => 2,
                ("client-id", None) => 3,
                _ => continue,
            };
            if best.map(|(b, _)| rank < b).unwrap_or(true) {
                best = Some((rank, r));
            }
        }
        let (rank, row) = best?;
        let who = match rank {
            0 | 1 => format!("user:{principal}"),
            _ => format!("client-id:{client_id}"),
        };
        Some((who, row.value))
    }
}

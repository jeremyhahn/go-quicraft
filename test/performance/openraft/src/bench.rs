use std::time::{Duration, Instant};

use crate::result::LatencyStats;

/// Records individual operation latency samples and computes statistics.
pub struct LatencyRecorder {
    samples: Vec<Duration>,
}

impl LatencyRecorder {
    pub fn new(cap: usize) -> Self {
        Self {
            samples: Vec::with_capacity(cap),
        }
    }

    pub fn record(&mut self, d: Duration) {
        self.samples.push(d);
    }

    pub fn count(&self) -> usize {
        self.samples.len()
    }

    /// Returns the duration at the given percentile in [0.0, 1.0].
    /// Sorts samples in place on first call per invocation.
    pub fn percentile(&mut self, p: f64) -> Duration {
        if self.samples.is_empty() {
            return Duration::ZERO;
        }
        self.samples.sort_unstable();

        if p <= 0.0 {
            return self.samples[0];
        }

        let len = self.samples.len();
        let rank = ((p * len as f64).ceil() as usize).max(1).min(len);
        self.samples[rank - 1]
    }

    pub fn p50(&mut self) -> Duration {
        self.percentile(0.50)
    }

    pub fn p90(&mut self) -> Duration {
        self.percentile(0.90)
    }

    pub fn p95(&mut self) -> Duration {
        self.percentile(0.95)
    }

    pub fn p99(&mut self) -> Duration {
        self.percentile(0.99)
    }

    pub fn p999(&mut self) -> Duration {
        self.percentile(0.999)
    }

    pub fn min(&mut self) -> Duration {
        self.percentile(0.0)
    }

    pub fn max(&mut self) -> Duration {
        self.percentile(1.0)
    }

    pub fn mean(&self) -> Duration {
        if self.samples.is_empty() {
            return Duration::ZERO;
        }
        let total: Duration = self.samples.iter().sum();
        total / self.samples.len() as u32
    }

    pub fn to_stats(&mut self) -> LatencyStats {
        LatencyStats {
            p50_ns: self.p50().as_nanos() as u64,
            p90_ns: self.p90().as_nanos() as u64,
            p95_ns: self.p95().as_nanos() as u64,
            p99_ns: self.p99().as_nanos() as u64,
            p999_ns: self.p999().as_nanos() as u64,
            min_ns: self.min().as_nanos() as u64,
            max_ns: self.max().as_nanos() as u64,
            mean_ns: self.mean().as_nanos() as u64,
            count: self.count() as u64,
        }
    }
}

/// A thin wrapper that pairs a `LatencyRecorder` with wall-clock timing so
/// callers can time a closure and collect the sample in one step.
pub struct Timer {
    start: Instant,
}

impl Timer {
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

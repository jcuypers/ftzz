use rand::Rng;
use rand_distr::{Distribution, Normal};
pub use scheduler::{GeneratorStats, run};
pub use tasks::{DynamicGenerator, GeneratorBytes, StaticGenerator};

#[derive(Debug, Clone, Copy)]
pub struct FileSpec {
    pub seed: u64,
    pub is_duplicate: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct PendingDuplicate {
    pub spec: FileSpec,
    pub size: Option<u64>,
}

pub mod audit;
mod file_contents;
mod files;
mod scheduler;
mod tasks;

#[cfg_attr(feature = "tracing", tracing::instrument(level = "trace"))]
pub fn truncatable_normal(mean: f64) -> Normal<f64> {
    let mean = mean + 0.5;
    Normal::new(mean, mean / 3.).unwrap()
}

// TODO https://github.com/rust-random/rand/issues/1189
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
#[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip(rng)))]
fn sample_truncated<R: Rng + ?Sized>(normal: &Normal<f64>, rng: &mut R) -> u64 {
    let max = normal.mean() * 2.;
    for _ in 0..5 {
        let x = normal.sample(rng);
        if 0. <= x && x < max {
            return x as u64;
        }
    }
    normal.mean() as u64
}

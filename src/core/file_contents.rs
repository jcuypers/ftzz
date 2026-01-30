#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{fs, fs::File, io, io::Read};

use cfg_if::cfg_if;
use rand::{RngCore, SeedableRng, TryRngCore};
use rand_distr::Normal;
use rand_xoshiro::Xoshiro256PlusPlus;
#[cfg(target_os = "linux")]
use rustix::fs::{FileType, Mode, mknodat};
#[cfg(all(unix, not(target_os = "linux")))]
use rustix::fs::{Mode, OFlags, openat};

use crate::{
    core::{FileSpec, sample_truncated},
    utils::FastPathBuf,
};

pub trait FileContentsGenerator {
    type State;

    fn initialize(&self) -> Self::State;

    fn create_file(
        &mut self,
        file: &mut FastPathBuf,
        file_num: usize,
        retryable: bool,
        state: &mut Self::State,
        hash_seed: Option<u64>,
        spec: &FileSpec,
    ) -> io::Result<(u64, Option<u64>)>;

    fn byte_counts_pool_return(self) -> Option<Vec<u64>>;
}

pub struct NoGeneratedFileContents;

impl FileContentsGenerator for NoGeneratedFileContents {
    type State = ();

    fn initialize(&self) -> Self::State {}

    #[inline]
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip(self)))]
    fn create_file(
        &mut self,
        file: &mut FastPathBuf,
        _: usize,
        _: bool,
        (): &mut Self::State,
        _: Option<u64>,
        spec: &FileSpec,
    ) -> io::Result<(u64, Option<u64>)> {
        let mode = spec.permission.unwrap_or(0o664);
        cfg_if! {
            if #[cfg(any(not(unix), miri))] {
                File::create(file).and_then(|f| {
                    if let Some(p) = spec.permission {
                        f.set_permissions(fs::Permissions::from_mode(p))?;
                    }
                    Ok((0, None))
                })
            } else if #[cfg(target_os = "linux")] {
                let cstr = file.to_cstr_mut();
                mknodat(
                    rustix::fs::CWD,
                    &*cstr,
                    FileType::RegularFile,
                    Mode::from_bits_retain(mode),
                    0,
                )
                .map_err(io::Error::from)
                .map(|()| (0, None))
            } else {
                let cstr = file.to_cstr_mut();
                openat(
                    rustix::fs::CWD,
                    &*cstr,
                    OFlags::CREATE,
                    Mode::from_bits_retain(mode),
                )
                .map_err(io::Error::from)
                .map(|_| (0, None))
            }
        }
    }

    fn byte_counts_pool_return(self) -> Option<Vec<u64>> {
        None
    }
}

#[derive(Debug)]
pub struct OnTheFlyGeneratedFileContents {
    pub num_bytes_distr: Normal<f64>,
    pub seed: u64,
    pub fill_byte: Option<u8>,
}

impl FileContentsGenerator for OnTheFlyGeneratedFileContents {
    type State = Xoshiro256PlusPlus;

    fn initialize(&self) -> Self::State {
        let Self { seed, .. } = *self;

        Xoshiro256PlusPlus::seed_from_u64(seed)
    }

    #[inline]
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace"))]
    fn create_file(
        &mut self,
        file: &mut FastPathBuf,
        file_num: usize,
        retryable: bool,
        _random: &mut Self::State,
        hash_seed: Option<u64>,
        spec: &FileSpec,
    ) -> io::Result<(u64, Option<u64>)> {
        let Self {
            ref num_bytes_distr,
            seed: _,
            fill_byte,
        } = *self;

        // Use the seed from the spec for content generation if applicable.
        // But `random` here is the state of the generator.
        // Wait, if we want deterministic content per file based on seed, we should NOT
        // use `random` (which is shared state). OR we should re-seed `random`
        // or use a new RNG based on `spec.seed`.

        // Since `queue_gen` will pass unique seeds for each file, we can use
        // `spec.seed`.

        let mut file_rnd = Xoshiro256PlusPlus::seed_from_u64(spec.seed);
        // We use local RNG for this file.
        // But `create_file` signature takes `random: &mut Self::State`.
        // We should ignore `random` for content generation if we use `spec.seed`.
        // However, `random` might be used for other things? No, it's
        // `OnTheFlyGeneratedFileContents`. The `random` passed in is the
        // generator's state. If we want deterministic per file, we should use
        // `spec.seed`.

        let num_bytes = sample_truncated(num_bytes_distr, &mut file_rnd);
        if num_bytes > 0 || retryable {
            File::create(&*file).and_then(|f| {
                let hash = write_bytes(f, num_bytes, (fill_byte, &mut file_rnd), hash_seed)?;
                #[cfg(unix)]
                if let Some(p) = spec.permission {
                    fs::set_permissions(file, fs::Permissions::from_mode(p))?;
                }
                Ok((num_bytes, hash))
            })
        } else {
            NoGeneratedFileContents.create_file(file, file_num, retryable, &mut (), hash_seed, spec)
        }
    }

    fn byte_counts_pool_return(self) -> Option<Vec<u64>> {
        None
    }
}

#[derive(Debug)]
pub struct PreDefinedGeneratedFileContents {
    pub byte_counts: Vec<u64>,
    pub seed: u64,
    pub fill_byte: Option<u8>,
}

impl FileContentsGenerator for PreDefinedGeneratedFileContents {
    type State = Xoshiro256PlusPlus;

    fn initialize(&self) -> Self::State {
        let Self { seed, .. } = *self;

        Xoshiro256PlusPlus::seed_from_u64(seed)
    }

    #[inline]
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace"))]
    fn create_file(
        &mut self,
        file: &mut FastPathBuf,
        file_num: usize,
        retryable: bool,
        _random: &mut Self::State,
        hash_seed: Option<u64>,
        spec: &FileSpec,
    ) -> io::Result<(u64, Option<u64>)> {
        let Self {
            ref byte_counts,
            seed: _,
            fill_byte,
        } = *self;

        // For PreDefined, we use the byte counts.
        // But for content generation (if random bytes), we should use `spec.seed`?
        // PreDefinedGeneratedFileContents usually has a shared RNG state.
        // If we want duplicate files to be identical, we must use `spec.seed`.

        let mut file_rnd = Xoshiro256PlusPlus::seed_from_u64(spec.seed);

        let num_bytes = byte_counts[file_num];
        if num_bytes > 0 {
            File::create(&*file)
                .and_then(|f| {
                    let hash = write_bytes(f, num_bytes, (fill_byte, &mut file_rnd), hash_seed)?;
                    #[cfg(unix)]
                    if let Some(p) = spec.permission {
                        fs::set_permissions(file, fs::Permissions::from_mode(p))?;
                    }
                    Ok(hash)
                })
                .map(|hash| (num_bytes, hash))
        } else {
            NoGeneratedFileContents.create_file(file, file_num, retryable, &mut (), hash_seed, spec)
        }
    }

    fn byte_counts_pool_return(self) -> Option<Vec<u64>> {
        Some(self.byte_counts)
    }
}

enum BytesKind<'a, R> {
    Random(&'a mut R),
    Fixed(u8),
}

impl<'a, R> From<(Option<u8>, &'a mut R)> for BytesKind<'a, R> {
    fn from((fill_byte, random): (Option<u8>, &'a mut R)) -> Self {
        fill_byte.map_or(BytesKind::Random(random), |byte| BytesKind::Fixed(byte))
    }
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(level = "trace", skip(file, kind))
)]
fn write_bytes<'a, R: RngCore + 'static>(
    file: File,
    num: u64,
    kind: impl Into<BytesKind<'a, R>>,
    hash_seed: Option<u64>,
) -> io::Result<Option<u64>> {
    use crate::core::audit::HashingWriter;

    if let Some(seed) = hash_seed {
        let mut writer = HashingWriter::new(file, seed);
        let copied = match kind.into() {
            BytesKind::Random(random) => {
                io::copy(&mut random.read_adapter().take(num), &mut writer)
            }
            BytesKind::Fixed(byte) => io::copy(&mut io::repeat(byte).take(num), &mut writer),
        }?;
        debug_assert_eq!(num, copied);
        Ok(Some(writer.finalize()))
    } else {
        let mut file = file;
        let copied = match kind.into() {
            BytesKind::Random(random) => io::copy(&mut random.read_adapter().take(num), &mut file),
            BytesKind::Fixed(byte) => io::copy(&mut io::repeat(byte).take(num), &mut file),
        }?;
        debug_assert_eq!(num, copied);
        Ok(None)
    }
}

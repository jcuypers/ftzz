#![allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]

use std::{cmp::min, io, num::NonZeroU64, sync::Arc};

use rand::RngCore;
use rand_distr::Normal;
use tokio::{task, task::JoinHandle};

use crate::{
    core::{
        FileSpec, PendingDuplicate,
        audit::AuditTrail,
        file_contents::{
            FileContentsGenerator, NoGeneratedFileContents, OnTheFlyGeneratedFileContents,
            PreDefinedGeneratedFileContents,
        },
        files::{GeneratorTaskOutcome, GeneratorTaskParams, create_files_and_dirs},
        sample_truncated,
    },
    utils::FastPathBuf,
};

pub type QueueResult = Result<QueueOutcome, QueueErrors>;

pub struct QueueOutcome {
    #[cfg(not(feature = "dry_run"))]
    pub task: JoinHandle<error_stack::Result<GeneratorTaskOutcome, io::Error>>,
    #[cfg(feature = "dry_run")]
    pub task: GeneratorTaskOutcome,

    pub num_files: u64,
    pub num_dirs: usize,
    pub done: bool,
}

#[derive(Debug)]
pub enum QueueErrors {
    NothingToDo(FastPathBuf),
}

pub trait TaskGenerator {
    fn queue_gen(
        &mut self,
        num_files_distr: &Normal<f64>,
        file: FastPathBuf,
        gen_dirs: bool,
        byte_counts_pool: &mut Vec<Vec<u64>>,
    ) -> QueueResult;

    fn maybe_queue_final_gen(&mut self, file: FastPathBuf, _: &mut Vec<Vec<u64>>) -> QueueResult {
        Err(QueueErrors::NothingToDo(file))
    }

    fn uses_byte_counts_pool(&self) -> bool {
        false
    }
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(level = "trace", skip(params))
)]
fn queue(
    params: GeneratorTaskParams<impl FileContentsGenerator + Send + 'static>,
    done: bool,
) -> QueueResult {
    if !params.file_objs.is_empty() || params.num_dirs > 0 {
        Ok(QueueOutcome {
            num_files: params.file_objs.len() as u64,
            num_dirs: params.num_dirs,
            done,

            #[cfg(not(feature = "dry_run"))]
            task: task::spawn_blocking(move || create_files_and_dirs(params)),
            #[cfg(feature = "dry_run")]
            task: {
                std::hint::black_box(&params);
                GeneratorTaskOutcome {
                    files_generated: params.file_objs.len() as u64,
                    dirs_generated: params.num_dirs,
                    bytes_generated: 0,

                    pool_return_file: params.target_dir,
                    pool_return_byte_counts: None,
                }
            },
        })
    } else {
        Err(QueueErrors::NothingToDo(params.target_dir))
    }
}

fn dirs_to_gen<R: RngCore + ?Sized>(
    files_created: u64,
    gen_dirs: bool,
    num_dirs_distr: &Normal<f64>,
    random: &mut R,
) -> usize {
    if gen_dirs {
        let dirs = usize::try_from(sample_truncated(num_dirs_distr, random)).unwrap_or(usize::MAX);
        if files_created > 0 && dirs == 0 {
            1
        } else {
            dirs
        }
    } else {
        0
    }
}

pub struct GeneratorBytes {
    pub num_bytes_distr: Normal<f64>,
    pub fill_byte: Option<u8>,
}

pub struct DynamicGenerator<R> {
    pub num_dirs_distr: Normal<f64>,
    pub random: R,

    pub bytes: Option<GeneratorBytes>,
    pub duplicate_percentage: f64,
    pub max_duplicates_per_file: std::num::NonZeroUsize,
    pub pending_duplicates: Vec<PendingDuplicate>,
    pub audit_trail: Option<Arc<AuditTrail>>,
    pub permissions: Vec<u32>,
}

fn generate_primary_specs(
    num_files: u64,
    rng: &mut impl RngCore,
    permissions: &[u32],
) -> Vec<FileSpec> {
    let mut specs = Vec::with_capacity(num_files as usize);
    for _ in 0..num_files {
        let seed = rng.next_u64();
        specs.push(FileSpec {
            seed,
            is_duplicate: false,
            permission: if permissions.is_empty() {
                None
            } else {
                Some(permissions[(seed % permissions.len() as u64) as usize])
            },
        });
    }
    specs
}

fn add_duplicates_to_specs_and_buffer(
    specs: &mut Vec<FileSpec>,
    mut file_sizes: Option<&mut Vec<u64>>,
    pending_buffer: &mut Vec<PendingDuplicate>,
    duplicate_percentage: f64,
    max_duplicates_per_file: std::num::NonZeroUsize,
    rng: &mut impl RngCore,
    permissions: &[u32],
) {
    let num_files = specs.len() as u64;
    if num_files == 0 || duplicate_percentage <= 0.0 {
        return;
    }

    // Calculate probability that a file has duplicates
    // Total Duplicates Wanted = num_files * rate
    // Expected Duplicates per File (given it has duplicates) = (min + max) / 2
    // Min extra copies = 1 (since max >= 2)
    // Max extra copies = max_duplicates_per_file - 1
    // Average extra = (1 + (max - 1)) / 2 = max / 2

    let max_dups = max_duplicates_per_file.get();
    if max_dups < 2 {
        return;
    }

    let avg_extra = max_dups as f64 / 2.0;
    let prob = (duplicate_percentage / 100.0) / avg_extra;
    // Probability cannot be > 1.0 (if percentage is high and max dups is low)
    // If percentage is 200%, and max dups is 2 (avg 1), prob = 2.0.
    // In that case we need multiple "duplication events" or ensure every file is
    // duplicated. Simpler: iterate primary files, check prob.

    for i in 0..(num_files as usize) {
        // If file_sizes provided, check size > 0.
        // If size is 0, we skip duplicating this file.
        if let Some(sizes) = &file_sizes
            && sizes[i] == 0
        {
            continue;
        }

        let original_seed = specs[i].seed;

        let threshold = (prob * (u64::MAX as f64)) as u64;
        if rng.next_u64() < threshold {
            // Generate duplicates
            let max_extra = max_dups - 1;
            let copies = if max_extra > 1 {
                (rng.next_u64() % (max_extra as u64)) + 1
            } else {
                1
            };

            for _ in 0..copies {
                let spec = FileSpec {
                    seed: original_seed,
                    is_duplicate: true,
                    permission: if permissions.is_empty() {
                        None
                    } else {
                        Some(permissions[(original_seed % permissions.len() as u64) as usize])
                    },
                };

                // Hybrid approach: 50% chance to scatter, 50% chance to keep local
                // Unless we have 0 bytes logic (handled by caller passing None sizes for 0
                // byte), but checking size here just in case sizes is Some

                let size_val = file_sizes.as_ref().map(|sizes| sizes[i]);

                // Determine if we scatter or keep local
                // If scatter, push to pending buffer
                // If local, push to specs (and sizes)
                if rng.next_u32().is_multiple_of(2) {
                    // Scatter
                    pending_buffer.push(PendingDuplicate {
                        spec,
                        size: size_val,
                    });
                } else {
                    // Keep local
                    specs.push(spec);
                    if let Some(sizes) = &mut file_sizes {
                        // size_val is Some(sizes[i])
                        if let Some(s) = size_val {
                            sizes.push(s);
                        }
                    }
                }
            }
        }
    }
}

impl<R: RngCore + Clone + Send + 'static> TaskGenerator for DynamicGenerator<R> {
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip(self)))]
    fn queue_gen(
        &mut self,
        num_files_distr: &Normal<f64>,
        file: FastPathBuf,
        gen_dirs: bool,
        _: &mut Vec<Vec<u64>>,
    ) -> QueueResult {
        let Self {
            ref num_dirs_distr,
            ref mut random,
            ref bytes,
            duplicate_percentage,
            max_duplicates_per_file,
            ref audit_trail,
            ref mut pending_duplicates,
            ref permissions,
        } = *self;

        let num_files = sample_truncated(num_files_distr, random);
        let num_dirs = dirs_to_gen(num_files, gen_dirs, num_dirs_distr, random);

        let mut file_specs = generate_primary_specs(num_files, random, permissions);

        // Use a separate RNG for duplicates to avoid affecting the primary structure
        // sequence
        let mut dup_rng = random.clone();

        if bytes.is_some() {
            add_duplicates_to_specs_and_buffer(
                &mut file_specs,
                None, // Dynamic generator doesn't track per-file sizes here
                pending_duplicates,
                duplicate_percentage,
                max_duplicates_per_file,
                &mut dup_rng,
                permissions,
            );

            // Inject pending (LIFO for efficiency)
            let limit = (num_files / 2).max(1) as usize;
            let drain_count = min(pending_duplicates.len(), limit);
            for _ in 0..drain_count {
                // LIFO pop
                if let Some(dup) = pending_duplicates.pop() {
                    file_specs.push(dup.spec);
                }
            }
        }

        macro_rules! build_params {
            ($file_specs:expr, $file_contents:expr, $audit_trail:expr) => {{
                GeneratorTaskParams {
                    target_dir: file,
                    file_objs: $file_specs,
                    num_dirs,
                    file_offset: 0,
                    file_contents: $file_contents,
                    audit_trail: $audit_trail.clone(),
                }
            }};
        }

        if let Some(GeneratorBytes {
            num_bytes_distr,
            fill_byte,
        }) = *bytes
        {
            queue(
                build_params!(
                    file_specs,
                    OnTheFlyGeneratedFileContents {
                        num_bytes_distr,
                        seed: random.next_u64(),
                        fill_byte,
                    },
                    audit_trail
                ),
                false,
            )
        } else {
            queue(
                build_params!(file_specs, NoGeneratedFileContents, audit_trail),
                false,
            )
        }
    }

    fn maybe_queue_final_gen(&mut self, file: FastPathBuf, _: &mut Vec<Vec<u64>>) -> QueueResult {
        let Self {
            ref pending_duplicates,
            audit_trail: _,
            ..
        } = *self;

        if pending_duplicates.is_empty() {
            return Err(QueueErrors::NothingToDo(file));
        }

        // Flush all pending
        let mut file_specs = Vec::with_capacity(pending_duplicates.len());
        // We need to drain from self.pending_duplicates, but we only have ref to self?
        // No, signature is &mut self.
        // But we destructured `ref pending_duplicates`.
        // Let's re-destructure or use self directly.

        let Self {
            ref mut pending_duplicates,
            ref mut random,
            ref bytes,
            ref audit_trail,
            ..
        } = *self;

        while let Some(dup) = pending_duplicates.pop() {
            file_specs.push(dup.spec);
        }

        if file_specs.is_empty() {
            return Err(QueueErrors::NothingToDo(file));
        }

        macro_rules! build_params {
            ($file_specs:expr, $file_contents:expr, $audit_trail:expr) => {{
                GeneratorTaskParams {
                    target_dir: file,
                    file_objs: $file_specs,
                    num_dirs: 0,
                    file_offset: 0,
                    file_contents: $file_contents,
                    audit_trail: $audit_trail.clone(),
                }
            }};
        }

        if let Some(GeneratorBytes {
            num_bytes_distr,
            fill_byte,
        }) = *bytes
        {
            queue(
                build_params!(
                    file_specs,
                    OnTheFlyGeneratedFileContents {
                        num_bytes_distr,
                        seed: random.next_u64(),
                        fill_byte,
                    },
                    audit_trail
                ),
                true, // done
            )
        } else {
            queue(
                build_params!(file_specs, NoGeneratedFileContents, audit_trail),
                true,
            )
        }
    }
}

pub struct StaticGenerator<R> {
    pub random: R,
    pub files_exact: Option<u64>,
    pub bytes_exact: Option<u64>,
    pub duplicate_percentage: f64,
    pub max_duplicates_per_file: std::num::NonZeroUsize,
    pub audit_trail: Option<Arc<AuditTrail>>,
    pub done: bool,
    pub root_num_files_hack: Option<u64>,

    // We keep DynamicGenerator's distributions for directory generation and probabilistic file
    // sizes
    pub num_dirs_distr: Normal<f64>,
    pub bytes: Option<GeneratorBytes>,
    pub pending_duplicates: Vec<PendingDuplicate>,
    pub permissions: Vec<u32>,
}

impl<R: RngCore + Clone + Send + 'static> StaticGenerator<R> {
    pub fn new(
        dynamic: DynamicGenerator<R>,
        files_exact: Option<NonZeroU64>,
        bytes_exact: Option<NonZeroU64>,
    ) -> Self {
        let DynamicGenerator {
            num_dirs_distr,
            random,
            bytes,
            duplicate_percentage,
            max_duplicates_per_file,
            audit_trail,
            pending_duplicates,
            permissions,
        } = dynamic;
        debug_assert!(files_exact.is_some() || bytes_exact.is_some());
        Self {
            random,
            files_exact: files_exact.map(NonZeroU64::get),
            bytes_exact: bytes_exact.map(NonZeroU64::get),
            duplicate_percentage,
            max_duplicates_per_file,
            audit_trail,
            done: false,
            root_num_files_hack: None,
            num_dirs_distr,
            bytes,
            pending_duplicates,
            permissions,
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "trace", skip(self, byte_counts_pool))
    )]
    fn queue_gen_internal(
        &mut self,
        file: FastPathBuf,
        num_files: u64,
        num_dirs: usize,
        offset: u64,
        byte_counts_pool: &mut Vec<Vec<u64>>,
    ) -> QueueResult {
        macro_rules! build_params {
            ($file_specs:expr, $file_contents:expr, $audit_trail:expr) => {{
                GeneratorTaskParams {
                    target_dir: file,
                    file_objs: $file_specs,
                    num_dirs,
                    file_offset: offset,
                    file_contents: $file_contents,
                    audit_trail: $audit_trail.clone(),
                }
            }};
        }

        let Self {
            ref mut random,
            files_exact: _,
            ref mut bytes_exact,
            duplicate_percentage,
            max_duplicates_per_file,
            ref audit_trail,
            done,
            root_num_files_hack: _,
            num_dirs_distr: _,
            bytes: ref bytes_opt,
            ref mut pending_duplicates,
            ref permissions,
        } = *self;

        let mut file_specs = generate_primary_specs(num_files, random, permissions);
        let mut dup_rng = random.clone();

        if let Some(GeneratorBytes {
            num_bytes_distr,
            fill_byte,
        }) = *bytes_opt
        {
            // We have bytes config. We might have duplicates.

            if let Some(bytes) = bytes_exact {
                // Exact bytes mode: we need to track byte counts for every file (primary + dup)
                let mut byte_counts: Vec<u64> = byte_counts_pool.pop().unwrap_or_default();
                debug_assert!(byte_counts.is_empty());

                // 1. Handle Primary Files
                if num_files > 0 && *bytes > 0 {
                    let num_files_usize = num_files.try_into().unwrap_or(usize::MAX);
                    byte_counts.reserve(num_files_usize);
                    let raw_byte_counts = byte_counts
                        .spare_capacity_mut()
                        .split_at_mut(num_files_usize)
                        .0;

                    for count in raw_byte_counts {
                        let num_bytes = min(*bytes, sample_truncated(&num_bytes_distr, random));
                        *bytes -= num_bytes;
                        count.write(num_bytes);
                    }

                    unsafe {
                        byte_counts.set_len(num_files_usize);
                    }

                    if done {
                        // Spread leftovers if done
                        let base = *bytes / num_files;
                        let mut leftovers = *bytes % num_files;
                        for count in &mut byte_counts {
                            if leftovers > 0 {
                                *count += base + 1;
                                leftovers -= 1;
                            } else {
                                *count += base;
                            }
                        }
                    }
                }

                // 2. Generate NEW duplicates (if any primary files)
                if num_files > 0 && *bytes > 0 {
                    add_duplicates_to_specs_and_buffer(
                        &mut file_specs,
                        Some(&mut byte_counts),
                        pending_duplicates,
                        duplicate_percentage,
                        max_duplicates_per_file,
                        &mut dup_rng,
                        permissions,
                    );
                }

                // 3. Inject PENDING duplicates
                let limit = if done {
                    pending_duplicates.len()
                } else {
                    (num_files / 2).max(1) as usize
                };
                let drain_count = min(pending_duplicates.len(), limit);

                for _ in 0..drain_count {
                    if let Some(dup) = pending_duplicates.pop() {
                        file_specs.push(dup.spec);
                        // For bytes_exact, dup must have a size.
                        // If it doesn't (maybe generated when bytes_exact was temporarily off?
                        // Impossible in this logic), we should probably
                        // give it 0 or log error. But here it should have size.
                        if let Some(s) = dup.size {
                            byte_counts.push(s);
                        } else {
                            byte_counts.push(0); // Should not happen if logic is consistent
                        }
                    }
                }

                // 4. Queue PreDefined
                // If bytes_exact is 0, we treat as NoGeneratedFileContents?
                // Original logic: if *bytes > 0 { ... } else { NoGenerated ... }
                // But now we might have duplicates even if *bytes == 0 (remaining global
                // bytes), if they were buffered? Actually, if *bytes == 0, we
                // stop generating content for primary files? If *bytes > 0 is
                // false, we technically shouldn't generate more bytes.
                // But duplicates *have* their size pre-calculated. They don't consume *new*
                // from `bytes`. So if we have duplicates in buffer, we should
                // output them.

                if !byte_counts.is_empty() || (num_files > 0 && *bytes > 0) {
                    // Condition to use PreDefined
                    queue(
                        build_params!(
                            file_specs,
                            PreDefinedGeneratedFileContents {
                                byte_counts,
                                seed: random.next_u64(),
                                fill_byte,
                            },
                            audit_trail
                        ),
                        done,
                    )
                } else {
                    // Recycled byte_counts since unused
                    byte_counts.clear();
                    byte_counts_pool.push(byte_counts);
                    queue(
                        build_params!(file_specs, NoGeneratedFileContents, audit_trail),
                        done,
                    )
                }
            } else {
                // OnTheFly Mode (No bytes_exact tracking)

                // 2. Generate NEW duplicates
                if num_files > 0 {
                    add_duplicates_to_specs_and_buffer(
                        &mut file_specs,
                        None,
                        pending_duplicates,
                        duplicate_percentage,
                        max_duplicates_per_file,
                        &mut dup_rng,
                        permissions,
                    );
                }

                // 3. Inject PENDING duplicates
                let limit = if done {
                    pending_duplicates.len()
                } else {
                    (num_files / 2).max(1) as usize
                };
                let drain_count = min(pending_duplicates.len(), limit);
                for _ in 0..drain_count {
                    if let Some(dup) = pending_duplicates.pop() {
                        file_specs.push(dup.spec);
                    }
                }

                queue(
                    build_params!(
                        file_specs,
                        OnTheFlyGeneratedFileContents {
                            num_bytes_distr,
                            seed: random.next_u64(),
                            fill_byte,
                        },
                        audit_trail
                    ),
                    done,
                )
            }
        } else {
            // No bytes configured (0-byte files inferred), so no duplicates logic needed
            queue(
                build_params!(file_specs, NoGeneratedFileContents, audit_trail),
                done,
            )
        }
    }
}

impl<R: RngCore + Clone + Send + 'static> TaskGenerator for StaticGenerator<R> {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "trace", skip(self, byte_counts_pool))
    )]
    fn queue_gen(
        &mut self,
        num_files_distr: &Normal<f64>,
        file: FastPathBuf,
        gen_dirs: bool,
        byte_counts_pool: &mut Vec<Vec<u64>>,
    ) -> QueueResult {
        let Self {
            ref mut random,
            ref mut files_exact,
            bytes_exact: _,
            duplicate_percentage: _,
            max_duplicates_per_file: _,
            audit_trail: _,
            ref mut done,
            ref mut root_num_files_hack,
            ref num_dirs_distr,
            bytes: _,
            pending_duplicates: _,
            permissions: _,
        } = *self;

        debug_assert!(!*done);

        let mut num_files = sample_truncated(num_files_distr, random);
        if let Some(files) = files_exact {
            if num_files >= *files {
                *done = true;
                num_files = *files;
            } else {
                *files -= num_files;
            }
        }

        if root_num_files_hack.is_none() {
            *root_num_files_hack = Some(num_files);
        }

        let num_dirs = if *done {
            0
        } else {
            dirs_to_gen(num_files, gen_dirs, num_dirs_distr, random)
        };
        self.queue_gen_internal(file, num_files, num_dirs, 0, byte_counts_pool)
    }

    fn maybe_queue_final_gen(
        &mut self,
        file: FastPathBuf,
        byte_counts_pool: &mut Vec<Vec<u64>>,
    ) -> QueueResult {
        if self.done {
            return Err(QueueErrors::NothingToDo(file));
        }
        self.done = true;

        // We set done = true, so calling queue_gen_internal below will trigger the
        // flush logic because we passed `done` (which is now true) to it?
        // Wait, `queue_gen_internal` reads `done` from `self.done`.
        // We modified `self.done` above.
        // So yes, it will see true.

        if let Some(files) = self.files_exact {
            self.queue_gen_internal(
                file,
                files,
                0,
                self.root_num_files_hack.unwrap_or(0),
                byte_counts_pool,
            )
        } else if matches!(self.bytes_exact, Some(b) if b > 0) {
            self.queue_gen_internal(
                file,
                1,
                0,
                self.root_num_files_hack.unwrap_or(0),
                byte_counts_pool,
            )
        } else {
            Err(QueueErrors::NothingToDo(file))
        }
    }

    fn uses_byte_counts_pool(&self) -> bool {
        let Self { bytes_exact, .. } = *self;

        matches!(bytes_exact, Some(b) if b > 0)
    }
}

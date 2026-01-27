use std::{
    borrow::Cow,
    io,
    io::{Write, stdout},
    num::NonZeroU64,
    path::PathBuf,
    process::{ExitCode, Termination},
};

use clap::{ArgAction, Args, Parser, ValueHint};
use clap_num::si_number;
use clap_verbosity_flag::Verbosity;
use error_stack::ResultExt;
use ftzz::{Generator, NumFilesWithRatio, NumFilesWithRatioError};
use io_adapters::WriteExtension;

mod config;

use crate::config::Config;

#[cfg(not(feature = "trace"))]
type DefaultLevel = clap_verbosity_flag::WarnLevel;
#[cfg(feature = "trace")]
type DefaultLevel = clap_verbosity_flag::TraceLevel;

/// Generate a random directory hierarchy with some number of files
///
/// A pseudo-random directory hierarchy will be generated (seeded by this
/// command's input parameters) containing approximately the target
/// number of files. The exact configuration of files and directories in
/// the hierarchy is probabilistically determined to mostly match the
/// specified parameters.
///
/// Generated files and directories are named using monotonically increasing
/// numbers, where files are named `n` and directories are named `n.dir`
/// for a given natural number `n`.
///
/// By default, generated files are empty, but random data can be used as
/// the file contents with the `total-bytes` option.
#[derive(Parser, Debug)]
#[command(version, author = "Alex Saveau (@SUPERCILEX)")]
#[command(infer_subcommands = true, infer_long_args = true)]
#[command(disable_help_flag = true)]
#[command(max_term_width = 100)]
#[cfg_attr(test, command(help_expected = true))]
struct Ftzz {
    #[command(flatten)]
    options: Generate,

    #[command(flatten)]
    #[command(next_display_order = None)]
    verbose: Verbosity<DefaultLevel>,

    /// Path to a TOML configuration file
    #[arg(long = "config", value_hint = ValueHint::FilePath, global = true)]
    config_file: Option<PathBuf>,

    #[arg(short, long, short_alias = '?', global = true)]
    #[arg(action = ArgAction::Help, help = "Print help (use `--help` for more detail)")]
    #[arg(long_help = "Print help (use `-h` for a summary)")]
    help: Option<bool>,
}

#[derive(Args, Debug)]
#[command(arg_required_else_help = true)]
struct Generate {
    /// The directory in which to generate files
    ///
    /// The directory will be created if it does not exist.
    #[arg(value_hint = ValueHint::DirPath)]
    root_dir: PathBuf,

    /// The number of files to generate
    ///
    /// Note: this value is probabilistically respected, meaning any number of
    /// files may be generated so long as we attempt to get close to N.
    #[arg(short = 'n', long = "files", alias = "num-files")]
    #[arg(value_parser = num_files_parser)]
    num_files: Option<NonZeroU64>,

    /// Whether or not to generate exactly N files
    #[arg(long = "files-exact", action = ArgAction::SetTrue)]
    files_exact: bool,

    /// The total amount of random data to be distributed across the generated
    /// files
    ///
    /// Note: this value is probabilistically respected, meaning any amount of
    /// data may be generated so long as we attempt to get close to N.
    #[arg(short = 'b', long = "total-bytes", aliases = & ["num-bytes", "num-total-bytes"])]
    #[arg(group = "num-bytes")]
    #[arg(value_parser = si_number::<u64>)]
    #[arg(help = "The total amount of random data [default: 0]")]
    num_bytes: Option<u64>,

    /// Specify a specific fill byte to be used instead of deterministically
    /// random data
    ///
    /// This can be used to improve compression ratios of the generated files.
    #[arg(long = "fill-byte")]
    #[arg(requires = "num-bytes")]
    fill_byte: Option<u8>,

    /// Whether or not to generate exactly N bytes
    #[arg(long = "bytes-exact", action = ArgAction::SetTrue)]
    #[arg(requires = "num-bytes")]
    bytes_exact: bool,

    /// Whether or not to generate exactly N files and bytes
    #[arg(short = 'e', long = "exact", action = ArgAction::SetTrue)]
    #[arg(conflicts_with_all = & ["files_exact", "bytes_exact"])]
    exact: bool,

    /// The maximum directory tree depth
    #[arg(short = 'd', long = "max-depth", alias = "depth")]
    #[arg(value_parser = si_number::<u32>)]
    #[arg(help = "The maximum directory tree depth [default: 5]")]
    max_depth: Option<u32>,

    /// The number of files to generate per directory (default: files / 1000)
    ///
    /// Note: this value is probabilistically respected, meaning not all
    /// directories will have N files).
    #[arg(short = 'r', long = "ftd-ratio")]
    #[arg(value_parser = file_to_dir_ratio_parser)]
    file_to_dir_ratio: Option<NonZeroU64>,

    /// Write an audit log of all generated files to this path
    #[arg(short = 'a', long = "audit-output", alias = "audit-output")]
    #[arg(value_hint = ValueHint::FilePath)]
    audit_output: Option<PathBuf>,
    #[arg(help = "Change the PRNG's starting seed [default: 0]")]
    seed: Option<u64>,

    /// Percentage of additional duplicate files to generate (relative to the number of files)
    #[arg(long = "duplicate-percentage", value_name = "PERCENTAGE")]
    duplicate_percentage: Option<f64>,

    /// Maximum number of duplicates per file
    #[arg(long = "max-duplicates-per-file", value_name = "MAX")]
    max_duplicates_per_file: Option<std::num::NonZeroUsize>,
}

impl Generate {
    fn merge(&mut self, config: &Config) {
        if self.num_files.is_none() {
            self.num_files = config.files;
        }
        if !self.files_exact {
            self.files_exact = config.files_exact.unwrap_or(false);
        }
        if self.num_bytes.is_none() {
            self.num_bytes = config.total_bytes;
        }
        if self.fill_byte.is_none() {
            self.fill_byte = config.fill_byte;
        }
        if !self.bytes_exact {
            self.bytes_exact = config.bytes_exact.unwrap_or(false);
        }
        if !self.exact {
            self.exact = config.exact.unwrap_or(false);
        }
        if self.max_depth.is_none() {
            self.max_depth = config.max_depth;
        }
        if self.file_to_dir_ratio.is_none() {
            self.file_to_dir_ratio = config.ftd_ratio;
        }
        if self.seed.is_none() {
            self.seed = config.seed;
        }
        if self.audit_output.is_none() {
            self.audit_output = config.audit_output.clone();
        }
        if self.duplicate_percentage.is_none() {
            self.duplicate_percentage = config.duplicate_percentage;
        }
        if self.max_duplicates_per_file.is_none() {
            self.max_duplicates_per_file = config.max_duplicates_per_file;
        }
    }
}

impl TryFrom<Generate> for Generator {
    type Error = NumFilesWithRatioError;
    fn try_from(
        Generate {
            root_dir,
            num_files,
            files_exact,
            num_bytes,
            fill_byte,
            bytes_exact,
            exact,
            max_depth,
            file_to_dir_ratio,
            seed,
            audit_output,
            duplicate_percentage,
            max_duplicates_per_file,
        }: Generate,
    ) -> Result<Self, Self::Error> {
        let num_files = num_files.ok_or(NumFilesWithRatioError::InvalidRatio {
            num_files: NonZeroU64::new(1).unwrap(),
            file_to_dir_ratio: NonZeroU64::new(2).unwrap(),
        })?;
        let files_exact = files_exact || exact;
        let num_bytes = num_bytes.unwrap_or(0);
        let bytes_exact = bytes_exact || exact;

        let max_depth = max_depth.unwrap_or(5);
        let seed = seed.unwrap_or(0);

        let builder = Self::builder();
        let builder = builder.root_dir(root_dir);
        let builder = builder.files_exact(files_exact);
        let builder = builder.num_bytes(num_bytes);
        let builder = builder.bytes_exact(bytes_exact);
        let builder = builder.max_depth(max_depth);
        let builder = builder.seed(seed);
        let builder = builder.maybe_fill_byte(fill_byte);
        let builder = if let Some(ratio) = file_to_dir_ratio {
            builder.num_files_with_ratio(NumFilesWithRatio::new(num_files, ratio)?)
        } else {
            builder.num_files_with_ratio(NumFilesWithRatio::from_num_files(num_files))
        };
        let builder = builder.maybe_audit_output(audit_output);
        let builder = builder.maybe_duplicate_percentage(duplicate_percentage);
        let builder = builder.maybe_max_duplicates_per_file(max_duplicates_per_file);
        Ok(builder.build())
    }
}

#[cfg(test)]
mod generate_tests {
    use super::*;

    #[test]
    fn params_are_mapped_correctly() {
        let options = Generate {
            root_dir: PathBuf::from("abc"),
            num_files: Some(NonZeroU64::new(373).unwrap()),
            num_bytes: Some(637),
            fill_byte: None,
            max_depth: Some(43),
            file_to_dir_ratio: Some(NonZeroU64::new(37).unwrap()),
            seed: Some(775),
            files_exact: false,
            bytes_exact: false,
            exact: false,
            audit_output: None,
            duplicate_percentage: None,
            max_duplicates_per_file: None,
        };

        let generator = Generator::try_from(options).unwrap();
        let hack = format!("{generator:?}");

        assert!(hack.contains("root_dir: \"abc\""));
        assert!(hack.contains("num_files: 373"));
        assert!(hack.contains("num_bytes: 637"));
        assert!(hack.contains("max_depth: 43"));
        assert!(hack.contains("file_to_dir_ratio: 37"));
        assert!(hack.contains("seed: 775"));
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CliError {
    #[error("File generator failed.")]
    Generator,
    #[error("An argument combination was invalid.")]
    InvalidArgs,
    #[error("The number of files to generate must be specified via --files or configuration.")]
    MissingNumFiles,
}

#[cfg(feature = "trace")]
#[global_allocator]
static GLOBAL: tracy_client::ProfiledAllocator<std::alloc::System> =
    tracy_client::ProfiledAllocator::new(std::alloc::System, 100);

fn main() -> ExitCode {
    #[cfg(not(debug_assertions))]
    error_stack::Report::install_debug_hook::<std::panic::Location>(|_, _| {});
    error_stack::Report::install_debug_hook::<ExitCode>(|_, _| {});

    let args = Ftzz::parse();

    {
        let level = args.verbose.log_level().unwrap_or_else(log::Level::max);

        #[cfg(not(feature = "trace"))]
        env_logger::builder()
            .format_timestamp(None)
            .filter_level(level.to_level_filter())
            .init();
        #[cfg(feature = "trace")]
        {
            use tracing_log::AsTrace;
            use tracing_subscriber::{
                fmt::format::DefaultFields, layer::SubscriberExt, util::SubscriberInitExt,
            };

            #[derive(Default)]
            struct Config(DefaultFields);

            impl tracing_tracy::Config for Config {
                type Formatter = DefaultFields;

                fn formatter(&self) -> &Self::Formatter {
                    &self.0
                }

                fn stack_depth(&self, _: &tracing::Metadata<'_>) -> u16 {
                    32
                }

                fn format_fields_in_zone_name(&self) -> bool {
                    false
                }
            }

            tracing_subscriber::registry()
                .with(tracing_tracy::TracyLayer::new(Config::default()))
                .with(tracing::level_filters::LevelFilter::from(level.as_trace()))
                .init();
        };
    }

    match ftzz(args) {
        Ok(o) => o.report(),
        Err(err) => {
            drop(writeln!(io::stderr(), "Error: {err:?}"));
            err.report()
        }
    }
}

fn ftzz(
    Ftzz {
        mut options,
        verbose: _,
        help: _,
        config_file,
    }: Ftzz,
) -> error_stack::Result<(), CliError> {
    if let Some(path) = config_file {
        let config = Config::from_file(&path).change_context(CliError::InvalidArgs)?;
        options.merge(&config);
    }

    if options.num_files.is_none() {
        return Err(error_stack::report!(CliError::MissingNumFiles));
    }

    let stdout = stdout();
    Generator::try_from(options)
        .change_context(CliError::InvalidArgs)?
        .generate(&mut stdout.write_adapter())
        .change_context(CliError::Generator)
}

fn num_files_parser(s: &str) -> Result<NonZeroU64, Cow<'static, str>> {
    NonZeroU64::new(si_number(s)?).ok_or_else(|| "At least one file must be generated.".into())
}

fn file_to_dir_ratio_parser(s: &str) -> Result<NonZeroU64, Cow<'static, str>> {
    NonZeroU64::new(si_number(s)?).ok_or_else(|| "Cannot have no files per directory.".into())
}

#[cfg(test)]
mod cli_tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn verify_app() {
        Ftzz::command().debug_assert();
    }

    #[test]
    fn help_for_review() {
        supercilex_tests::help_for_review(Ftzz::command());
    }
}

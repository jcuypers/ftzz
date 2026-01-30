# FTZZ

File Tree Fuzzer creates a pseudo-random directory hierarchy filled with some number of files.

A technical overview of the project is available at https://alexsaveau.dev/blog/ftzz.

## Installation

### Use prebuilt binaries

Binaries for a number of platforms are available on the
[release page](https://github.com/SUPERCILEX/ftzz/releases/latest).

### Build from source

```console,ignore
$ cargo +nightly install ftzz
```

> To install cargo, follow
> [these instructions](https://doc.rust-lang.org/cargo/getting-started/installation.html).

## Usage

Generate a reproducibly random tree in the current directory with *approximately* 1 million files:

```console
$ ftzz /dev/shm/simple -n 1M
? 65
Error: [1mFile generator failed.[22m
├╴at [3msrc/main.rs:385:10[23m
│
╰─▶ [1mFailed to achieve valid generator environment.[22m
    ├╴at [3msrc/generator.rs:211:20[23m
    ╰╴The root directory "/dev/shm/simple" must be empty.

```

Generate *exactly* 1 million files:
 
```console
$ ftzz /dev/shm/exact -en 1M
? 65
Error: [1mFile generator failed.[22m
├╴at [3msrc/main.rs:385:10[23m
│
╰─▶ [1mFailed to achieve valid generator environment.[22m
    ├╴at [3msrc/generator.rs:211:20[23m
    ╰╴The root directory "/dev/shm/exact" must be empty.

```

Generate ~10_000 files with ~1 MB of random data spread across them:

```console
$ ftzz /dev/shm/with_data -n 10K -b 1M
? 65
Error: [1mFile generator failed.[22m
├╴at [3msrc/main.rs:385:10[23m
│
╰─▶ [1mFailed to achieve valid generator environment.[22m
    ├╴at [3msrc/generator.rs:211:20[23m
    ╰╴The root directory "/dev/shm/with_data" must be empty.

```

Because FTZZ creates reproducible outputs, the generated directory will always have the same
structure given the same inputs. To generate variations on a structure with the same parameters,
change the starting seed:

```console
$ ftzz /dev/shm/unseeded -n 100
? 65
Error: [1mFile generator failed.[22m
├╴at [3msrc/main.rs:385:10[23m
│
╰─▶ [1mFailed to achieve valid generator environment.[22m
    ├╴at [3msrc/generator.rs:211:20[23m
    ╰╴The root directory "/dev/shm/unseeded" must be empty.

```

```console
$ ftzz /dev/shm/seeded -n 100 42 # Or $RANDOM
? 65
Error: [1mFile generator failed.[22m
├╴at [3msrc/main.rs:385:10[23m
│
╰─▶ [1mFailed to achieve valid generator environment.[22m
    ├╴at [3msrc/generator.rs:211:20[23m
    ╰╴The root directory "/dev/shm/seeded" must be empty.

```

Other parameters can be found in the built-in docs:

```console
$ ftzz --help
Generate a random directory hierarchy with some number of files

A pseudo-random directory hierarchy will be generated (seeded by this command's input parameters)
containing approximately the target number of files. The exact configuration of files and
directories in the hierarchy is probabilistically determined to mostly match the specified
parameters.

Generated files and directories are named using monotonically increasing numbers, where files are
named `n` and directories are named `n.dir` for a given natural number `n`.

By default, generated files are empty, but random data can be used as the file contents with the
`total-bytes` option.

Usage: ftzz [OPTIONS] <ROOT_DIR> [SEED]

Arguments:
  <ROOT_DIR>
          The directory in which to generate files
          
          The directory will be created if it does not exist.

  [SEED]
          Change the PRNG's starting seed [default: 0]

Options:
  -n, --files <NUM_FILES>
          The number of files to generate
          
          Note: this value is probabilistically respected, meaning any number of files may be
          generated so long as we attempt to get close to N.

      --files-exact
          Whether or not to generate exactly N files

  -b, --total-bytes <NUM_BYTES>
          The total amount of random data to be distributed across the generated files
          
          Note: this value is probabilistically respected, meaning any amount of data may be
          generated so long as we attempt to get close to N.

      --fill-byte <FILL_BYTE>
          Specify a specific fill byte to be used instead of deterministically random data
          
          This can be used to improve compression ratios of the generated files.

      --bytes-exact
          Whether or not to generate exactly N bytes

  -e, --exact
          Whether or not to generate exactly N files and bytes

  -d, --max-depth <MAX_DEPTH>
          The maximum directory tree depth [default: 5]

  -r, --ftd-ratio <FILE_TO_DIR_RATIO>
          The number of files to generate per directory (default: files / 1000)
          
          Note: this value is probabilistically respected, meaning not all directories will have N
          files).

  -a, --audit-output <AUDIT_OUTPUT>
          Write an audit log of all generated files to this path

      --duplicate-percentage <PERCENTAGE>
          Percentage of additional duplicate files to generate (relative to the number of files)

      --max-duplicates-per-file <MAX>
          Maximum number of duplicates per file

      --permissions <OCTAL>
          List of file permission octals to deterministically select from

      --config <CONFIG_FILE>
          Path to a TOML configuration file

  -h, --help
          Print help (use `-h` for a summary)

  -q, --quiet...
          Decrease logging verbosity

  -v, --verbose...
          Increase logging verbosity

  -V, --version
          Print version

```

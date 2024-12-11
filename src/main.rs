use clap::Parser;
use multiqueue::mpmc_queue;
use std::fs::{File, OpenOptions};
use std::io::{self, prelude::*, BufRead, BufWriter, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use tqdm::pbar;

#[derive(Debug, Parser)]
struct Args {
    /// Number of times to repeat each input file
    #[arg(short, long, default_value = "1")]
    repeat_each: NonZeroUsize,

    /// Number of times to repeat all input files
    #[arg(long, default_value = "1")]
    repeat_all: NonZeroUsize,

    /// Optional output location (default: stdout, can improve performance)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Limit maximum memory usage
    #[arg(short, long)]
    max_mem_usage: Option<NonZeroUsize>,

    #[arg(short, long)]
    nthreads: Option<NonZeroUsize>,

    /// Read list of input files from stdin (all position arguments written first)
    #[arg(short, long)]
    stdin_input_list: bool,

    files: Vec<PathBuf>,
}

fn output_to_stdout(
    repeat_each: NonZeroUsize,
    repeat_all: NonZeroUsize,
    files: &Vec<PathBuf>,
) -> Result<(), io::Error> {
    let stdout = io::stdout();
    for _ in 0..repeat_all.into() {
        for f in files {
            for _ in 0..repeat_each.into() {
                let data = std::fs::read(f)?;
                stdout.lock().write_all(&data)?;
            }
        }
    }
    Ok(())
}

fn output_to_file(
    output_file: PathBuf,
    repeat_each: NonZeroUsize,
    repeat_all: NonZeroUsize,
    files: &Vec<PathBuf>,
    nthreads: NonZeroUsize,
) -> Result<(), io::Error> {
    let output = File::create(&output_file)?;

    let mut buffers = Vec::new();
    let mut total_len = 0;
    for f in files {
        // todo limit mem usage
        let buf = Arc::new(std::fs::read(f)?);
        total_len += buf.len() * usize::from(repeat_each);
        buffers.push(buf);
    }
    total_len *= usize::from(repeat_all);
    let buffers = buffers;

    output.set_len(total_len as u64)?;
    drop(output);

    let (sender, receiver) = mpmc_queue::<(u64, Arc<Vec<u8>>)>(usize::from(nthreads) as u64 * 2);
    let mut children: Vec<JoinHandle<Result<(), io::Error>>> = vec![];
    for _ in 0..nthreads.into() {
        let receiver = receiver.clone();
        let output = OpenOptions::new().write(true).open(&output_file)?;
        let mut output = BufWriter::new(output);
        children.push(thread::spawn(move || {
            for (offset, buffer) in receiver {
                output.seek(SeekFrom::Start(offset))?;
                output.write_all(&buffer)?;
            }

            Ok(())
        }));
    }
    receiver.unsubscribe();

    let mut offset = 0;
    let mut pbar = pbar(Some(total_len));
    for _ in 0..repeat_all.into() {
        for b in &buffers {
            for _ in 0..repeat_each.into() {
                while let Err(_) = sender.try_send((offset, b.clone())) {}
                offset += b.len() as u64;
                let _ = pbar.update(b.len());
            }
        }
    }
    Ok(())
}

fn validate_input_paths(files: &Vec<PathBuf>) {
    for f in files {
        assert!(f.exists());
    }
}

fn main() -> Result<(), std::io::Error> {
    let mut args = Args::parse();
    if args.stdin_input_list {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            args.files.push(PathBuf::from(line?));
        }
    }

    validate_input_paths(&args.files);

    match args.output {
        Some(output) => output_to_file(
            output,
            args.repeat_each,
            args.repeat_all,
            &args.files,
            if let Some(n) = args.nthreads {
                n
            } else {
                NonZeroUsize::new(num_cpus::get()).expect("Failed to get core count")
            },
        ),
        None => output_to_stdout(args.repeat_each, args.repeat_all, &args.files),
    }
}

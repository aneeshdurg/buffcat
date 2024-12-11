use clap::Parser;
use multiqueue::mpmc_queue;
use std::fs::{File, OpenOptions};
use std::io::{self, prelude::*, BufRead, BufWriter, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
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
    max_mem_usage: NonZeroUsize,
) -> Result<(), io::Error> {
    let mut files: Vec<File> = files
        .iter()
        .map(|f| File::open(f))
        .collect::<io::Result<Vec<_>>>()?;
    let mut total_each = 0;
    let mut prefix_lens = Vec::new();
    let mut file_sizes = Vec::new();
    for f in &files {
        let bytes_from_file: usize = f.metadata()?.len() as usize;
        file_sizes.push(bytes_from_file);
        prefix_lens.push(total_each);
        total_each += bytes_from_file;
    }
    prefix_lens.push(total_each);
    let total_len = total_each * usize::from(repeat_each) * usize::from(repeat_all);

    // Truncate the output file to the final size
    let output = File::create(&output_file)?;
    output.set_len(total_len as u64)?;
    drop(output);

    let unlimited_mem = usize::from(max_mem_usage) > *prefix_lens.last().unwrap() as usize;
    let buf_size = if unlimited_mem {
        *file_sizes.iter().max().unwrap()
    } else {
        page_size::get()
    };
    assert!(
        buf_size < max_mem_usage.into(),
        "--max-mem-usage value must be at least the size of one page {}",
        buf_size
    );

    let mem_usage = std::cmp::min(
        *prefix_lens.last().unwrap() as usize,
        usize::from(max_mem_usage),
    );
    let n_bufs = mem_usage / buf_size;
    let (buffer_free_notify, buffer_free_events) =
        mpmc_queue((n_bufs * usize::from(repeat_each)) as u64);
    let queue_depth = usize::from(nthreads) as u64 * 2;
    let (task_sender, task_recvr) = mpmc_queue::<(u64, usize, Arc<Vec<u8>>)>(queue_depth);

    let mut buf_alloc_counts: Vec<usize> = Vec::new();
    for i in 0..n_bufs {
        // Initialize every buffer as ready to be written
        buf_alloc_counts.push(1);
        while let Err(_) = buffer_free_notify.try_send(i) {}
    }

    let pbar = Arc::new(Mutex::new(pbar(Some(total_len))));

    let mut children: Vec<JoinHandle<Result<(), io::Error>>> = vec![];
    for _ in 0..nthreads.into() {
        let buffer_free_notify = buffer_free_notify.clone();
        let task_recvr = task_recvr.clone();
        let output = OpenOptions::new().write(true).open(&output_file)?;
        let mut output = BufWriter::new(output);
        let pbar = pbar.clone();
        children.push(thread::spawn(move || {
            for (offset, buf_id, buffer) in task_recvr {
                output.seek(SeekFrom::Start(offset))?;
                output.write_all(&buffer)?;
                let _ = pbar.lock().unwrap().update(buffer.len());
                while let Err(_) = buffer_free_notify.try_send(buf_id) {}
            }

            Ok(())
        }));
    }
    buffer_free_notify.unsubscribe();
    task_recvr.unsubscribe();

    let mut total_written = 0;
    for buf_id in &buffer_free_events {
        buf_alloc_counts[buf_id] -= 1;
        if buf_alloc_counts[buf_id] == 0 {
            let mut buf: Vec<u8> = std::iter::repeat(0).take(buf_size).collect();
            for fid in 0..files.len() {
                let f = &mut files[fid];
                let foffset = f.seek(SeekFrom::Current(0))?;
                let nbytes = f.read(&mut buf)?;
                if nbytes == 0 {
                    continue;
                }

                let buf = Arc::new(buf);
                buf_alloc_counts[buf_id] = usize::from(repeat_each) * usize::from(repeat_all);
                for i in 0..usize::from(repeat_all) {
                    let mut offset = total_each * i + prefix_lens[fid] * usize::from(repeat_each);
                    for _ in 0..usize::from(repeat_each) {
                        while let Err(_) =
                            task_sender.try_send((offset as u64 + foffset, buf_id, buf.clone()))
                        {
                        }
                        offset += file_sizes[fid];
                        total_written += buf.len();
                    }
                }

                break;
            }
        }

        if total_written == total_len {
            break;
        }
    }

    let mut n_outstanding_buffers = buf_alloc_counts
        .iter()
        .fold(0, |acc, e| acc + if *e > 0 { 1 } else { 0 });
    let evt_iter = &mut buffer_free_events.into_iter();
    while n_outstanding_buffers > 0 {
        match evt_iter.next() {
            Some(buf_id) => {
                buf_alloc_counts[buf_id] -= 1;
                if buf_alloc_counts[buf_id] == 0 {
                    n_outstanding_buffers -= 1
                }

                if n_outstanding_buffers == 0 {
                    break;
                }
            }
            None => break,
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
            args.max_mem_usage
                .unwrap_or(NonZeroUsize::new(usize::max_value()).unwrap()),
        ),
        None => output_to_stdout(args.repeat_each, args.repeat_all, &args.files),
    }
}

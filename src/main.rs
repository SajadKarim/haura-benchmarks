use std::{error::Error, path::PathBuf, process, thread, time::Duration};

use betree_perf::Control;
use structopt::StructOpt;

mod ingest;
mod rewrite;
mod switchover;
mod tiered1;
mod zip;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(StructOpt)]
enum Mode {
    Tiered1 {
        log_path: String,
        n_MAX_INTERNAL_NODE_SIZE: usize, 
        n_MIN_FLUSH_SIZE: usize, 
        n_MIN_LEAF_NODE_SIZE: usize, 
        n_MAX_MESSAGE_SIZE: usize, 
        n_CHUNK_SIZE: u32,
    },
    Zip {
        n_clients: u32,
        runs_per_client: u32,
        files_per_run: u32,
        path: PathBuf,
        start_of_eocr: u64,
    },
    Ingest {
        path: PathBuf,
    },
    Switchover {
        part_count: u64,
        part_size: u64,
    },
    Rewrite {
        object_size: u64,
        rewrite_count: u64,
    },
}

fn run_all(mode: Mode) -> Result<(), Box<dyn Error>> {
    thread::spawn(|| betree_perf::log_process_info("proc.jsonl", 250));
    let mut sysinfo = process::Command::new("/home/skarim/myrepo/benchmarks/haura/betree/haura-benchmarks/target/release/sysinfo-log")
        .args(&["--output", "sysinfo.jsonl", "--interval-ms", "250"])
        .spawn()?;

    //let mut control = Control::new();

    match mode {
        Mode::Tiered1 {
            log_path,
            n_MAX_INTERNAL_NODE_SIZE,
            n_MIN_FLUSH_SIZE,
            n_MIN_LEAF_NODE_SIZE,
            n_MAX_MESSAGE_SIZE,
            n_CHUNK_SIZE,
        }=> {
            let mut control = Control::newex(n_MAX_INTERNAL_NODE_SIZE, n_MIN_FLUSH_SIZE, n_MIN_LEAF_NODE_SIZE, n_MAX_MESSAGE_SIZE, n_CHUNK_SIZE);
            let client = control.client(0, b"tiered1");
            tiered1::run(client, n_MAX_INTERNAL_NODE_SIZE * 6144, log_path)?;
            //control.database.write().sync()?;
        }
        Mode::Zip {
            path,
            start_of_eocr,
            n_clients,
            runs_per_client,
            files_per_run,
        } => {
            let mut control = Control::new();
            let mut client = control.client(0, b"zip");

            zip::prepare(&mut client, path, start_of_eocr)?;
            control.database.write().sync()?;

            zip::read(&mut client, n_clients, runs_per_client, files_per_run)?;
        }
        Mode::Ingest { path } => {
            let mut control = Control::new();
            let mut client = control.client(0, b"ingest");
            ingest::run(&mut client, path)?;
        }
        Mode::Switchover {
            part_count,
            part_size,
        } => {
            let mut control = Control::new();
            let mut client = control.client(0, b"switchover");
            switchover::run(&mut client, part_count, part_size)?;
        }
        Mode::Rewrite {
            object_size,
            rewrite_count,
        } => {
            let mut control = Control::new();
            let mut client = control.client(0, b"rewrite");
            rewrite::run(&mut client, object_size, rewrite_count)?;
        }
    }

    thread::sleep(Duration::from_millis(2000));

    sysinfo.kill()?;
    sysinfo.wait()?;

    Ok(())
}

fn main() {
    let mode = Mode::from_args();
    if let Err(e) = run_all(mode) {
        eprintln!("error: {}", e);
        process::exit(1);
    }
}

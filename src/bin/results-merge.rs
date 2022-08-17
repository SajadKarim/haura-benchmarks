use std::{
    fs::File,
    io::{self, Write, BufRead},
    path::{Path, PathBuf},
};

use structopt::StructOpt;

#[derive(StructOpt)]
struct Opts {
    out_file: String,
    in_file: String
}

fn main() {
    let cfg = Opts::from_args();

    let mut dest_file = std::fs::OpenOptions::new()
      .read(true)
      .write(true)
      .append(true)
      .create(true)
      .open(cfg.out_file)
      .unwrap();

    let mut write_total: usize = 0;
    let mut read_total: usize = 0;
    let mut counter: usize = 0;

    if let Ok(lines) = read_lines(cfg.in_file) {
        for line in lines {
            if let Ok(ip) = line {
                let v: Vec<&str> = ip.split(',').collect();
                write_total += v[0].parse::<usize>().unwrap();
                read_total += v[1].parse::<usize>().unwrap();
                
                counter += 1;

            }
        }
    }

    dest_file.write(format!("{},{}\n", write_total/counter, read_total/counter).as_bytes());
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

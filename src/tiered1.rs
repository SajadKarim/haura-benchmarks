use betree_perf::*;
use betree_storage_stack::StoragePreference;
use std::{error::Error, io::Write};

pub fn run(mut client: Client, object_size: usize, log_path: String) -> Result<(), Box<dyn Error>> {
    let OBJECT_SIZE: u64 = object_size as u64;
    println!("running tiered1");

    let os = &client.object_store;

    let mut object_name = "obj1";

    let write = std::time::Instant::now();

        let (obj, _info) =
            os.open_or_create_object_with_pref(object_name.as_bytes(), StoragePreference::FASTEST)?;
        let mut cursor = obj.cursor();

        with_random_bytes(&mut client.rng, OBJECT_SIZE,  1*512*512, |b| {
            cursor.write_all(b)
        })?;

    client.sync().expect("Failed to sync database");

    println!("Writing {} MB took {:?} time", object_size/(1024*1024) , write.elapsed());

    let read = std::time::Instant::now();
    //println!("start reading object");

        let (obj, info) = os
            .open_object_with_info(object_name.as_bytes())?
            .expect("Object was just created, but can't be opened!");

        assert_eq!(info.size, OBJECT_SIZE);
        let size = obj
            .read_all_chunks()?
            .map(|chunk| {
                if let Ok((_k, v)) = chunk {
                    v.len() as u64
                } else {
                    0
                }
            })
            .sum::<u64>();
        assert_eq!(info.size, size);

    println!("Reading {} MB took {:?} time", object_size/(1024*1024) , read.elapsed());
//println!("--- {}",log_path);
let mut file = std::fs::OpenOptions::new()
      .read(true)
      .write(true)
      .append(true)
      .create(true)
      .open(log_path)
      .unwrap();

file.write(format!("{},{},\n", write.elapsed().as_nanos(), read.elapsed().as_nanos()).as_bytes());

    Ok(())
}

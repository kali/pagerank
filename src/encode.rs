use std::mem;

use std::io::{ Result, BufWriter, Write };
use std::fs::File;

pub fn write<I>(target:&str, pairs:&mut I) -> Result<()> where I:Iterator<Item=(u32,u32)> {

    let mut node_writer = BufWriter::new(File::create(format!("{}.offsets", target)).unwrap());
    let mut edge_writer = BufWriter::new(File::create(format!("{}.targets", target)).unwrap());

    let mut cur_source = 0u32;
    let mut cur_offset = 0u64;
    let mut max_vertex = 0u32;

    while let Some((source,target)) = pairs.next() {
        while cur_source < source {
            node_writer.write(&unsafe { mem::transmute::<_, [u8; 8]>(cur_offset) }).unwrap();
            cur_source += 1;
        }

        max_vertex = ::std::cmp::max(max_vertex, source);
        max_vertex = ::std::cmp::max(max_vertex, target);

        edge_writer.write(&unsafe { mem::transmute::<_, [u8; 4]>(target) }).unwrap();
        cur_offset += 1;
    }

    println!("max vertex: {}", max_vertex);

    // a bit of a waste. convenient.
    while cur_source <= max_vertex {
        node_writer.write(&unsafe { mem::transmute::<_, [u8; 8]>(cur_offset) }).unwrap();
        cur_source += 1;
    };

    Ok(())
}

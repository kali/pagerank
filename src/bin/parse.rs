extern crate pagerank;

use std::io::BufRead;

use pagerank::encode;

// output file format is
//
//     offset: [u64; max_src_node_id+1],
//     target: [u32; edges],
//
// target[offset[i]..offset[i+1]] are node i's edge targets.

fn main() {
    println!("usage: parse <target>");
    println!("will overwrite <target>.offsets and <target>.targets");
    let target = std::env::args().skip(1).next().unwrap();
    println!("target: {}", target);

    let input = ::std::io::stdin();
    let mut source = input.lock().lines().map(|x| x.unwrap()).filter(|x| !x.starts_with('#')).map(|line| {

        let elts: Vec<&str> = line[..].split("\t").collect();
        let source: u32 = elts[0].parse().ok().expect("malformed source");
        let target: u32 = elts[1].parse().ok().expect("malformed target");

        (source,target)
    });

    encode::write(&*target,&mut source).unwrap();
}

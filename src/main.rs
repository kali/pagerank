extern crate mmap;
extern crate time;
extern crate timely;
extern crate getopts;
extern crate pagerank;

use timely::progress::timestamp::RootTimestamp;
// use timely::progress::nested::Summary::Local;
use timely::dataflow::*;
use timely::dataflow::operators::*;
// use timely::communication::*;
use timely::dataflow::channels::pact::Exchange;
use timely::drain::DrainExt;

use pagerank::graphmap::GraphMMap;
use pagerank::sorting::{SegmentList, radix_sort_32};

fn main () {

    let filename = std::env::args().skip(1).next().unwrap();
    let strategy = std::env::args().skip(2).next().unwrap() == "process";

    // currently need timely's full option set to parse args
    let mut opts = getopts::Options::new();
    opts.optopt("w", "workers", "", "");
    opts.optopt("p", "process", "", "");
    opts.optopt("n", "processes", "", "");
    opts.optopt("h", "hostfile", "", "");

    if let Ok(matches) = opts.parse(std::env::args().skip(3)) {

        let workers: usize = matches.opt_str("w").map(|x| x.parse().unwrap_or(1)).unwrap_or(1);

        timely::execute_from_args(std::env::args().skip(2), move |root| {

            let index = root.index() as usize;
            let peers = root.peers() as usize;

            let start = time::precise_time_s();

            let nodes = GraphMMap::new(&filename).nodes();

            let mut segments = SegmentList::new(1024); // list of edge segments

            let mut src = vec![];   // holds ranks
            let mut deg = vec![];   // holds source degrees
            let mut rev = vec![];   // holds (dst, deg) pairs
            let mut trn = vec![];   // holds transposed sources

            let mut going = start;

            let mut input = root.scoped(|builder| {

                let (input, edges) = builder.new_input::<(u32, u32)>();
                let (cycle, ranks) = builder.loop_variable::<(u32, f32)>(20, 1);

                let mut ranks = edges.binary_notify(&ranks,
                                    Exchange::new(|x: &(u32,u32)| x.0 as u64),
                                    Exchange::new(|x: &(u32,f32)| x.0 as u64),
                                    "pagerank",
                                    vec![RootTimestamp::new(0)],
                                    move |input1, input2, output, notificator| {

                    // receive incoming edges (should only be iter 0)
                    while let Some((_index, data)) = input1.next() {
                        segments.push(data.drain_temp());
                    }

                    // all inputs received for iter, commence multiplication
                    while let Some((iter, _)) = notificator.next() {

                        let now = time::now();

                        if index == 0 { println!("{}:{}:{}.{} starting iteration {}", now.tm_hour, now.tm_min, now.tm_sec, now.tm_nsec, iter.inner); }

                        // if the very first iteration, prepare some stuff.
                        // specifically, transpose edges and sort by destination.
                        if iter.inner == 0 {
                            let (a, b, c) = transpose(segments.finalize(), peers, nodes);
                            deg = a; rev = b; trn = c;
                            src = vec![0.0f32; deg.len()];
                        }

                        // record some timings in order to estimate per-iteration times
                        if iter.inner == 0  { println!("src: {}, dst: {}, edges: {}", src.len(), rev.len(), trn.len()); }
                        if iter.inner == 10 && index == 0 { going = time::precise_time_s(); }
                        if iter.inner == 20 && index == 0 { println!("average: {}", (time::precise_time_s() - going) / 10.0 ); }

                        // prepare src for transmitting to destinations
                        for s in 0..src.len() { src[s] = 0.15 + 0.85 * src[s] / deg[s] as f32; }

                        // wander through destinations
                        let mut trn_slice = &trn[..];
                        let mut session = output.session(&iter);
                        for &(dst, deg) in &rev {
                            let mut accum = 0.0;
                            for &s in &trn_slice[..deg as usize] {
                                // accum += src[s as usize];
                                unsafe { accum += *src.get_unchecked(s as usize); }
                            }
                            trn_slice = &trn_slice[deg as usize..];
                            session.give((dst, accum));
                        }

                        for s in &mut src { *s = 0.0; }
                    }

                    // receive data from workers, accumulate in src
                    while let Some((iter, data)) = input2.next() {
                        notificator.notify_at(&iter);
                        for &(node, rank) in data.iter() {
                            src[node as usize / peers] += rank;
                        }
                    }
                });

                // optionally, do process-local accumulation
                if strategy {
                    let local_base = workers * (index / workers);
                    let local_index = index % workers;
                    let mut acc = vec![0.0; (nodes / workers) + 1];   // holds ranks
                    ranks = ranks.unary_notify(
                        Exchange::new(move |x: &(u32,f32)| (local_base as u64 + (x.0 as u64 % workers as u64))),
                        "aggregation",
                        vec![],
                        move |input, output, iterator| {
                            while let Some((iter, data)) = input.next() {
                                iterator.notify_at(&iter);
                                for &(node, rank) in data.iter() {
                                    acc[node as usize / workers] += rank;
                                }
                            }

                            while let Some((item, _)) = iterator.next() {
                                output.session(&item)
                                      .give_iterator(acc.drain_temp()
                                                        .enumerate()
                                                        .filter(|x| x.1 != 0.0)
                                                        .map(|(u,f)| ((u * workers + local_index) as u32, f)));

                                for _ in 0..(1 + (nodes/workers)) { acc.push(0.0); }
                            }
                        }
                    );
                }

                ranks.connect_loop(cycle);

                input
            });

            // introduce edges into the computation;
            // allow mmaped file to drop
            {
                let graph = GraphMMap::new(&filename);
                for node in 0..graph.nodes() {
                    if node % peers == index {
                        for dst in graph.edges(node) {
                            input.send((node as u32, *dst as u32));
                        }
                    }
                }
            }
            input.close();
            while root.step() { }

            if index == 0 { println!("elapsed: {}", time::precise_time_s() - start); }
        });
    }
    else {
        println!("error parsing arguments");
        println!("usage:\tpagerank <source> (worker|process) [timely options]");
    }
}

// returns [src/peers] degrees, (dst, deg) pairs, and a list of [src/peers] endpoints
fn transpose(mut edges: Vec<Vec<(u32, u32)>>, peers: usize, nodes: usize) -> (Vec<u32>, Vec<(u32, u32)>, Vec<u32>)  {

    let mut deg = vec![0; (nodes as usize / peers) + 1];
    for list in &edges {
        for &(s, _) in list {
            deg[s as usize / peers] += 1;
        }
    }

    radix_sort_32(&mut edges, &mut Vec::new(), &|&(_,d)| d);

    let mut rev = Vec::<(u32,u32)>::with_capacity(deg.len());
    let mut trn = Vec::with_capacity(edges.len() * 1024);
    for list in edges {
        for (s,d) in list {
            let len = rev.len();
            if (len == 0) || (rev[len-1].0 < d) {
                rev.push((d, 0u32));
            }

            let len = rev.len();
            rev[len-1].1 += 1;
            trn.push(s / peers as u32);
        }
    }

    return (deg, rev, trn);
}

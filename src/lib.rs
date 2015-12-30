extern crate memmap;
extern crate time;
extern crate timely;
extern crate getopts;
#[cfg(test)]
extern crate tempdir;
extern crate timely_communication;

pub mod typedrw;
pub mod graphmap;
pub mod sorting;
pub mod encode;


use timely::progress::timestamp::RootTimestamp;
// use timely::progress::nested::Summary::Local;
use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::scopes::root::Root;
use timely::dataflow::channels::pact::Exchange;
use timely::drain::DrainExt;
use timely_communication::Allocate;

use graphmap::Graph;
use sorting::{SegmentList, radix_sort_32};

pub fn run<G:Graph,A:Allocate>(workers:usize, root:&mut Root<A>, graph:&G, use_process_local:bool, iterations:usize) {

    let index = root.index() as usize;
    let peers = root.peers() as usize;

    let start = time::precise_time_s();

    let nodes = graph.nodes();

    let mut segments = SegmentList::new(1024); // list of edge segments

    // src is sharded naturally by node identifier
    // edges are sharded by source only
    // deg: vec of outgoing edges count, indexed by node/peers
    // rev: list of (dst, incoming edges count)
    // trn: source of edges, each one divided by peers count
    let mut src = vec![];   // holds sharded ranks
    let mut deg = vec![];   // holds sharded source degrees
    let mut rev = vec![];   // holds (dst, deg) pairs
    let mut trn = vec![];   // holds transposed sources

    let mut going = start;

    let mut input = root.scoped(|builder| {

        let (input, edges) = builder.new_input::<(u32, u32)>();
        let (cycle, ranks) = builder.loop_variable::<(u32, f32)>(iterations, 1);

        let mut ranks = edges.binary_notify(&ranks,
                            Exchange::new(|x: &(u32,u32)| x.0 as u64), // edges are hashed by source
                            Exchange::new(|x: &(u32,f32)| x.0 as u64), // ranks
                            "pagerank",
                            vec![RootTimestamp::new(0)],
                            move |input1, input2, output, notificator| {

            // receive outgoing edges (should only be iter 0)
            while let Some((_iter, data)) = input1.next() {
                segments.push(data.drain_temp());
            }

            // all inputs received for iter, commence multiplication
            while let Some((iter, _)) = notificator.next() {
                let now = time::now();

                if index == 0 { println!("{}:{}:{}.{} starting iteration {}", now.tm_hour, now.tm_min, now.tm_sec, now.tm_nsec, iter.inner); }

                // if the very first iteration, prepare some stuff.
                // specifically, transpose edges and sort by destination.
                if iter.inner == 0 {
                    let segs = segments.finalize();
                    let (a, b, c) = transpose(segs, peers, nodes);
                    deg = a; rev = b; trn = c;
                    src = vec![0.15f32; deg.len()];
                }

                // record some timings in order to estimate per-iteration times
                if iter.inner == 0  { println!("worker: {} src: {}, dst: {}, edges: {}", index, src.len(), rev.len(), trn.len()); }
                if iter.inner == 10 && index == 0 { going = time::precise_time_s(); }
                if iter.inner == 20 && index == 0 { println!("average: {}", (time::precise_time_s() - going) / 10.0 ); }

/*
                if iter.inner == iterations {
                    for (ix,x) in src.iter().enumerate() {
                        println!("{}\t{:0.3}", ix*peers+index, x);
                    }
                }
*/
                // from here on, src hold the amount to propagate on each edge
                for s in 0..src.len() { src[s] = 0.85 * src[s] / deg[s] as f32; }

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

                // reset src to 0.15 before accumulation
                for s in &mut src { *s = 0.15; }
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
        if use_process_local {
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
        for node in 0..graph.nodes() {
            if node % peers == index {
                for dst in graph.edges(node) {
                    input.send((node as u32, *dst as u32));
                }
            }
        }
    }
    input.close();
    while root.step() { };
}
// returns [src/peers] degrees, (dst, deg) pairs, and a list of [src/peers] endpoints
fn transpose(mut edges: Vec<Vec<(u32, u32)>>, peers: usize, nodes: usize) -> (Vec<u32>, Vec<(u32, u32)>, Vec<u32>)  {

    let mut deg = vec![0; 1+(nodes as usize / peers)];
    for list in &edges {
        for &(s, _) in list {
            deg[s as usize / peers] += 1;
        }
    }

    radix_sort_32(&mut edges, &mut Vec::new(), &|&(_,d)| d);

    let mut rev = Vec::<(u32,u32)>::with_capacity(deg.len());
    let mut trn = Vec::with_capacity(edges.len() * 1024);
    let mut max = 0;
    for list in edges {
        for (s,d) in list {
            if (rev.len() == 0) || (rev[rev.len()-1].0 < d) {
                rev.push((d, 0u32));
            }

            if max < s {
                max = s;
            }
            if max < d {
                max = d;
            }

            let len = rev.len();
            rev[len-1].1 += 1;
            trn.push(s / peers as u32);
        }
    }
    while deg.len() < (max as usize / peers) {
        deg.push(0);
    }

    (deg, rev, trn)
}

#[test]
fn test_transpose() {
    // three branch star, one worker
    let (deg,rev,trn) = transpose(vec!(vec!((0,1),(0,2),(0,3))), 1, 4);
    assert_eq!(*deg, [3, 0, 0, 0]);
    assert_eq!(*rev, [(1,1),(2,1),(3,1)]);
    assert_eq!(*trn, [0,0,0]);

    // three node pipe, one worker
    let (deg,rev,trn) = transpose(vec!(vec!((0,1),(1,2),(2,3))), 1, 4);
    assert_eq!(*deg, [1, 1, 1, 0]);
    assert_eq!(*rev, [(1,1),(2,1),(3,1)]);
    assert_eq!(*trn, [0,1,2]);

    // three branch star, first worker of two
    let (deg,rev,trn) = transpose(vec!(vec!((0,1),(0,2),(0,3))), 2, 4);
    assert_eq!(*deg, [3, 0]);
    assert_eq!(*rev, [(1,1),(2,1),(3,1)]);
    assert_eq!(*trn, [0,0,0]);

    // three branch star, second worker of two
    let (deg,rev,trn) = transpose(vec!(vec!()), 2, 4);
    assert_eq!(*deg, [0, 0]);
    assert_eq!(*rev, []);
    assert_eq!(*trn, []);

    // three node pipe, first worker of two
    let (deg,rev,trn) = transpose(vec!(vec!((0,1),(2,3))), 2, 4);
    assert_eq!(*deg, [1, 1]);
    assert_eq!(*rev, [(1,1),(3,1)]);
    assert_eq!(*trn, [0,1]);

    // three node pipe, second worker of two
    let (deg,rev,trn) = transpose(vec!(vec!((1,2))), 2, 4);
    assert_eq!(*deg, [1, 0]);
    assert_eq!(*rev, [(2,1)]);
    assert_eq!(*trn, [0]);
}

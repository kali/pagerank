extern crate timely;
extern crate getopts;
extern crate pagerank;

use pagerank::graphmap::GraphMMap;


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

        let graph = GraphMMap::new(&filename);
        let workers: usize = matches.opt_str("w").map(|x| x.parse().unwrap_or(1)).unwrap_or(1);
        timely::execute_from_args(std::env::args().skip(2), move |root| {
            ::pagerank::run(workers, root, &graph, strategy, 20)
        })

    }
    else {
        println!("error parsing arguments");
        println!("usage:\tpagerank <source> (worker|process) [timely options]");
    }
}


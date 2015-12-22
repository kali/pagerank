use typedrw::TypedMemoryMap;

pub struct GraphMMap {
    nodes: TypedMemoryMap<u64>,
    edges: TypedMemoryMap<u32>,
}

impl GraphMMap {
    pub fn nodes(&self) -> usize { self.nodes[..].len() }
    pub fn edges(&self, node: usize) -> &[u32] {
        let nodes = &self.nodes[..];
        if node < nodes.len() {
            let start = if node==0 { 0 } else { nodes[node-1] } as usize;
            let limit = nodes[node] as usize;
            &self.edges[..][start..limit]
        }
        else { &[] }
    }
    pub fn new(prefix: &str) -> GraphMMap {
        GraphMMap {
            nodes: TypedMemoryMap::new(format!("{}.offsets", prefix)),
            edges: TypedMemoryMap::new(format!("{}.targets", prefix)),
        }
    }
}

#[test]
#[cfg(test)]
fn encode_and_graphmap() {
    use encode;
    let target = ::tempdir::TempDir::new("encode_and_graphmap").unwrap();
    let files = target.path().to_string_lossy();
    let data:&[(u32,u32)] = &[(0u32,1u32),(1,2),(2,3)];
    encode::write(&*files, &mut data.iter().cloned()).unwrap();
    let graph = GraphMMap::new(&*files);
    assert_eq!(graph.nodes(), 4);
    let read:Vec<(u32,u32)> = (0..graph.nodes()).flat_map(|src| {
        graph.edges(src).into_iter().map(|d| (src as u32, *d)).collect::<Vec<(u32,u32)>>()
    }).collect();
    assert_eq!(read, data);
}

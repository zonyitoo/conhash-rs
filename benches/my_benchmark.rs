use std::sync::Mutex;

use conhash::{ConsistentHash, Node};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use once_cell::sync::Lazy;

#[derive(Debug, Clone, Eq, PartialEq)]
struct ServerNode {
    host: String,
    port: u16,
}

impl Node for ServerNode {
    fn name(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl ServerNode {
    fn new(host: &str, port: u16) -> ServerNode {
        ServerNode {
            host: host.to_owned(),
            port: port,
        }
    }
}

type Nodes = ConsistentHash<ServerNode>;

static CH: Lazy<Nodes> = Lazy::new(|| new_large_nodes());
static CH_MUT: Lazy<Mutex<Nodes>> = Lazy::new(|| Mutex::new(new_large_nodes()));

fn new_large_nodes() -> Nodes {
    const NODES: usize = 1000;
    const REPLICAS: usize = NODES;
    let mut ch = Nodes::new();

    for i in 0..NODES {
        let node = ServerNode::new("localhost", 10000 + i as u16);
        ch.add(&node, REPLICAS);
    }
    ch
}

fn get(key: &str) {
    CH.get_str(key);
}

fn get_mut(key: &str) {
    CH_MUT.lock().unwrap().get_str_mut(key);
}

fn bench_get(c: &mut Criterion) {
    c.bench_function("get", |b| b.iter(|| get(black_box(""))));
}

fn bench_get_mut(c: &mut Criterion) {
    c.bench_function("get_mut", |b| b.iter(|| get_mut(black_box(""))));
}

criterion_group!(benches, bench_get, bench_get_mut);
criterion_main!(benches);

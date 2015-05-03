# Consistent Hashing for Rust

[![Build Status](https://travis-ci.org/zonyitoo/conhash-rs.svg)](https://travis-ci.org/zonyitoo/conhash-rs)

[Consistent hashing](http://en.wikipedia.org/wiki/Consistent_hashing) is a special kind of hashing such that
when a hash table is resized and consistent hashing is used, only K/n keys need to be remapped on average,
where K is the number of keys, and n is the number of slots.

## Usage

```toml
[dependencies]
conhash = "*"
```

```rust
extern crate conhash;

use conhash::{ConsistentHash, Node};

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

fn main() {
    let nodes = [
        ServerNode::new("localhost", 12345),
        ServerNode::new("localhost", 12346),
        ServerNode::new("localhost", 12347),
        ServerNode::new("localhost", 12348),
        ServerNode::new("localhost", 12349),
        ServerNode::new("localhost", 12350),
        ServerNode::new("localhost", 12351),
        ServerNode::new("localhost", 12352),
        ServerNode::new("localhost", 12353),
    ];

    const REPLICAS: usize = 20;

    let mut ch = ConsistentHash::new();

    for node in nodes.iter() {
        ch.add(node, REPLICAS);
    }

    assert_eq!(ch.len(), nodes.len() * REPLICAS);

    let node_for_hello = ch.get("hello").unwrap().clone();
    assert_eq!(node_for_hello, ServerNode::new("localhost", 12347));

    ch.remove(&ServerNode::new("localhost", 12350));
    assert_eq!(ch.get("hello").unwrap().clone(), node_for_hello);
}
```

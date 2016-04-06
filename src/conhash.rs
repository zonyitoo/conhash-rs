// Copyright 2016 conhash-rs developers
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::{BTreeMap, HashMap};
use std::iter::repeat;

use crypto::digest::Digest;
use crypto::md5::Md5;

use node::Node;

fn default_md5_hash_fn(input: &[u8]) -> Vec<u8> {
    let mut d = Md5::new();
    d.input(input);

    let mut buf = repeat(0).take((d.output_bits() + 7) / 8).collect::<Vec<u8>>();
    d.result(&mut buf);

    buf
}

/// Consistent Hash
pub struct ConsistentHash<N: Node> {
    hash_fn: fn(&[u8]) -> Vec<u8>,
    nodes: BTreeMap<Vec<u8>, N>,
    replicas: HashMap<String, usize>,
}

impl<N: Node> ConsistentHash<N> {

    /// Construct with default hash function (Md5)
    pub fn new() -> ConsistentHash<N> {
        ConsistentHash::with_hash(default_md5_hash_fn)
    }

    /// Construct with customized hash function
    pub fn with_hash(hash_fn: fn(&[u8]) -> Vec<u8>) -> ConsistentHash<N> {
        ConsistentHash {
            hash_fn: hash_fn,
            nodes: BTreeMap::new(),
            replicas: HashMap::new(),
        }
    }

    /// Add a new node
    pub fn add(&mut self, node: &N, num_replicas: usize) {
        debug!("Adding node {:?} with {} replicas", node.name(), num_replicas);
        let node_name = node.name();
        match self.replicas.remove(&node_name) {
            Some(rep) => {
                debug!("Node {:?} is already set with {} replicas, remove it first", node_name, rep);
            },
            None => {}
        }

        for replica in 0..num_replicas {
            let node_ident = format!("{}:{}", node_name, replica);
            let key = (self.hash_fn)(node_ident.as_bytes());
            debug!("Adding node {:?} of replica {}, hashed key is {:?}", node.name(), replica, key);

            self.nodes.insert(key, node.clone());
        }
    }

    /// Get a node by key. Return `None` if no valid node inside
    pub fn get<'a>(&'a self, key: &[u8]) -> Option<&'a N> {
        let hashed_key = (self.hash_fn)(key);
        debug!("Getting key {:?}, hashed key is {:?}", key, hashed_key);

        let mut first_one = None;
        for (k, v) in self.nodes.iter() {
            if hashed_key <= *k {
                debug!("Found node {:?}", v.name());
                return Some(v);
            }

            if first_one.is_none() {
                first_one = Some(v);
            }
        }

        debug!("Search to the end, coming back to the head ...");
        match first_one {
            Some(ref v) => debug!("Found node {:?}", v.name()),
            None => debug!("The container is empty"),
        }
        // Back to the first one
        first_one
    }

    /// Get a node by string key
    pub fn get_str<'a>(&'a self, key: &str) -> Option<&'a N> {
        self.get(key.as_bytes())
    }

    /// Get a node by key. Return `None` if no valid node inside
    pub fn get_mut<'a>(&'a mut self, key: &[u8]) -> Option<&'a mut N> {
        let hashed_key = (self.hash_fn)(key);
        debug!("Getting key {:?}, hashed key is {:?}", key, hashed_key);

        let mut first_one = None;
        for (k, v) in self.nodes.iter_mut() {
            if hashed_key <= *k {
                debug!("Found node {:?}", v.name());
                return Some(v);
            }

            if first_one.is_none() {
                first_one = Some(v);
            }
        }

        debug!("Search to the end, coming back to the head ...");
        match first_one {
            Some(ref v) => debug!("Found node {:?}", v.name()),
            None => debug!("The container is empty"),
        }
        // Back to the first one
        first_one
    }

    /// Get a node by string key
    pub fn get_str_mut<'a>(&'a mut self, key: &str) -> Option<&'a mut N> {
        self.get_mut(key.as_bytes())
    }

    /// Remove a node with all replicas (virtual nodes)
    pub fn remove(&mut self, node: &N) {
        let node_name = node.name();
        debug!("Removing node {:?}", node_name);

        let num_replicas = match self.replicas.remove(&node_name) {
            Some(val) => {
                debug!("Node {:?} has {} replicas", node_name, val);
                val
            },
            None => {
                debug!("Node {:?} not exists", node_name);
                return;
            }
        };

        for replica in 0..num_replicas {
            let node_ident = format!("{}:{}", node.name(), replica);
            let key = (self.hash_fn)(node_ident.as_bytes());
            self.nodes.remove(&key);
        }
    }

    /// Number of nodes
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
}

#[cfg(test)]
mod test {
    use super::ConsistentHash;
    use node::Node;

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

    #[test]
    fn test_basic() {
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

        let node_for_hello = ch.get_str("hello").unwrap().clone();
        assert_eq!(node_for_hello, ServerNode::new("localhost", 12347));

        ch.remove(&ServerNode::new("localhost", 12350));
        assert_eq!(ch.get_str("hello").unwrap().clone(), node_for_hello);
    }
}

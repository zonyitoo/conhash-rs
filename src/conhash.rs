// Copyright 2016 conhash-rs developers
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::{BTreeMap, HashMap};

use md5;

use crate::Node;

fn default_md5_hash_fn(input: &[u8]) -> Vec<u8> {
    let digest = md5::compute(input);
    digest.to_vec()
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
            hash_fn,
            nodes: BTreeMap::new(),
            replicas: HashMap::new(),
        }
    }

    /// Add a new node
    pub fn add(&mut self, node: &N, num_replicas: usize) {
        let node_name = node.name();
        debug!("Adding node {:?} with {} replicas", node_name, num_replicas);

        // Remove it first
        self.remove(node);

        self.replicas.insert(node_name.clone(), num_replicas);
        for replica in 0..num_replicas {
            let node_ident = format!("{}:{}", node_name, replica);
            let key = (self.hash_fn)(node_ident.as_bytes());
            debug!(
                "Adding node {:?} of replica {}, hashed key is {:?}",
                node.name(),
                replica,
                key
            );

            self.nodes.insert(key, node.clone());
        }
    }

    /// Get a node by key. Return `None` if no valid node inside
    pub fn get<'a>(&'a self, key: &[u8]) -> Option<&'a N> {
        if self.nodes.is_empty() {
            debug!("The container is empty");
            return None;
        }

        let hashed_key = (self.hash_fn)(key);
        debug!("Getting key {:?}, hashed key is {:?}", key, hashed_key);

        let entry = self.nodes.range(hashed_key..).next();
        if let Some((_k, v)) = entry {
            debug!("Found node {:?}", v.name());
            return Some(v);
        }

        // Back to the first one
        debug!("Search to the end, coming back to the head ...");
        let first = self.nodes.iter().next();
        debug_assert!(first.is_some());
        let (_k, v) = first.unwrap();
        debug!("Found node {:?}", v.name());
        Some(v)
    }

    /// Get a node by string key
    pub fn get_str<'a>(&'a self, key: &str) -> Option<&'a N> {
        self.get(key.as_bytes())
    }

    /// Get a node by key. Return `None` if no valid node inside
    pub fn get_mut<'a>(&'a mut self, key: &[u8]) -> Option<&'a mut N> {
        let hashed_key = self.get_node_hashed_key(key);
        hashed_key.and_then(move |k| self.nodes.get_mut(&k))
    }

    // Get a node's hashed key by key. Return `None` if no valid node inside
    fn get_node_hashed_key(&self, key: &[u8]) -> Option<Vec<u8>> {
        if self.nodes.is_empty() {
            debug!("The container is empty");
            return None;
        }

        let hashed_key = (self.hash_fn)(key);
        debug!("Getting key {:?}, hashed key is {:?}", key, hashed_key);

        let entry = self.nodes.range(hashed_key..).next();
        if let Some((k, v)) = entry {
            debug!("Found node {:?}", v.name());
            return Some(k.clone());
        }

        // Back to the first one
        debug!("Search to the end, coming back to the head ...");
        let first = self.nodes.iter().next();
        debug_assert!(first.is_some());
        let (k, v) = first.unwrap();
        debug!("Found node {:?}", v.name());
        Some(k.clone())
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
            }
            None => {
                debug!("Node {:?} not exists", node_name);
                return;
            }
        };

        debug!("Node {:?} replicas {}", node_name, num_replicas);

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

    /// Is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<N: Node> Default for ConsistentHash<N> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::*;

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

        assert_eq!(ch.len(), (nodes.len() - 1) * REPLICAS);

        ch.remove(&ServerNode::new("localhost", 12347));
        assert_ne!(ch.get_str("hello").unwrap().clone(), node_for_hello);

        assert_eq!(ch.len(), (nodes.len() - 2) * REPLICAS);
    }
}

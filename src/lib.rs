// Copyright 2016 conhash-rs developers
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! [Consistent Hashing](http://en.wikipedia.org/wiki/Consistent_hashing) is a special
//! kind of hashing such that when a hash table is resized and consistent hashing is used,
//! only K/n keys need to be remapped on average, where K is the number of keys, and n
//! is hte number of slots.

#[macro_use]
extern crate log;
extern crate md5;

pub use conhash::ConsistentHash;
pub use node::Node;

pub mod conhash;
pub mod node;

use std::collections::HashSet;
use std::sync::{Arc, Condvar, Mutex};

use crossbeam_utils::sync::WaitGroup;
use rand::{Rng, thread_rng};
use rand::seq::IteratorRandom;

use kvs::benchmark_common::{self, Promise};
use kvs::KvsEngine;
use kvs::thread_pool::*;

fn write_heavy(store: impl KvsEngine, pool: impl ThreadPool) {
    let keys = benchmark_common::insert_keys(store.clone(), &pool, 10000, (0, 10000000));
    benchmark_common::read_exist(store.clone(), &pool, 100, keys);
}

fn write_queued_kvstore() {}
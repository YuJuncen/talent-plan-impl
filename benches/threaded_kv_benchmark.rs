use std::collections::HashSet;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use crossbeam_utils::sync::WaitGroup;
use rand::{Rng, thread_rng};
use rand::seq::IteratorRandom;

use kvs::benchmark_common::{self, RemoteEngine};
use kvs::KvsEngine;
use kvs::server_common::{Engine, Pool};
use kvs::thread_pool::*;

fn write_heavy(store: impl KvsEngine, pool: impl ThreadPool) {
    let keys = benchmark_common::insert_keys(store.clone(), &pool, 100);
    benchmark_common::read_exist(store.clone(), &pool, 10, keys);
}

fn read_heavy(store: impl KvsEngine, pool: impl ThreadPool) {
    let keys = benchmark_common::insert_keys(store.clone(), &pool, 10);
    benchmark_common::read_exist(store.clone(), &pool, 100, keys);
}

fn write_queued_kvstore(c: &mut Criterion) {
    let temp = tempfile::tempdir().unwrap();
    std::env::set_current_dir(temp.path()).unwrap();
    let store = RemoteEngine::spawn_new(None, Default::default(), Default::default());
    thread::sleep(Duration::from_secs(1));
    c.bench_function("queued_kvstore", |b|
        b.iter(|| {
            write_heavy(store.clone(), RayonThreadPool::new(4).unwrap());
        }),
    );
}

fn read_queued_kvstore(c: &mut Criterion) {
    let temp = tempfile::tempdir().unwrap();
    std::env::set_current_dir(temp.path()).unwrap();
    let store = RemoteEngine::spawn_new(Some("127.0.0.1:4005".parse().unwrap()), Default::default(), Default::default());
    thread::sleep(Duration::from_secs(1));
    c.bench_function("queued_kvstore_read", |b|
        b.iter(|| {
            read_heavy(store.clone(), RayonThreadPool::new(4).unwrap());
        }),
    );
}

fn write_rayon_kvstore(c: &mut Criterion) {
    let temp = tempfile::tempdir().unwrap();
    std::env::set_current_dir(temp.path()).unwrap();
    let store = RemoteEngine::spawn_new(Some("127.0.0.1:4001".parse().unwrap()), Default::default(), Pool::Rayon);
    thread::sleep(Duration::from_secs(1));
    c.bench_function("rayon_kvstore", |b|
        b.iter(|| {
            write_heavy(store.clone(), RayonThreadPool::new(4).unwrap());
        }),
    );
}

fn read_rayon_kvstore(c: &mut Criterion) {
    let temp = tempfile::tempdir().unwrap();
    std::env::set_current_dir(temp.path()).unwrap();
    let store = RemoteEngine::spawn_new(Some("127.0.0.1:4006".parse().unwrap()), Default::default(), Pool::Rayon);
    thread::sleep(Duration::from_secs(1));
    c.bench_function("rayon_kvstore_read", |b|
        b.iter(|| {
            read_heavy(store.clone(), RayonThreadPool::new(4).unwrap());
        }),
    );
}


fn write_queued_sled(c: &mut Criterion) {
    let temp = tempfile::tempdir().unwrap();
    std::env::set_current_dir(temp.path()).unwrap();
    let store = RemoteEngine::spawn_new(Some("127.0.0.1:4002".parse().unwrap()), Engine::Sled, Default::default());
    thread::sleep(Duration::from_secs(1));
    c.bench_function("queued_sled", |b|
        b.iter(|| {
            write_heavy(store.clone(), RayonThreadPool::new(4).unwrap());
        }),
    );
}

fn read_queued_sled(c: &mut Criterion) {
    let temp = tempfile::tempdir().unwrap();
    std::env::set_current_dir(temp.path()).unwrap();
    let store = RemoteEngine::spawn_new(Some("127.0.0.1:4007".parse().unwrap()), Engine::Sled, Default::default());
    thread::sleep(Duration::from_secs(1));
    c.bench_function("queued_sled_read", |b|
        b.iter(|| {
            read_heavy(store.clone(), RayonThreadPool::new(4).unwrap());
        }),
    );
}

fn write_rayon_sled(c: &mut Criterion) {
    let temp = tempfile::tempdir().unwrap();
    std::env::set_current_dir(temp.path()).unwrap();
    let store = RemoteEngine::spawn_new(Some("127.0.0.1:4003".parse().unwrap()), Engine::Sled, Pool::Rayon);
    thread::sleep(Duration::from_secs(1));
    c.bench_function("rayon_sled", |b|
        b.iter(|| {
            write_heavy(store.clone(), RayonThreadPool::new(4).unwrap());
        }),
    );
}

fn read_rayon_sled(c: &mut Criterion) {
    let temp = tempfile::tempdir().unwrap();
    std::env::set_current_dir(temp.path()).unwrap();
    let store = RemoteEngine::spawn_new(Some("127.0.0.1:4008".parse().unwrap()), Engine::Sled, Pool::Rayon);
    thread::sleep(Duration::from_secs(1));
    c.bench_function("read_rayon_sled", |b|
        b.iter(|| {
            read_heavy(store.clone(), RayonThreadPool::new(4).unwrap());
        }),
    );
}

criterion_group! {
    name = tbenches;
    config = Criterion::default().sample_size(10);
    targets =  write_rayon_sled, write_queued_kvstore, write_rayon_kvstore, write_queued_sled,
        read_rayon_sled, read_queued_kvstore, read_rayon_kvstore, read_queued_sled
}
criterion_main!(tbenches);


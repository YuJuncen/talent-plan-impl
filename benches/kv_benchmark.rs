use std::collections::HashSet;

use criterion::{black_box, Criterion, criterion_group, criterion_main};
use rand::prelude::*;

use kvs::{KvsEngine, KvStore};
use kvs::engines::sled::SledEngine;

fn gen_str(rng: &mut impl Rng, length: usize) -> String {
    (0..length).map(|_| format!("{}", rng.gen_range(0, 9))).collect()
}

fn write(engine: &mut impl KvsEngine) -> HashSet<String> {
    let mut keys = HashSet::new();
    let mut rng = thread_rng();
    for _i in 0..100 {
        let key_len = rng.gen_range(1, 100000);
        let key = gen_str(&mut rng, key_len);
        let value_len = rng.gen_range(1, 100000);
        let value = gen_str(&mut rng, value_len);
        keys.insert(key.clone());
        engine.set(key, value).unwrap();
    }
    keys
}

fn read(engine: &mut impl KvsEngine, keys: &HashSet<String>) {
    let mut rng = thread_rng();
    for _i in 0..1000 {
        let k = IteratorRandom::choose(keys.iter(), &mut rng).unwrap();
        engine.get(k.clone()).unwrap().unwrap();
    }
}


fn benchmark(engine: &mut impl KvsEngine) {
    let ks = write(engine);
    read(engine, &ks);
}

fn benchmark_kvs(c: &mut Criterion) {
    let temp = tempfile::tempdir().expect("Unable to get temp dir.");
    let path = temp.path();
    let mut kvs = KvStore::open(path).unwrap();
    c.bench_function("kvs", |b|
        b.iter(|| benchmark(&mut kvs)));
}

fn benchmark_sled(c: &mut Criterion) {
    let temp = tempfile::tempdir().expect("Unable to get temp dir.");
    let path = temp.path();
    let mut sled = SledEngine::open(path).unwrap();
    c.bench_function("sled", |b| {
        b.iter(|| benchmark(&mut sled));
    });
}
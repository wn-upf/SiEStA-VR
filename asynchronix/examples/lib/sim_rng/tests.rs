use super::*;
use rand::Rng;

#[test]
fn same_seed_and_tag_give_the_same_stream_and_others_differ() {
    let mut a = SimRng::for_seed(7, 1);
    let mut b = SimRng::for_seed(7, 1);
    let mut c = SimRng::for_seed(7, 2);
    let mut d = SimRng::for_seed(8, 1);
    let va: Vec<u32> = (0..64).map(|_| a.gen_range(0..1000)).collect();
    assert_eq!(va, (0..64).map(|_| b.gen_range(0..1000)).collect::<Vec<u32>>());
    assert_ne!(va, (0..64).map(|_| c.gen_range(0..1000)).collect::<Vec<u32>>());
    assert_ne!(va, (0..64).map(|_| d.gen_range(0..1000)).collect::<Vec<u32>>());
}

#[test]
fn output_is_uniform_enough_for_backoff_draws() {
    let mut r = SimRng::for_seed(1, 99);
    let n = 200_000;
    let mut counts = [0u32; 16];
    for _ in 0..n {
        counts[r.gen_range(0..16usize)] += 1;
    }
    let expect = n as f64 / 16.0;
    for &c in &counts {
        assert!((c as f64 - expect).abs() < 0.03 * expect, "{counts:?}");
    }
    let mean: f64 = (0..n).map(|_| r.gen::<f64>()).sum::<f64>() / n as f64;
    assert!((mean - 0.5).abs() < 0.005, "mean {mean}");
}

#[test]
fn tags_mix_all_parts() {
    assert_ne!(tag(&[1, 2]), tag(&[2, 1]));
    assert_ne!(tag(&[1, 2]), tag(&[1, 3]));
    assert_ne!(tag_str("a"), tag_str("b"));
}

/// Draws of a fresh run of `seed`, one `next_stream` per call of `step`.
fn run_draws(seed: u64, n: usize, mut step: impl FnMut()) -> Vec<u64> {
    set_global_seed(seed);
    (0..n).map(|_| { step(); next_stream("x").next_u64() ^ stream(5).next_u64() }).collect()
}

#[test]
fn runs_set_up_in_parallel_threads_do_not_move_each_other() {
    let alone = [run_draws(1, 50, || {}), run_draws(2, 50, || {})];
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let interleaved: Vec<Vec<u64>> = [1u64, 2]
        .map(|seed| {
            let b = barrier.clone();
            std::thread::spawn(move || run_draws(seed, 50, || { b.wait(); }))
        })
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();
    assert_eq!(interleaved, alone);
}

#[test]
fn a_captured_run_continues_its_counter_and_a_new_run_restarts_it() {
    let a = run_draws(3, 3, || {});
    set_global_seed(3);
    let run = current();
    let first = next_stream("x").next_u64() ^ stream(5).next_u64();
    // later draws through the handle (as model code makes them) continue the same sequence
    let later: Vec<u64> = (0..2).map(|_| run.next_stream("x").next_u64() ^ run.stream(5).next_u64()).collect();
    assert_eq!(a, [vec![first], later].concat());
    // a new run on this thread does not move the old handle's counter, and starts its own from 0
    assert_eq!(run_draws(3, 3, || {}), a);
    assert_ne!(run.next_stream("x").next_u64() ^ run.stream(5).next_u64(), a[0]);
}

#[test]
fn once_draws_once_per_run_and_key() {
    set_global_seed(4);
    let run = current();
    let v = run.once("k", |r| r.next_stream("o").next_u64());
    assert_eq!(run.once("k", |_| unreachable!()), v);
    assert_ne!(run.once("k2", |r| r.next_stream("o").next_u64()), v);
    set_global_seed(4);
    assert_eq!(current().once("k", |r| r.next_stream("o").next_u64()), v, "a new run draws again, from counter 0");
}

struct Probe;
impl asynchronix::model::Model for Probe {}
impl Probe {
    async fn in_model(&mut self) -> bool {
        asynchronix::simulation::in_model_context()
    }
    async fn seed(&mut self) -> bool {
        std::panic::catch_unwind(global_seed).is_err()
    }
}

#[test]
fn the_free_functions_refuse_model_code() {
    use asynchronix::simulation::{Mailbox, SimInit};
    assert!(!asynchronix::simulation::in_model_context());
    for threads in [1, 2] {
        let mbox = Mailbox::new();
        let addr = mbox.address();
        let mut simu = SimInit::with_num_threads(threads).add_model(Probe, mbox, "probe").init(asynchronix::time::MonotonicTime::EPOCH);
        assert!(simu.process_query(Probe::in_model, (), &addr).unwrap());
        assert!(simu.process_query(Probe::seed, (), &addr).unwrap(), "global_seed() must panic in model code");
    }
}

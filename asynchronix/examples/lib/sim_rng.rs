//! Deterministic simulation randomness.
//!
//! Every random draw that influences a simulation (EDCA back-off, per-MPDU losses, MLO link choice,
//! emulated-link events, ABR sampling, frame-timing jitter, mobility, start offsets, ...) comes from a
//! [`SimRng`] stream derived from the run's seed (the command line's seed, [`set_global_seed`]) and a
//! `tag` that identifies the owner of the stream (an EDCAF, the AP, an emulated link, a headset, ...).
//! Two runs with the same seed and configuration give byte-identical output, whatever the executor's
//! thread count, because each stream is owned by one model and drawn in that model's event order.
//!
//! The seed and the construction counter ([`next_stream`]) belong to the run being set up on the
//! calling thread ([`RunRng`]), not to the process: tests running simulations in parallel threads
//! cannot move each other's draws. Model code may run on executor threads, so a model that draws after
//! setup captures the run with [`current`] (or its own stream) when it is built; the free functions
//! panic when called from model code.
//!
//! `SimRng` is SplitMix64 (8 bytes of state), which is what these draws need (uniform / normal /
//! exponential samples, coin flips); it is not meant for cryptography.
//!
//! Ported from SiEStA-VR main's `sim_rng.rs` (7e9ffa6, b06c73e), without the fork salts.

use rand::RngCore;
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Hasher with fixed keys: `std::collections::HashMap`'s default `RandomState` reseeds every map,
/// so anything that iterates a map (or picks "the first" entry) differs between runs. These aliases
/// give the same iteration order for the same insertion sequence. Build with `default()` /
/// `with_capacity_and_hasher(n, Default::default())`.
pub type DetHasher = std::hash::BuildHasherDefault<std::hash::DefaultHasher>;
pub type DetHashMap<K, V> = std::collections::HashMap<K, V, DetHasher>;
pub type DetHashSet<K> = std::collections::HashSet<K, DetHasher>;

/// Cheap multiply-rotate hasher (the rustc-hash 2 algorithm) for maps that are only ever *looked up*
/// (`get` / `get_mut` / `entry` / `contains` / `insert`), or iterated only in order-independent ways (`any`, a
/// sum, a reverse lookup of a unique value): their results cannot depend on the hasher, and SipHash is a
/// visible share of the MAC's per-TXOP time. A map whose iteration order can reach the results must stay a
/// [`DetHashMap`]. (Ported from SiEStA-VR main b26efbf.)
#[derive(Clone, Copy, Default)]
pub struct FastHasher {
    hash: u64,
}

impl FastHasher {
    const K: u64 = 0xf135_7aea_2e62_a9c5;
    #[inline(always)]
    fn add(&mut self, w: u64) {
        self.hash = self.hash.wrapping_add(w).wrapping_mul(Self::K);
    }
}

impl std::hash::Hasher for FastHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for c in &mut chunks {
            self.add(u64::from_le_bytes(c.try_into().unwrap()));
        }
        let rest = chunks.remainder();
        if !rest.is_empty() {
            let mut b = [0u8; 8];
            b[..rest.len()].copy_from_slice(rest);
            self.add(u64::from_le_bytes(b) ^ ((rest.len() as u64) << 56));
        }
    }
    #[inline] fn write_u8(&mut self, i: u8) { self.add(i as u64) }
    #[inline] fn write_u16(&mut self, i: u16) { self.add(i as u64) }
    #[inline] fn write_u32(&mut self, i: u32) { self.add(i as u64) }
    #[inline] fn write_u64(&mut self, i: u64) { self.add(i) }
    #[inline] fn write_usize(&mut self, i: usize) { self.add(i as u64) }
    #[inline] fn write_i8(&mut self, i: i8) { self.add(i as u8 as u64) }
    #[inline] fn write_i16(&mut self, i: i16) { self.add(i as u16 as u64) }
    #[inline] fn write_i32(&mut self, i: i32) { self.add(i as u32 as u64) }
    #[inline] fn write_i64(&mut self, i: i64) { self.add(i as u64) }
    #[inline] fn write_isize(&mut self, i: isize) { self.add(i as usize as u64) }
    #[inline]
    fn finish(&self) -> u64 {
        self.hash.rotate_left(26)
    }
}

pub type FastHashMap<K, V> = std::collections::HashMap<K, V, std::hash::BuildHasherDefault<FastHasher>>;
pub type FastHashSet<K> = std::collections::HashSet<K, std::hash::BuildHasherDefault<FastHasher>>;

/// Seed used until [`set_global_seed`] is called (runs are deterministic by default).
pub const DEFAULT_SEED: u64 = 0x5157_3D57_A5EE_D001;

/// The seed and construction counter of one run. Clones share the counter, so a model that captured
/// the run at construction continues the same [`next_stream`] sequence when it draws later.
#[derive(Clone, Debug)]
pub struct RunRng {
    seed: u64,
    state: Arc<RunState>,
}

#[derive(Debug, Default)]
struct RunState {
    counter: AtomicU64,
    once: std::sync::Mutex<DetHashMap<String, u64>>,
}

impl RunRng {
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Stream of this run's seed for owner `tag`.
    pub fn stream(&self, tag: u64) -> SimRng {
        SimRng::for_seed(self.seed, tag)
    }

    /// See [`next_stream`].
    pub fn next_stream(&self, tag_name: &str) -> SimRng {
        let n = self.state.counter.fetch_add(1, Ordering::Relaxed);
        self.stream(tag(&[tag_str(tag_name), n]))
    }

    /// `draw`s once per run and `key`; later calls in the same run return the first value. For a random
    /// choice shared by every model that uses one file (a trace's start offset): the file's contents can
    /// be cached for the whole process, the choice must not outlive the run.
    pub fn once(&self, key: &str, draw: impl FnOnce(&RunRng) -> u64) -> u64 {
        let mut once = self.state.once.lock().unwrap();
        if let Some(&v) = once.get(key) {
            return v;
        }
        let v = draw(self);
        once.insert(key.to_string(), v);
        v
    }
}

thread_local! {
    static RUN: RefCell<RunRng> = RefCell::new(RunRng { seed: DEFAULT_SEED, state: Arc::default() });
}

fn assert_setup(f: &str) {
    assert!(
        !asynchronix::simulation::in_model_context(),
        "sim_rng::{f} called from model code: capture the run with sim_rng::current() when the model is built"
    );
}

/// The run being set up on this thread.
pub fn current() -> RunRng {
    assert_setup("current");
    RUN.with(|r| r.borrow().clone())
}

/// Sets the seed every stream derived afterwards on this thread starts from, and starts a new
/// construction counter. Call once per run, before building models.
pub fn set_global_seed(seed: u64) {
    assert_setup("set_global_seed");
    RUN.with(|r| *r.borrow_mut() = RunRng { seed, state: Arc::default() });
}

pub fn global_seed() -> u64 {
    current().seed
}

/// SplitMix64 finalizer: a strong 64-bit mixing function.
#[inline]
pub fn mix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Combines several identifiers into one stream tag.
#[inline]
pub fn tag(parts: &[u64]) -> u64 {
    parts.iter().fold(0x243F_6A88_85A3_08D3, |acc, &p| mix64(acc ^ mix64(p)))
}

/// FNV-1a hash of a string, for tags built from names/paths.
pub fn tag_str(s: &str) -> u64 {
    s.bytes().fold(0xCBF2_9CE4_8422_2325u64, |h, b| (h ^ b as u64).wrapping_mul(0x0000_0100_0000_01B3))
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct SimRng {
    state: u64,
}

impl SimRng {
    /// Stream starting from an explicit seed.
    pub fn new(seed: u64) -> Self {
        Self { state: mix64(seed) }
    }

    /// Independent stream of `seed` for owner `tag`.
    pub fn for_seed(seed: u64, tag: u64) -> Self {
        Self::new(seed ^ mix64(tag))
    }
}

/// Stream of this thread's run seed for owner `tag`.
pub fn stream(tag: u64) -> SimRng {
    current().stream(tag)
}

/// Stream for a *construction-time* draw (random start offsets, random file choices, initial
/// positions) or an owner without a natural id: the n-th call in the run gets its own stream.
/// Deterministic as long as construction happens in a fixed order on one thread (it does: model
/// setup runs sequentially before the simulation starts). From model code, call it on the
/// [`RunRng`] captured at construction.
pub fn next_stream(tag_name: &str) -> SimRng {
    current().next_stream(tag_name)
}

impl RngCore for SimRng {
    #[inline]
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    #[inline]
    fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        for chunk in dest.chunks_mut(8) {
            let b = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&b[..chunk.len()]);
        }
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
        self.fill_bytes(dest);
        Ok(())
    }
}

#[cfg(test)]
mod tests;

//! Thread-safe, **arena-backed** string interner used during N-Triples parsing.
//!
//! Replaces `lasso::ThreadedRodeo` (unmaintained; multi-TB allocation bug under
//! high concurrency, <https://github.com/Kixiron/lasso/issues/48>) while keeping
//! lasso's decisive memory property: term bytes are bump-packed into a few large
//! per-shard buffers, **not** allocated one-per-term.
//!
//! An earlier `dashmap` + `boxcar` version stored one `Arc<str>` per unique
//! term. On real conversions that measured ~20% higher resident memory than
//! lasso even though heap *consumption* was identical — the cost was per-`malloc`
//! overhead plus fragmentation from millions of tiny allocations, invisible to
//! allocation-size accounting but plain in kernel RSS (and enough to OOM a 63 GB
//! input that lasso handled).
//!
//! Design: terms are sharded by content hash. Each shard owns a
//! [`hashbrown::HashTable`] mapping a term to its local index, plus a byte
//! buffer with the term bytes packed end to end and per-term `(offset, len)`
//! spans. A global atomic hands out the dense indices returned to callers. A
//! term always hashes to the same shard, so a fixed shard count bounds lock
//! contention the same way `dashmap` does internally.
use ahash::RandomState;
use hashbrown::HashTable;
use std::str;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

/// Number of lock shards. Power of two so the shard index is a mask of the hash.
const SHARD_COUNT: usize = 64;

#[derive(Default)]
struct Shard {
    /// term hash -> local index within this shard.
    table: HashTable<u32>,
    /// every term's bytes for this shard, packed end to end.
    bytes: Vec<u8>,
    /// local index -> (offset into `bytes`, byte length).
    spans: Vec<(usize, u32)>,
    /// local index -> the dense global index returned by `get_or_intern`.
    globals: Vec<u32>,
}

/// Concurrent, arena-backed string interner returning dense `u32` indices in
/// `0..len()`. Indices are assigned in first-touch order across threads (not a
/// stable insertion order), which is all `parse_nt_terms` requires.
pub(crate) struct Interner {
    shards: Box<[Mutex<Shard>]>,
    next: AtomicU32,
    hasher: RandomState,
}

impl Interner {
    pub fn new() -> Self {
        let shards = (0..SHARD_COUNT).map(|_| Mutex::new(Shard::default())).collect();
        Self { shards, next: AtomicU32::new(0), hasher: RandomState::new() }
    }

    /// Return the dense index for `s`, copying its bytes into the arena and
    /// assigning a new index if it has not been interned yet.
    pub fn get_or_intern(&self, s: &str) -> u32 {
        // Hash the raw bytes (not `s` as a `&str`): `str` and `[u8]` have
        // different `Hash` impls, and the table's rehash closure below hashes
        // `&[u8]` from the arena. Both paths must agree or resizes desync.
        let hash = self.hasher.hash_one(s.as_bytes());
        let shard_idx = (hash as usize) & (SHARD_COUNT - 1);
        let mut guard = self.shards[shard_idx].lock().expect("interner shard poisoned");
        // Split the borrow so the lookup/insert closures can read `bytes`/`spans`
        // while `table` is borrowed.
        let Shard { table, bytes, spans, globals } = &mut *guard;

        if let Some(&local) = table.find(hash, |&l| {
            let (off, len) = spans[l as usize];
            &bytes[off..off + len as usize] == s.as_bytes()
        }) {
            return globals[local as usize];
        }

        // New term: append its bytes to the arena and assign indices.
        let off = bytes.len();
        bytes.extend_from_slice(s.as_bytes());
        let local = u32::try_from(spans.len()).expect("more than u32::MAX terms in one shard");
        spans.push((off, s.len() as u32));
        let global = self.next.fetch_add(1, Ordering::Relaxed);
        globals.push(global);

        let hasher = &self.hasher;
        table.insert_unique(hash, local, |&l| {
            let (o, len) = spans[l as usize];
            hasher.hash_one(&bytes[o..o + len as usize])
        });
        global
    }

    /// Number of distinct terms interned so far.
    pub fn len(&self) -> usize {
        self.next.load(Ordering::Relaxed) as usize
    }

    /// Consume the interner into an index-addressable, owned view of the terms.
    /// Moves the packed shard buffers out directly — no string bytes are copied.
    pub fn into_terms(self) -> Terms {
        let n = self.len();
        let shards: Vec<Shard> =
            self.shards.into_vec().into_iter().map(|m| m.into_inner().expect("interner shard poisoned")).collect();

        // global index -> (shard, offset, len)
        let mut locations = vec![(0u32, 0usize, 0u32); n];
        for (sid, shard) in shards.iter().enumerate() {
            for (local, &global) in shard.globals.iter().enumerate() {
                let (off, len) = shard.spans[local];
                locations[global as usize] = (sid as u32, off, len);
            }
        }
        let bytes: Vec<Vec<u8>> = shards.into_iter().map(|s| s.bytes).collect();
        Terms { bytes, locations }
    }
}

impl std::fmt::Debug for Interner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Interner")
            .field("len", &self.len())
            .field("shards", &self.shards.len())
            .finish_non_exhaustive()
    }
}

/// Owned, index-addressable view of interned terms produced by
/// [`Interner::into_terms`]. The packed byte buffers are moved in directly, so
/// resolving an index is an offset slice with no per-term allocation.
pub(crate) struct Terms {
    /// per-shard packed byte buffers.
    bytes: Vec<Vec<u8>>,
    /// global index -> (shard, offset, len).
    locations: Vec<(u32, usize, u32)>,
}

impl Terms {
    pub const fn len(&self) -> usize {
        self.locations.len()
    }

    /// Raw bytes of the term at `idx`.
    fn raw(&self, idx: u32) -> &[u8] {
        let (sid, off, len) = self.locations[idx as usize];
        &self.bytes[sid as usize][off..off + len as usize]
    }

    /// Order two terms by their bytes. For valid UTF-8 this equals `str`
    /// ordering, so the dictionary sort compares bytes directly and never
    /// re-validates UTF-8 in the hot comparison path.
    pub fn cmp(&self, a: u32, b: u32) -> std::cmp::Ordering {
        self.raw(a).cmp(self.raw(b))
    }

    /// The term at `idx`. The bytes were copied from a `&str`, so they are valid
    /// UTF-8.
    pub fn get(&self, idx: u32) -> &str {
        str::from_utf8(self.raw(idx)).expect("interned bytes are valid UTF-8")
    }
}

#[cfg(test)]
mod tests {
    use super::Interner;
    use rayon::prelude::*;

    #[test]
    fn round_trip_indices() {
        let interner = Interner::new();
        let a = interner.get_or_intern("alpha");
        let b = interner.get_or_intern("beta");
        let a2 = interner.get_or_intern("alpha");
        assert_eq!(a, a2);
        assert_ne!(a, b);
        assert_eq!(interner.len(), 2);
        let terms = interner.into_terms();
        assert_eq!(terms.get(a), "alpha");
        assert_eq!(terms.get(b), "beta");
    }

    #[test]
    fn parallel_inserts_are_consistent() {
        let interner = Interner::new();
        let inputs: Vec<String> = (0..10_000).map(|i| format!("term-{}", i % 1000)).collect();
        let indices: Vec<u32> = inputs.par_iter().map(|s| interner.get_or_intern(s)).collect();
        assert_eq!(interner.len(), 1000);
        let terms = interner.into_terms();
        for (input, idx) in inputs.iter().zip(indices.iter()) {
            assert_eq!(terms.get(*idx), input.as_str());
        }
    }

    #[test]
    fn empty_interner() {
        let interner = Interner::new();
        assert_eq!(interner.len(), 0);
        assert_eq!(interner.into_terms().len(), 0);
    }

    /// Stress test analogous to `concurrent_growth_bounded` in lasso PR #54
    /// (<https://github.com/Kixiron/lasso/pull/54>), which regressed against the
    /// issue #48 multi-TB allocation cascade.
    ///
    /// The structural risk here is different but the invariant is the same:
    /// under heavy contention, racing `Vacant` inserts must not push duplicate
    /// terms into a shard, or the unique count would drift above the truth. The
    /// barrier maximizes contention; the assertions catch any drift and verify
    /// every returned index still round-trips.
    #[test]
    fn concurrent_growth_bounded() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        const THREADS: usize = 64;
        const STRINGS_PER_THREAD: usize = 200;
        const SHARED_KEYS: usize = 50;

        let interner = Arc::new(Interner::new());
        let barrier = Arc::new(Barrier::new(THREADS));

        let handles: Vec<_> = (0..THREADS)
            .map(|tid| {
                let interner = Arc::clone(&interner);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let mut local = Vec::with_capacity(STRINGS_PER_THREAD);
                    for i in 0..STRINGS_PER_THREAD {
                        let s = if i < SHARED_KEYS { format!("shared-{i}") } else { format!("thread-{tid}-{i}") };
                        let idx = interner.get_or_intern(&s);
                        local.push((s, idx));
                    }
                    local
                })
            })
            .collect();

        let mut results: Vec<(String, u32)> = Vec::new();
        for h in handles {
            results.extend(h.join().unwrap());
        }

        let interner = Arc::try_unwrap(interner).unwrap();

        let expected_unique = SHARED_KEYS + THREADS * (STRINGS_PER_THREAD - SHARED_KEYS);
        assert_eq!(interner.len(), expected_unique, "racing inserts produced duplicates in storage");

        let terms = interner.into_terms();
        assert_eq!(terms.len(), expected_unique);
        for (s, idx) in &results {
            assert_eq!(terms.get(*idx), s.as_str(), "round-trip failed for {s}");
        }
    }
}

//! Thread-safe string interner used during N-Triples parsing.
//!
//! Replaces `lasso::ThreadedRodeo`, which is unmaintained and has a known
//! multi-TB allocation bug under high concurrency
//! (<https://github.com/Kixiron/lasso/issues/48>). The composition of
//! `dashmap` + `boxcar` gives the same `get_or_intern` / iterate-in-order
//! contract in ~40 lines.
use boxcar::Vec as BoxcarVec;
use dashmap::{DashMap, Entry};

/// Concurrent string interner returning sequential integer indices.
///
/// Indices are assigned in insertion order: the `n`th unique string interned
/// gets index `n`, and `into_strings()[n]` returns that string.
#[derive(Debug)]
pub(crate) struct Interner {
    map: DashMap<Box<str>, usize>,
    strings: BoxcarVec<Box<str>>,
}

impl Interner {
    pub fn new() -> Self {
        Self { map: DashMap::new(), strings: BoxcarVec::new() }
    }

    /// Return the index for `s`, interning it if not already present.
    pub fn get_or_intern(&self, s: String) -> usize {
        if let Some(v) = self.map.get(s.as_str()) {
            return *v;
        }
        let key: Box<str> = s.into_boxed_str();
        match self.map.entry(key.clone()) {
            Entry::Occupied(o) => *o.get(),
            Entry::Vacant(v) => {
                let idx = self.strings.push(key);
                v.insert(idx);
                idx
            }
        }
    }

    pub fn len(&self) -> usize {
        self.strings.count()
    }

    /// Consume the interner and return strings in the order they were interned.
    pub fn into_strings(self) -> Vec<String> {
        self.strings.into_iter().map(String::from).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::Interner;
    use rayon::prelude::*;

    #[test]
    fn round_trip_indices() {
        let interner = Interner::new();
        let a = interner.get_or_intern("alpha".to_owned());
        let b = interner.get_or_intern("beta".to_owned());
        let a2 = interner.get_or_intern("alpha".to_owned());
        assert_eq!(a, a2);
        assert_ne!(a, b);
        assert_eq!(interner.len(), 2);
        let strings = interner.into_strings();
        assert_eq!(strings[a], "alpha");
        assert_eq!(strings[b], "beta");
    }

    #[test]
    fn parallel_inserts_are_consistent() {
        let interner = Interner::new();
        let inputs: Vec<String> = (0..10_000).map(|i| format!("term-{}", i % 1000)).collect();
        let indices: Vec<usize> = inputs.par_iter().map(|s| interner.get_or_intern(s.clone())).collect();
        assert_eq!(interner.len(), 1000);
        let strings = interner.into_strings();
        for (input, idx) in inputs.iter().zip(indices.iter()) {
            assert_eq!(&strings[*idx], input);
        }
    }

    #[test]
    fn empty_interner() {
        let interner = Interner::new();
        assert_eq!(interner.len(), 0);
        assert!(interner.into_strings().is_empty());
    }

    /// Stress test analogous to `concurrent_growth_bounded` in lasso PR #54
    /// (<https://github.com/Kixiron/lasso/pull/54>), which regressed against
    /// the issue #48 multi-TB allocation cascade.
    ///
    /// Lasso's bug was bucket-allocator-specific (racing threads each doubled
    /// `bucket_capacity`). We don't share that mechanism, but the structural
    /// risk is the same: under heavy contention, racing `Vacant` arms in the
    /// `DashMap` entry path could push duplicate strings into `boxcar`, so
    /// the visible count would drift above the true unique count. The
    /// barrier maximizes contention; the assertion catches any such drift.
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
                        let idx = interner.get_or_intern(s.clone());
                        local.push((s, idx));
                    }
                    local
                })
            })
            .collect();

        let mut results: Vec<(String, usize)> = Vec::new();
        for h in handles {
            results.extend(h.join().unwrap());
        }

        let interner = Arc::try_unwrap(interner).unwrap();

        let expected_unique = SHARED_KEYS + THREADS * (STRINGS_PER_THREAD - SHARED_KEYS);
        assert_eq!(interner.len(), expected_unique, "racing inserts produced duplicates in storage");

        let strings = interner.into_strings();
        assert_eq!(strings.len(), expected_unique);
        for (s, idx) in &results {
            assert_eq!(&strings[*idx], s, "round-trip failed for {s}");
        }
    }
}

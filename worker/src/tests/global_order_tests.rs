use super::*;
use tokio::sync::mpsc;
use std::collections::HashSet;

fn thresholds(n: u64, f: u64, gamma: f64) -> (u16, u16) {
    let non_blank_threshold =
        ((n as f64) * (1.0 - gamma) + gamma * (f as f64) + 1.0).floor() as u16;
    let solid_threshold = (n - 2 * f) as u16;
    (non_blank_threshold, solid_threshold)
}

/// Basic sanity test — ensures that solid txs always appear in prefix
#[test]
fn utig_basic_invariants_hold() {
    let (non_blank_threshold, solid_threshold) = thresholds(9, 2, 1.0);
    // non_blank = 3, solid = 5

    // tx0, tx1 appear 6 times → solid
    // tx2 appears 3 times → non-blank only
    let indices_sets = vec![
        vec![0, 1, 2],
        vec![1, 0],
        vec![0, 1],
        vec![0, 1],
        vec![0, 2],
        vec![1, 2],
        vec![0, 1],
        vec![3],
        vec![4],
    ];

    let (tx, mut rx) = mpsc::channel(1);
    run_utig(indices_sets.clone(), non_blank_threshold, solid_threshold, tx);

    let final_global = rx
        .blocking_recv()
        .expect("UTIG did not produce a prefix (anchor_idx was None)");
    let set: HashSet<_> = final_global.iter().copied().collect();

    // check: solid txs (0,1) must appear
    assert!(set.contains(&0), "solid tx 0 missing");
    assert!(set.contains(&1), "solid tx 1 missing");

    // ensure no duplicates
    assert_eq!(set.len(), final_global.len(), "prefix has duplicates");

    // ensure no invalid indices
    let max_idx = indices_sets.iter().flatten().max().unwrap();
    for idx in &final_global {
        assert!(*idx <= *max_idx, "prefix contains invalid index {}", idx);
    }
}

/// Classic FairPropose shaded-path test (solid anchor, shaded predecessor)
#[test]
fn utig_shaded_vertices_with_path_to_solid_are_included() {
    let (non_blank_threshold, solid_threshold) = thresholds(9, 2, 1.0);
    // solid_threshold = 5

    // tx0 is solid (appears 6×)
    // tx1 is shaded and points into tx0 (appears 3×)
    // tx2, tx3 appear once — remain excluded
    let indices_sets = vec![
        vec![1, 0],
        vec![1, 0],
        vec![0],
        vec![0, 2],
        vec![0, 3],
        vec![0, 4],
        vec![0, 5],
        vec![1, 0],
        vec![0],
    ];

    let (tx, mut rx) = mpsc::channel(1);
    run_utig(indices_sets.clone(), non_blank_threshold, solid_threshold, tx);

    let final_global = rx
        .blocking_recv()
        .expect("UTIG did not produce a prefix (anchor_idx was None)");

    let set: HashSet<_> = final_global.iter().copied().collect();

    assert!(set.contains(&0), "solid tx 0 missing");
    assert!(set.contains(&1), "shaded tx 1 (path→0) missing");
    assert!(
        !set.contains(&2) && !set.contains(&3) && !set.contains(&4) && !set.contains(&5),
        "tx 2 and 3 should not appear, got {:?}",
        final_global
    );
}

/// Multi-cluster UTIG case — two solid clusters, last one defines the prefix cutoff.
#[test]
fn utig_multiple_solid_clusters_and_prefix_cutoff() {
    let (non_blank_threshold, solid_threshold) = thresholds(9, 2, 1.0);
    // non_blank = 3, solid = 5

    let indices_sets = vec![
        // cluster A (solid)
        vec![0, 1, 2, 3, 4],
        vec![1, 2, 3, 4, 0],
        vec![2, 3, 4, 0, 1],
        vec![0, 1, 2, 3, 4],
        vec![1, 2, 3, 4, 0],
        vec![2, 3, 4, 0, 1],

        // cluster B (solid, later)
        vec![5, 6, 7],
        vec![6, 7, 5],
        vec![7, 5, 6],

        // shaded bridges
        vec![10, 2],   // shaded → cluster A
        vec![11, 7],   // shaded → cluster B
        vec![12, 11],  // 12 → 11 → B
    ];

    let (tx, mut rx) = mpsc::channel(1);
    run_utig(indices_sets.clone(), non_blank_threshold, solid_threshold, tx);

    let final_global = rx
        .blocking_recv()
        .expect("UTIG did not produce a prefix (anchor_idx was None)");

    let set: HashSet<_> = final_global.iter().copied().collect();

    for i in 0..=4 {
        assert!(
            set.contains(&i),
            "solid tx {} missing from prefix {:?}",
            i,
            final_global
        );
    }

    assert!(!set.contains(&10), "10 should not be there");
    assert!(!set.contains(&11), "11 should not be there");
    assert!(!set.contains(&12), "12 should not be there");

    assert!(set.iter().all(|&x| x <= 9));
}

#[test]
fn global_order_state_index_and_remove_roundtrip() {
    let mut state = GlobalOrderState::new();
    let d1 = vec![1u8, 2, 3];
    let d2 = vec![4u8, 5, 6];
    let d3 = vec![7u8, 8, 9];

    let i1 = state.index_for_digest(d1.clone());
    let i2 = state.index_for_digest(d2.clone());
    assert_ne!(i1, i2);

    let i1b = state.index_for_digest(d1.clone());
    assert_eq!(i1, i1b);

    state.remove_tx(i1);
    let i3 = state.index_for_digest(d3.clone());
    if i3 == i1 {
        assert_eq!(&state.index_to_tx_digest[i3], &d3);
    }

    let i1c = state.index_for_digest(d1.clone());
    assert_eq!(&state.index_to_tx_digest[i1c], &d1);
}

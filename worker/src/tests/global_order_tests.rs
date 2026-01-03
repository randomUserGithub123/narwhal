// use super::*;
// use tokio::sync::mpsc;
// use std::collections::HashSet;

// fn thresholds(n: u64, f: u64, gamma: f64) -> (u16, u16) {
//     let non_blank_threshold =
//         ((n as f64) * (1.0 - gamma) + gamma * (f as f64) + 1.0).floor() as u16;
//     let solid_threshold = (n - 2 * f) as u16;
//     (non_blank_threshold, solid_threshold)
// }

// fn compute_k(indices_sets: &[Vec<usize>]) -> usize {
//     indices_sets
//         .iter()
//         .flatten()
//         .copied()
//         .max()
//         .map(|m| m + 1)
//         .unwrap_or(0)
// }

// #[test]
// fn utig_basic_invariants_hold() {
//     let (non_blank_threshold, solid_threshold) = thresholds(9, 2, 1.0);

//     let indices_sets = vec![
//         vec![0, 1, 2],
//         vec![1, 0],
//         vec![0, 1],
//         vec![0, 1],
//         vec![0, 2],
//         vec![1, 2],
//         vec![0, 1],
//         vec![3],
//         vec![4],
//     ];

//     let k = compute_k(&indices_sets);

//     let (tx, mut rx) = mpsc::channel(1);
//     run_utig(
//         indices_sets.clone(),
//         k,
//         non_blank_threshold as u8,
//         solid_threshold as u8,
//         tx,
//     );

//     let (final_global, is_complete) = rx
//         .blocking_recv()
//         .expect("UTIG did not produce output");

//     let set: HashSet<_> = final_global.iter().copied().collect();

//     assert!(set.contains(&0));
//     assert!(set.contains(&1));

//     assert_eq!(set.len(), final_global.len());

//     let max_idx = indices_sets.iter().flatten().max().unwrap();
//     for idx in &final_global {
//         assert!(*idx <= *max_idx);
//     }

//     assert!(is_complete);
// }

// #[test]
// fn utig_shaded_vertices_with_path_to_solid_are_included() {
//     let (non_blank_threshold, solid_threshold) = thresholds(9, 2, 1.0);

//     let indices_sets = vec![
//         vec![1, 0],
//         vec![1, 0],
//         vec![0],
//         vec![0, 2],
//         vec![0, 3],
//         vec![0, 4],
//         vec![0, 5],
//         vec![1, 0],
//         vec![0],
//     ];

//     let k = compute_k(&indices_sets);

//     let (tx, mut rx) = mpsc::channel(1);
//     run_utig(
//         indices_sets.clone(),
//         k,
//         non_blank_threshold as u8,
//         solid_threshold as u8,
//         tx,
//     );

//     let (final_global, is_complete) = rx
//         .blocking_recv()
//         .expect("UTIG did not produce output");

//     let set: HashSet<_> = final_global.iter().copied().collect();

//     assert!(set.contains(&0));
//     assert!(set.contains(&1));

//     assert!(!set.contains(&2));
//     assert!(!set.contains(&3));
//     assert!(!set.contains(&4));
//     assert!(!set.contains(&5));

//     assert!(is_complete);
// }

// #[test]
// fn utig_multiple_solid_clusters_and_prefix_cutoff() {
//     let (non_blank_threshold, solid_threshold) = thresholds(9, 2, 1.0);

//     let indices_sets = vec![
//         vec![0, 1, 2, 3, 4],
//         vec![1, 2, 3, 4, 0],
//         vec![2, 3, 4, 0, 1],
//         vec![0, 1, 2, 3, 4],
//         vec![1, 2, 3, 4, 0],
//         vec![2, 3, 4, 0, 1],
//         vec![5, 6, 7],
//         vec![6, 7, 5],
//         vec![7, 5, 6],
//         vec![10, 2],
//         vec![11, 7],
//         vec![12, 11],
//     ];

//     let k = compute_k(&indices_sets);

//     let (tx, mut rx) = mpsc::channel(1);
//     run_utig(
//         indices_sets.clone(),
//         k,
//         non_blank_threshold as u8,
//         solid_threshold as u8,
//         tx,
//     );

//     let (final_global, _is_complete) = rx
//         .blocking_recv()
//         .expect("UTIG did not produce output");

//     let set: HashSet<_> = final_global.iter().copied().collect();

//     for i in 0..=4 {
//         assert!(set.contains(&i));
//     }

//     assert!(!set.contains(&10));
//     assert!(!set.contains(&11));
//     assert!(!set.contains(&12));

//     assert!(set.iter().all(|&x| x <= 9));
// }

// #[test]
// fn utig_incomplete_when_two_kept_shaded_have_no_edge() {
//     let (non_blank_threshold, solid_threshold) = thresholds(9, 2, 1.0);

//     let indices_sets = vec![
//         vec![1, 0],
//         vec![1, 0],
//         vec![1, 0],
//         vec![2, 0],
//         vec![2, 0],
//         vec![2, 0],
//     ];

//     let k = compute_k(&indices_sets);

//     let (tx, mut rx) = mpsc::channel(1);
//     run_utig(
//         indices_sets.clone(),
//         k,
//         non_blank_threshold as u8,
//         solid_threshold as u8,
//         tx,
//     );

//     let (final_global, is_complete) = rx
//         .blocking_recv()
//         .expect("UTIG did not produce output");

//     let set: HashSet<_> = final_global.iter().copied().collect();

//     assert!(set.contains(&0));
//     assert!(set.contains(&1));
//     assert!(set.contains(&2));

//     assert!(!is_complete);
// }

// TODO: TO RUN USE THIS
// cargo test global_order_tests -- --nocapture --test-threads=1

use super::*;
use once_cell::sync::Lazy;
use std::collections::HashSet;
use std::sync::Mutex;
use tokio::sync::mpsc;

static TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

type UtigOut = (Vec<usize>, Vec<u16>, Vec<(u16, u16)>, Vec<u64>);

fn thresholds(n: u64, f: u64, gamma: f64) -> (u16, u16) {
    let non_blank_threshold =
        ((n as f64) * (1.0 - gamma) + gamma * (f as f64) + 1.0).floor() as u16;
    let solid_threshold = (n - 2 * f) as u16;
    (non_blank_threshold, solid_threshold)
}

fn compute_k(indices_sets: &[Vec<usize>]) -> usize {
    indices_sets
        .iter()
        .flatten()
        .copied()
        .max()
        .map(|m| m + 1)
        .unwrap_or(0)
}

fn assert_usize_vec_sane(out: &[usize], k: usize) {
    let set: HashSet<_> = out.iter().copied().collect();
    assert_eq!(set.len(), out.len());
    assert!(out.iter().all(|&x| x < k));
}

fn assert_u16_vec_sane(out: &[u16], k: usize) {
    let set: HashSet<_> = out.iter().copied().collect();
    assert_eq!(set.len(), out.len());
    assert!(out.iter().all(|&x| (x as usize) < k));
}

fn assert_edges_sane(vs: &[u16], es: &[(u16, u16)]) {
    let vset: HashSet<u16> = vs.iter().copied().collect();
    assert!(es.iter().all(|(u, v)| vset.contains(u) && vset.contains(v)));
}

fn run_case(indices_sets: Vec<Vec<usize>>, n: u64, f: u64, gamma: f64) -> Option<UtigOut> {
    let _g = TEST_LOCK.lock().unwrap();

    let (nb, solid) = thresholds(n, f, gamma);
    let k = compute_k(&indices_sets);

    let (tx, mut rx) = mpsc::channel(1);
    run_utig(indices_sets, k, nb as u8, solid as u8, tx);
    rx.blocking_recv()
}

#[test]
fn utig_empty_input_no_output() {
    let _g = TEST_LOCK.lock().unwrap();

    let (nb, solid) = thresholds(9, 2, 1.0);
    let indices_sets: Vec<Vec<usize>> = vec![];
    let k = 0;

    let (tx, mut rx) = mpsc::channel(1);
    run_utig(indices_sets, k, nb as u8, solid as u8, tx);

    assert!(rx.blocking_recv().is_none());
}

#[test]
fn utig_all_blank_no_output() {
    let out = run_case(vec![vec![0], vec![0], vec![1]], 9, 2, 1.0);
    assert!(out.is_none());
}

#[test]
fn utig_nonblank_but_no_solid_anchor_no_output() {
    let out = run_case(vec![vec![0], vec![0], vec![0]], 9, 2, 1.0);
    assert!(out.is_none());
}

#[test]
fn utig_single_solid_only_complete() {
    let indices_sets = vec![vec![0], vec![0], vec![0], vec![0], vec![0]];
    let k = compute_k(&indices_sets);

    let (finalized_now, region_b_v, region_b_e, missing_edges) =
        run_case(indices_sets, 9, 2, 1.0).expect("expected output");

    assert_usize_vec_sane(&finalized_now, k);
    assert_u16_vec_sane(&region_b_v, k);
    assert_edges_sane(&region_b_v, &region_b_e);

    assert_eq!(finalized_now, vec![0]);
    assert!(region_b_v.is_empty());
    assert!(region_b_e.is_empty());
    assert!(missing_edges.is_empty());
}

#[test]
fn utig_tie_breaker_prefers_lower_index() {
    let indices_sets = vec![vec![0, 1], vec![0, 1], vec![1, 0], vec![1, 0]];
    let k = compute_k(&indices_sets);

    let (finalized_now, region_b_v, region_b_e, missing_edges) =
        run_case(indices_sets, 4, 1, 1.0).expect("expected output");

    assert_usize_vec_sane(&finalized_now, k);
    assert_u16_vec_sane(&region_b_v, k);
    assert_edges_sane(&region_b_v, &region_b_e);

    assert_eq!(finalized_now, vec![0, 1]);
    assert!(region_b_v.is_empty());
    assert!(region_b_e.is_empty());
    assert!(missing_edges.is_empty());
}

#[test]
fn utig_incomplete_when_two_kept_shaded_have_no_edge() {
    let indices_sets = vec![
        vec![1, 0],
        vec![1, 0],
        vec![1, 0],
        vec![2, 0],
        vec![2, 0],
        vec![2, 0],
    ];
    let k = compute_k(&indices_sets);

    let (finalized_now, region_b_v, region_b_e, missing_edges) =
        run_case(indices_sets, 9, 2, 1.0).expect("expected output");

    assert_usize_vec_sane(&finalized_now, k);
    assert_u16_vec_sane(&region_b_v, k);
    assert_edges_sane(&region_b_v, &region_b_e);

    let mut included: HashSet<usize> = finalized_now.into_iter().collect();
    included.extend(region_b_v.iter().map(|&x| x as usize));

    assert!(included.contains(&0));
    assert!(included.contains(&1));
    assert!(included.contains(&2));
    assert!(!missing_edges.is_empty());
}

#[test]
fn utig_complete_when_two_kept_shaded_have_edge() {
    let indices_sets = vec![vec![1, 2, 0], vec![1, 2, 0], vec![1, 2, 0], vec![0], vec![0]];
    let k = compute_k(&indices_sets);

    let (finalized_now, region_b_v, region_b_e, missing_edges) =
        run_case(indices_sets, 9, 2, 1.0).expect("expected output");

    assert_usize_vec_sane(&finalized_now, k);
    assert_u16_vec_sane(&region_b_v, k);
    assert_edges_sane(&region_b_v, &region_b_e);

    let mut included: HashSet<usize> = finalized_now.into_iter().collect();
    included.extend(region_b_v.iter().map(|&x| x as usize));

    assert!(included.contains(&0));
    assert!(included.contains(&1));
    assert!(included.contains(&2));
    assert!(missing_edges.is_empty());
}

#[test]
fn utig_excluded_shaded_do_not_affect_completeness() {
    let indices_sets = vec![
        vec![0, 1],
        vec![0, 1],
        vec![0, 1],
        vec![0, 2],
        vec![0, 2],
        vec![0, 2],
    ];
    let k = compute_k(&indices_sets);

    let (finalized_now, region_b_v, region_b_e, missing_edges) =
        run_case(indices_sets, 9, 2, 1.0).expect("expected output");

    assert_usize_vec_sane(&finalized_now, k);
    assert_u16_vec_sane(&region_b_v, k);
    assert_edges_sane(&region_b_v, &region_b_e);

    assert_eq!(finalized_now, vec![0]);
    assert!(region_b_v.is_empty());
    assert!(region_b_e.is_empty());
    assert!(missing_edges.is_empty());
}

#[test]
fn utig_multi_node_scc_is_sorted_in_output() {
    let indices_sets = vec![vec![1, 2, 3, 0], vec![2, 3, 1, 0], vec![3, 1, 2, 0]];
    let k = compute_k(&indices_sets);

    let (finalized_now, region_b_v, region_b_e, missing_edges) =
        run_case(indices_sets, 4, 1, 1.0).expect("expected output");

    assert_usize_vec_sane(&finalized_now, k);
    assert_u16_vec_sane(&region_b_v, k);
    assert_edges_sane(&region_b_v, &region_b_e);

    assert!(region_b_v.is_empty());
    assert!(region_b_e.is_empty());
    assert!(missing_edges.is_empty());

    let p1 = finalized_now.iter().position(|&x| x == 1).unwrap();
    let p2 = finalized_now.iter().position(|&x| x == 2).unwrap();
    let p3 = finalized_now.iter().position(|&x| x == 3).unwrap();
    assert!(p1 < p2 && p2 < p3);
    assert!(finalized_now.contains(&0));
}

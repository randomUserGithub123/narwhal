use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;
use rayon::prelude::*;
use regex::Regex;

/// Adversarial Reordering Metric — Dist(tx,tx') vs fraction reversed
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Directory containing log files
    #[arg(short = 'd', long)]
    logs_dir: PathBuf,

    /// Committee size (n)
    #[arg(short = 'n', long)]
    committee_size: usize,

    /// Number of faults (f)
    #[arg(short, long, default_value_t = 0)]
    faults: usize,

    /// Output JSON file (default: stdout)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Show progress
    #[arg(short, long)]
    verbose: bool,

    /// Log file glob pattern (default: worker-*-0.log)
    #[arg(short = 'p', long, default_value = "worker-*-0.log")]
    log_pattern: String,
}

/// Per-node arrival data: tx_uid → arrival timestamp (as f64 seconds)
type NodeArrivals = HashMap<u64, f64>;

/// Protocol ordering: tx_uid → position (from log line order)
type ProtocolOrder = HashMap<u64, u64>;

fn parse_timestamp(s: &str) -> f64 {
    // Parse ISO 8601: 2026-04-01T19:24:32.030Z
    // We only need relative ordering, so a simple parse suffices.
    let s = s.trim_end_matches('Z');
    let parts: Vec<&str> = s.splitn(2, 'T').collect();
    if parts.len() != 2 {
        return 0.0;
    }
    let time_parts: Vec<&str> = parts[1].splitn(4, |c| c == ':' || c == '.').collect();
    if time_parts.len() < 3 {
        return 0.0;
    }
    let h: f64 = time_parts[0].parse().unwrap_or(0.0);
    let m: f64 = time_parts[1].parse().unwrap_or(0.0);
    let s: f64 = time_parts[2].parse().unwrap_or(0.0);
    let ms: f64 = if time_parts.len() > 3 {
        time_parts[3].parse::<f64>().unwrap_or(0.0) / 1000.0
    } else {
        0.0
    };
    // Add date component for multi-day runs
    let date_parts: Vec<&str> = parts[0].split('-').collect();
    let day: f64 = date_parts.last().and_then(|d| d.parse().ok()).unwrap_or(0.0);
    day * 86400.0 + h * 3600.0 + m * 60.0 + s + ms
}

/// Extract node ID from filename.
/// Supports: worker-X-0.log → X, hotstuff-replica-X.log → X
fn extract_node_id(filename: &str) -> Option<usize> {
    // Try worker-X-0.log first
    if let Some(caps) = Regex::new(r"worker-(\d+)-").ok()?.captures(filename) {
        return caps.get(1)?.as_str().parse().ok();
    }
    // Try hotstuff-replica-X.log (Themis)
    if let Some(caps) = Regex::new(r"hotstuff-replica-(\d+)").ok()?.captures(filename) {
        return caps.get(1)?.as_str().parse().ok();
    }
    // Try replica-X.log (generic)
    if let Some(caps) = Regex::new(r"replica-(\d+)").ok()?.captures(filename) {
        return caps.get(1)?.as_str().parse().ok();
    }
    None
}

/// Parse worker/replica log for arrival times.
/// Supports both formats:
///   FairDAG:  "[2026-04-01T19:24:32.030Z ...] fairness_arrival tx_uid=12345"
///   Themis:   "2026-04-02 13:12:28.565 [hotstuff info] fairness_arrival tx_uid=12345 ts=1775128348.565"
fn parse_worker_arrivals(path: &PathBuf) -> Option<(usize, NodeArrivals)> {
    let filename = path.file_name()?.to_str()?;
    let node_id = extract_node_id(filename)?;

    let file = File::open(path).ok()?;
    let reader = BufReader::with_capacity(512 * 1024, file);

    // Regex for FairDAG format: [ISO_TIMESTAMP ...] fairness_arrival tx_uid=N
    let re_arrival_bracket =
        Regex::new(r"\[([\d\-T:.Z]+)\s.*fairness_arrival tx_uid=(\d+)").ok()?;
    // Regex for Themis format: fairness_arrival tx_uid=N ts=EPOCH
    let re_arrival_ts =
        Regex::new(r"fairness_arrival tx_uid=(\d+) ts=([\d.]+)").ok()?;

    let mut arrivals: NodeArrivals = HashMap::new();

    for line in reader.lines().filter_map(|l| l.ok()) {
        // Try Themis format first (has explicit ts= epoch)
        if let Some(caps) = re_arrival_ts.captures(&line) {
            let tx_uid: u64 = caps.get(1).unwrap().as_str().parse().unwrap_or(0);
            let ts: f64 = caps.get(2).unwrap().as_str().parse().unwrap_or(0.0);
            arrivals.entry(tx_uid).or_insert(ts);
            continue;
        }
        // Fall back to FairDAG bracket format
        if let Some(caps) = re_arrival_bracket.captures(&line) {
            let ts_str = caps.get(1).unwrap().as_str();
            let tx_uid: u64 = caps.get(2).unwrap().as_str().parse().unwrap_or(0);
            let ts = parse_timestamp(ts_str);
            arrivals.entry(tx_uid).or_insert(ts);
        }
    }

    Some((node_id, arrivals))
}

/// Parse worker log for protocol ordering (Herring).
/// Matches: "FairDAG-RL ordered transaction: 12345"
/// Returns tx_uid → position (0-based, order of appearance in log).
fn parse_worker_protocol_order_herring(path: &PathBuf) -> Option<(usize, ProtocolOrder)> {
    let filename = path.file_name()?.to_str()?;
    let node_id = extract_node_id(filename)?;

    let file = File::open(path).ok()?;
    let reader = BufReader::with_capacity(512 * 1024, file);

    let re_ordered =
        Regex::new(r"FairDAG-RL ordered transaction: (\d+)").ok()?;

    let mut order: ProtocolOrder = HashMap::new();
    let mut pos: u64 = 0;

    for line in reader.lines().filter_map(|l| l.ok()) {
        if let Some(caps) = re_ordered.captures(&line) {
            let tx_uid: u64 = caps.get(1).unwrap().as_str().parse().unwrap_or(0);
            if !order.contains_key(&tx_uid) {
                order.insert(tx_uid, pos);
                pos += 1;
            }
        }
    }

    Some((node_id, order))
}

/// Parse worker log for protocol ordering (DoD-W).
/// Matches: "fairness_ordered tx_uid=12345"
fn parse_worker_protocol_order_dodw(path: &PathBuf) -> Option<(usize, ProtocolOrder)> {
    let filename = path.file_name()?.to_str()?;
    let node_id = extract_node_id(filename)?;

    let file = File::open(path).ok()?;
    let reader = BufReader::with_capacity(512 * 1024, file);

    let re_ordered =
        Regex::new(r"fairness_ordered tx_uid=(\d+)").ok()?;

    let mut order: ProtocolOrder = HashMap::new();
    let mut pos: u64 = 0;

    for line in reader.lines().filter_map(|l| l.ok()) {
        if let Some(caps) = re_ordered.captures(&line) {
            let tx_uid: u64 = caps.get(1).unwrap().as_str().parse().unwrap_or(0);
            if !order.contains_key(&tx_uid) {
                order.insert(tx_uid, pos);
                pos += 1;
            }
        }
    }

    Some((node_id, order))
}

struct DistResult {
    dist: usize,
    count: u64,
    reversed: u64,
}

fn main() {
    let args = Args::parse();
    let n = args.committee_size;
    let f = args.faults;
    let min_coverage = n - f;

    let start = Instant::now();

    // ---- 1. Find and parse all worker logs ----
    let pattern = args.logs_dir.join(&args.log_pattern);
    let pattern_str = pattern.to_str().unwrap_or("");

    let paths: Vec<PathBuf> = glob::glob(pattern_str)
        .expect("Failed to read glob pattern")
        .filter_map(|e| e.ok())
        .collect();

    if args.verbose {
        eprintln!("[PARSE] Found {} worker log files", paths.len());
    }

    // Parse arrivals in parallel
    let arrival_results: Vec<(usize, NodeArrivals)> = paths
        .par_iter()
        .filter_map(|p| parse_worker_arrivals(p))
        .collect();

    // Build per-node arrival data, indexed by position 0..K-1
    // Map node_id → index for dense array access
    let mut node_ids: Vec<usize> = arrival_results.iter().map(|(id, _)| *id).collect();
    node_ids.sort();
    node_ids.dedup();
    let k = node_ids.len();
    let node_to_idx: HashMap<usize, usize> = node_ids.iter().enumerate().map(|(i, &id)| (id, i)).collect();

    let mut per_node: Vec<NodeArrivals> = vec![HashMap::new(); k];
    for (node_id, arrivals) in arrival_results {
        if let Some(&idx) = node_to_idx.get(&node_id) {
            per_node[idx] = arrivals;
        }
    }

    if args.verbose {
        for (i, na) in per_node.iter().enumerate() {
            eprintln!("[PARSE]   Node idx={}: {} arrivals", i, na.len());
        }
    }

    // ---- 2. Parse protocol ordering ----
    // Try Herring first, fall back to DoD-W
    let mut protocol_order: ProtocolOrder = HashMap::new();

    for path in &paths {
        if let Some((_, order)) = parse_worker_protocol_order_herring(path) {
            if !order.is_empty() && order.len() > protocol_order.len() {
                protocol_order = order;
            }
        }
    }
    if protocol_order.is_empty() {
        for path in &paths {
            if let Some((_, order)) = parse_worker_protocol_order_dodw(path) {
                if !order.is_empty() && order.len() > protocol_order.len() {
                    protocol_order = order;
                }
            }
        }
    }

    if args.verbose {
        eprintln!("[PARSE] Protocol ordering: {} txs", protocol_order.len());
    }

    // ---- 3. Build eligible tx list ----
    // Eligible = finalized AND seen by >= min_coverage nodes
    let mut eligible: Vec<u64> = Vec::new();
    for &tx in protocol_order.keys() {
        let n_seen = per_node.iter().filter(|na| na.contains_key(&tx)).count();
        if n_seen >= min_coverage {
            eligible.push(tx);
        }
    }
    eligible.sort_by_key(|tx| protocol_order[tx]);
    let n_txs = eligible.len();

    eprintln!(
        "[COMPUTE] {} eligible txs, {} pairs, {} nodes, min_coverage={}",
        n_txs,
        (n_txs as u64) * (n_txs as u64 - 1) / 2,
        k,
        min_coverage
    );
    eprintln!("[COMPUTE] Parse time: {:.1}s", start.elapsed().as_secs_f64());

    if n_txs < 2 {
        eprintln!("[ERROR] Not enough eligible txs");
        std::process::exit(1);
    }

    // ---- 4. Build dense arrival matrix (n_txs × k), NaN for missing ----
    let nan = f64::NAN;
    let mut arrival_matrix: Vec<f64> = vec![nan; n_txs * k];
    let tx_to_idx: HashMap<u64, usize> = eligible.iter().enumerate().map(|(i, &tx)| (tx, i)).collect();

    for (node_k, na) in per_node.iter().enumerate() {
        for (&tx, &t) in na.iter() {
            if let Some(&tx_idx) = tx_to_idx.get(&tx) {
                arrival_matrix[tx_idx * k + node_k] = t;
            }
        }
    }

    // Protocol positions (dense array)
    let fin_pos: Vec<u64> = eligible.iter().map(|tx| protocol_order[tx]).collect();

    // ---- 5. Compute Dist and reversals (parallelized over rows) ----
    let compute_start = Instant::now();

    // Valid Dist values: N%2, N%2+2, ..., N
    let start_dist = n % 2;
    let dist_values: Vec<usize> = (0..=n).filter(|d| d % 2 == start_dist % 2).collect();
    let max_dist = n + 1;

    // Per-thread accumulators → reduce at the end
    let chunk_results: Vec<Vec<(u64, u64)>> = (0..n_txs)
        .into_par_iter()
        .map(|i| {
            let mut local_counts: Vec<(u64, u64)> = vec![(0, 0); max_dist + 1]; // (count, reversed)

            let row_i = &arrival_matrix[i * k..(i + 1) * k];
            let fi = fin_pos[i];

            for j in (i + 1)..n_txs {
                let row_j = &arrival_matrix[j * k..(j + 1) * k];
                let fj = fin_pos[j];

                let mut c1: u32 = 0; // i before j
                let mut c2: u32 = 0; // j before i
                let mut both: u32 = 0;

                for node_k in 0..k {
                    let ti = row_i[node_k];
                    let tj = row_j[node_k];
                    if ti.is_nan() || tj.is_nan() {
                        continue;
                    }
                    both += 1;
                    if ti < tj {
                        c1 += 1;
                    } else if tj < ti {
                        c2 += 1;
                    }
                }

                if (both as usize) < min_coverage {
                    continue;
                }

                let dist = if c1 > c2 { (c1 - c2) as usize } else { (c2 - c1) as usize };

                // Snap to nearest valid Dist
                let snapped = if dist <= max_dist {
                    // Find nearest valid
                    let d = if dist % 2 == start_dist % 2 {
                        dist
                    } else if dist > 0 {
                        dist - 1
                    } else {
                        start_dist
                    };
                    d.min(n)
                } else {
                    n
                };

                local_counts[snapped].0 += 1;

                // Reversed = majority says one order, protocol says opposite
                let reversed = (c1 > c2 && fj < fi) || (c2 > c1 && fi < fj);
                if reversed {
                    local_counts[snapped].1 += 1;
                }
            }

            local_counts
        })
        .collect();

    // Reduce
    let mut total_counts: Vec<(u64, u64)> = vec![(0, 0); max_dist + 1];
    for local in &chunk_results {
        for (d, (c, r)) in local.iter().enumerate() {
            total_counts[d].0 += c;
            total_counts[d].1 += r;
        }
    }

    let compute_time = compute_start.elapsed().as_secs_f64();
    let total_time = start.elapsed().as_secs_f64();

    eprintln!(
        "[COMPUTE] Done in {:.1}s (total {:.1}s)",
        compute_time, total_time
    );

    // ---- 6. Output as JSON ----
    let mut json_entries: Vec<String> = Vec::new();
    let mut total_pairs: u64 = 0;
    let mut total_reversed: u64 = 0;

    for &d in &dist_values {
        let (count, reversed) = total_counts[d];
        total_pairs += count;
        total_reversed += reversed;
        let frac = if count > 0 { reversed as f64 / count as f64 } else { 0.0 };
        json_entries.push(format!(
            "    {{\"dist\": {}, \"fraction_reversed\": {:.6}, \"count\": {}, \"reversed\": {}}}",
            d, frac, count, reversed
        ));

        // Also print to stderr for immediate visibility
        eprintln!(
            "  Dist={:3}: {:.4}  ({} pairs, {} reversed)",
            d, frac, count, reversed
        );
    }

    eprintln!(
        "\n  Total: {} pairs, {} reversed ({:.4})",
        total_pairs,
        total_reversed,
        if total_pairs > 0 { total_reversed as f64 / total_pairs as f64 } else { 0.0 }
    );

    let json = format!(
        "{{\n  \"committee_size\": {},\n  \"faults\": {},\n  \"eligible_txs\": {},\n  \"total_pairs\": {},\n  \"total_reversed\": {},\n  \"compute_time_secs\": {:.2},\n  \"results\": [\n{}\n  ]\n}}",
        n, f, n_txs, total_pairs, total_reversed, total_time,
        json_entries.join(",\n")
    );

    if let Some(output_path) = args.output {
        let mut file = File::create(&output_path).expect("Failed to create output file");
        file.write_all(json.as_bytes()).expect("Failed to write");
        eprintln!("\nResults written to {:?}", output_path);
    } else {
        println!("{}", json);
    }
}
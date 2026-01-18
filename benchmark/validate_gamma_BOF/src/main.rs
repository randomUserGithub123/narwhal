use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;
use rayon::prelude::*;
use regex::Regex;

type BatchKey = (u64, u32); // (scc_id, sub_dag_id)

/// γ-Batch-Order-Fairness Validator
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Directory containing worker-*-0.log files
    #[arg(short = 'd', long)]
    logs_dir: PathBuf,

    /// Order-fairness parameter γ (gamma)
    #[arg(short, long, default_value_t = 1.0)]
    gamma: f64,

    /// Number of faulty nodes (for reference)
    #[arg(short, long, default_value_t = 1)]
    f: usize,

    /// Show verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Output file (default: stdout)
    #[arg(short, long)]
    output: Option<PathBuf>,
}

/// Parsed log data from a single node
struct NodeLogData {
    node_id: String,
    receive_orders: HashMap<String, u32>,
    executions: Vec<(u32, u64, String)>, // (sub_dag_id, scc_id, tx)
    missing_scc_lines: usize,            // how many exec lines matched old format without scc
}

/// A single violation
#[derive(Debug, Clone)]
struct Violation {
    tx_should_be_first: String,
    tx_executed_first: String,
    exec_pos_should_be_first: usize,
    exec_pos_executed_first: usize,
    count_correct: u32,
    total_nodes: usize,
    batch_should_be_first: BatchKey,  // (scc_id, sub_dag_id)
    batch_executed_first: BatchKey,   // (scc_id, sub_dag_id)
}

/// Validation result
struct ValidationResult {
    is_valid: bool,
    num_txs: usize,
    num_nodes: usize,
    gamma: f64,
    threshold: f64,
    num_batches: usize,
    largest_batch: usize,
    single_tx_batches: usize,
    violations: Vec<Violation>,
    execution_converged: bool,
    validation_time_secs: f64,
}

impl std::fmt::Display for ValidationResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = if self.is_valid { "✓ VALID" } else { "✗ INVALID" };
        let converged = if self.execution_converged { "✓ YES" } else { "✗ NO" };

        writeln!(f, "\n╔══════════════════════════════════════════════════════════════════╗")?;
        writeln!(f, "║          γ-Batch-Order-Fairness Validation Result                ║")?;
        writeln!(f, "╠══════════════════════════════════════════════════════════════════╣")?;
        writeln!(f, "║  Status:              {:<43} ║", status)?;
        writeln!(f, "║  Execution Converged: {:<43} ║", converged)?;
        writeln!(f, "╠══════════════════════════════════════════════════════════════════╣")?;
        writeln!(f, "║  Transactions:        {:<43} ║", self.num_txs)?;
        writeln!(f, "║  Correct Replicas:    {:<43} ║", self.num_nodes)?;
        writeln!(f, "║  γ (gamma):           {:<43} ║", self.gamma)?;
        writeln!(f, "║  Threshold:           {:<43.2} ║", self.threshold)?;
        writeln!(f, "║  Batches (SCC keys):  {:<43} ║", self.num_batches)?;
        writeln!(f, "║  Single-tx batches:   {:<43} ║", self.single_tx_batches)?;
        writeln!(f, "║  Largest Batch:       {:<43} ║", self.largest_batch)?;
        writeln!(f, "║  Violations:          {:<43} ║", self.violations.len())?;
        writeln!(f, "║  Time:                {:<43.2}s ║", self.validation_time_secs)?;
        writeln!(f, "╚══════════════════════════════════════════════════════════════════╝")?;

        if !self.violations.is_empty() {
            writeln!(f, "\n❌ VIOLATIONS FOUND ({}):", self.violations.len())?;
            writeln!(f, "   These pairs violate γ-batch-order-fairness:\n")?;

            for (i, v) in self.violations.iter().take(10).enumerate() {
                let tx1_short: String = v.tx_should_be_first.chars().take(24).collect();
                let tx2_short: String = v.tx_executed_first.chars().take(24).collect();

                let (scc_a, sd_a) = v.batch_should_be_first;
                let (scc_b, sd_b) = v.batch_executed_first;

                writeln!(f, "  {}. {} (batch: scc={}, sub_dag={})", i + 1, tx1_short, scc_a, sd_a)?;
                writeln!(f, "     SHOULD come no later than")?;
                writeln!(f, "     {} (batch: scc={}, sub_dag={})", tx2_short, scc_b, sd_b)?;
                writeln!(
                    f,
                    "     {}/{} nodes saw {} before {}",
                    v.count_correct, v.total_nodes, tx1_short, tx2_short
                )?;
                writeln!(
                    f,
                    "     But executed at positions {} vs {} (WRONG ORDER)\n",
                    v.exec_pos_executed_first, v.exec_pos_should_be_first
                )?;
            }

            if self.violations.len() > 10 {
                writeln!(f, "  ... and {} more violations", self.violations.len() - 10)?;
            }
        }

        Ok(())
    }
}

/// Parse a single worker log file
fn parse_worker_log(path: &PathBuf) -> Option<NodeLogData> {
    let filename = path.file_name()?.to_str()?;

    // Extract node ID from filename: worker-X-0.log -> X
    let re_filename = Regex::new(r"worker-(\d+)-").ok()?;
    let node_id = re_filename
        .captures(filename)?
        .get(1)?
        .as_str()
        .to_string();

    let file = File::open(path).ok()?;
    let reader = BufReader::with_capacity(1024 * 1024, file); // 1MB buffer

    let re_receive =
        Regex::new(r"Receival of tx_digest\s+([^\s]+)\s+at position\s+(\d+)").ok()?;

    // New format:
    //   sub_dag_id=12: scc=3: Executed 0xabc...
    let re_exec_new =
        Regex::new(r"sub_dag_id=(\d+):\s*scc=(\d+):\s*Executed\s+([^\s]+)").ok()?;

    // Old format fallback:
    //   sub_dag_id=12: Executed 0xabc...
    // If we hit this, we *guess* scc_id = sub_dag_id (works for your AUNCEL case, but may be wrong for UTIG).
    let re_exec_old = Regex::new(r"sub_dag_id=(\d+):\s*Executed\s+([^\s]+)").ok()?;

    let mut receive_orders = HashMap::new();
    let mut executions: Vec<(u32, u64, String)> = Vec::new();
    let mut seen_execs: HashSet<String> = HashSet::new();
    let mut missing_scc_lines = 0usize;

    for line in reader.lines().filter_map(|l| l.ok()) {
        // Parse receive orders
        if let Some(caps) = re_receive.captures(&line) {
            let tx = caps.get(1).unwrap().as_str().trim().to_string();
            let pos: u32 = caps.get(2).unwrap().as_str().parse().unwrap_or(0);
            receive_orders.entry(tx).or_insert(pos);
        }

        // Parse executions (new format preferred)
        if let Some(caps) = re_exec_new.captures(&line) {
            let sub_dag_id: u32 = caps.get(1).unwrap().as_str().parse().unwrap_or(0);
            let scc_id: u64 = caps.get(2).unwrap().as_str().parse().unwrap_or(0);
            let tx = caps.get(3).unwrap().as_str().trim().to_string();

            if !seen_execs.contains(&tx) {
                executions.push((sub_dag_id, scc_id, tx.clone()));
                seen_execs.insert(tx);
            }
            continue;
        }

        // Fallback (old format)
        if let Some(caps) = re_exec_old.captures(&line) {
            let sub_dag_id: u32 = caps.get(1).unwrap().as_str().parse().unwrap_or(0);
            let tx = caps.get(2).unwrap().as_str().trim().to_string();

            if !seen_execs.contains(&tx) {
                missing_scc_lines += 1;
                let scc_guess: u64 = sub_dag_id as u64;
                executions.push((sub_dag_id, scc_guess, tx.clone()));
                seen_execs.insert(tx);
            }
        }
    }

    Some(NodeLogData {
        node_id,
        receive_orders,
        executions,
        missing_scc_lines,
    })
}

/// Parse all logs in directory.
/// Returns:
/// - receive_orders[node_id][tx] = receive position
/// - canonical execution order (order-of-appearance in the log)
/// - canonical batch key map: batch_of_tx[tx] = (scc_id, sub_dag_id)
/// - converged: whether all nodes agree on execution order AND batch keys
fn parse_all_logs(
    logs_dir: &PathBuf,
) -> (
    HashMap<String, HashMap<String, u32>>,
    Vec<String>,
    HashMap<String, BatchKey>,
    bool,
) {
    let pattern = logs_dir.join("worker-*-0.log");
    let pattern_str = pattern.to_str().unwrap_or("");

    let paths: Vec<PathBuf> = glob::glob(pattern_str)
        .expect("Failed to read glob pattern")
        .filter_map(|e| e.ok())
        .collect();

    println!("[PARSE] Found {} worker log files", paths.len());

    let start = Instant::now();

    // Parse logs in parallel
    let results: Vec<NodeLogData> = paths
        .par_iter()
        .filter_map(|path| parse_worker_log(path))
        .collect();

    println!("[PARSE] Parsing completed in {:.1}s", start.elapsed().as_secs_f64());

    let mut receive_orders: HashMap<String, HashMap<String, u32>> = HashMap::new();
    let mut all_executions: HashMap<String, Vec<(u32, u64, String)>> = HashMap::new();
    let mut total_missing_scc_lines = 0usize;

    for data in results {
        println!(
            "[PARSE]   Node {}: {} received, {} executed{}",
            data.node_id,
            data.receive_orders.len(),
            data.executions.len(),
            if data.missing_scc_lines > 0 {
                format!(" (WARN: {} exec lines without scc=...)", data.missing_scc_lines)
            } else {
                "".to_string()
            }
        );
        total_missing_scc_lines += data.missing_scc_lines;
        receive_orders.insert(data.node_id.clone(), data.receive_orders);
        all_executions.insert(data.node_id, data.executions);
    }

    if total_missing_scc_lines > 0 {
        println!(
            "[PARSE] ⚠️  Detected {} execution lines without scc=... across all logs.",
            total_missing_scc_lines
        );
        println!(
            "[PARSE] ⚠️  Using fallback scc_id=sub_dag_id for those lines. Prefer updating logs to include scc=..."
        );
    }

    // Check convergence and build canonical order + canonical batch keys
    let mut canonical_order: Vec<String> = Vec::new();
    let mut canonical_batch_of_tx: HashMap<String, BatchKey> = HashMap::new();
    let mut converged = true;
    let mut canonical_node: Option<String> = None;

    for (node_id, execs) in all_executions.iter() {
        let mut exec_order: Vec<String> = Vec::with_capacity(execs.len());
        let mut batch_of_tx: HashMap<String, BatchKey> = HashMap::with_capacity(execs.len());

        // IMPORTANT: preserve order-of-appearance in the log (this is your true execution order)
        for (sub_dag_id, scc_id, tx) in execs.iter() {
            exec_order.push(tx.clone());
            batch_of_tx.insert(tx.clone(), (*scc_id, *sub_dag_id));
        }

        if canonical_order.is_empty() {
            canonical_order = exec_order;
            canonical_batch_of_tx = batch_of_tx;
            canonical_node = Some(node_id.clone());
        } else {
            if exec_order != canonical_order || batch_of_tx != canonical_batch_of_tx {
                converged = false;
                println!(
                    "[PARSE] ✗ Execution divergence: Node {} differs from Node {}",
                    node_id,
                    canonical_node.as_ref().unwrap()
                );
            }
        }
    }

    if converged {
        println!("[PARSE] ✓ All nodes converged on execution order + batch keys");
    }

    println!(
        "[PARSE] Total: {} nodes, {} executed txs",
        receive_orders.len(),
        canonical_order.len()
    );

    (receive_orders, canonical_order, canonical_batch_of_tx, converged)
}

fn validate_batch_order_fairness(
    receive_orders: &HashMap<String, HashMap<String, u32>>,
    execution_order: &[String],
    batch_of_tx: &HashMap<String, BatchKey>,
    gamma: f64,
    verbose: bool,
) -> ValidationResult {
    let start = Instant::now();

    let num_txs = execution_order.len();
    let num_nodes = receive_orders.len();
    let threshold = gamma * (num_nodes as f64);

    println!("\n{}", "=".repeat(70));
    println!("γ-BATCH-ORDER-FAIRNESS VALIDATION (LOGGED SCC)");
    println!("{}", "=".repeat(70));
    println!("Transactions: {}", num_txs);
    println!("Active nodes (n-f): {}", num_nodes);
    println!("γ (gamma): {}", gamma);
    println!("Threshold: {:.2} (γ × n, need >= threshold)", threshold);
    println!("{}\n", "=".repeat(70));

    if num_txs == 0 {
        return ValidationResult {
            is_valid: true,
            num_txs: 0,
            num_nodes,
            gamma,
            threshold,
            num_batches: 0,
            largest_batch: 0,
            single_tx_batches: 0,
            violations: vec![],
            execution_converged: true,
            validation_time_secs: start.elapsed().as_secs_f64(),
        };
    }

    // Step 1: Create execution position mapping (tx -> position)
    let exec_position: HashMap<&String, usize> = execution_order
        .iter()
        .enumerate()
        .map(|(i, tx)| (tx, i))
        .collect();

    // Batch stats (from logged SCC keys)
    let mut batch_sizes: HashMap<BatchKey, usize> = HashMap::new();
    for tx in execution_order.iter() {
        if let Some(key) = batch_of_tx.get(tx) {
            *batch_sizes.entry(*key).or_insert(0) += 1;
        }
    }
    let num_batches = batch_sizes.len();
    let largest_batch = batch_sizes.values().copied().max().unwrap_or(0);
    let single_tx_batches = batch_sizes.values().filter(|&&sz| sz == 1).count();

    println!(
        "[VALIDATE] Batches (unique (scc_id, sub_dag_id) keys): {} (largest={}, single_tx={})",
        num_batches, largest_batch, single_tx_batches
    );

    // Step 2: Build precedence edges
    // Edge A -> B means: >= γn nodes saw A before B
    println!("[VALIDATE] Building precedence edges...");

    // DEBUG: Check that all executed txs are in all nodes' receive orders
    if verbose {
        println!("[DEBUG] Verifying all executed txs are in any receive_orders...");
        let mut missing_count = 0usize;
        let mut found_count = 0usize;
        for tx in execution_order.iter() {
            let mut found_in_any = false;
            for (_node_id, node_orders) in receive_orders.iter() {
                if node_orders.contains_key(tx) {
                    found_in_any = true;
                    break;
                }
            }
            if found_in_any {
                found_count += 1;
            } else {
                if missing_count < 5 {
                    println!("[DEBUG] WARNING: Executed tx NOT in ANY receive_orders: {}", tx);
                }
                missing_count += 1;
            }
        }
        println!(
            "[DEBUG] Executed txs found in receive_orders: {}/{}",
            found_count, num_txs
        );
        if missing_count > 0 {
            println!(
                "[DEBUG] ⚠️ {} executed txs NOT found in receive_orders - FORMAT MISMATCH?",
                missing_count
            );
        }
    }

    // For each pair (A, B), count how many nodes saw A before B
    let edges: Vec<(usize, usize, u32)> = (0..num_txs)
        .into_par_iter()
        .flat_map(|i| {
            let tx_a = &execution_order[i];
            let mut local_edges = Vec::new();

            for j in 0..num_txs {
                if i == j {
                    continue;
                }
                let tx_b = &execution_order[j];

                let mut count_a_before_b: u32 = 0;

                for (_node_id, node_orders) in receive_orders.iter() {
                    if let (Some(&pos_a), Some(&pos_b)) = (node_orders.get(tx_a), node_orders.get(tx_b)) {
                        if pos_a < pos_b {
                            count_a_before_b += 1;
                        }
                    }
                }

                // If >= γn nodes saw A before B, add edge A -> B
                if (count_a_before_b as f64) >= threshold {
                    local_edges.push((i, j, count_a_before_b));
                }
            }

            local_edges
        })
        .collect();

    println!("[VALIDATE] Precedence edges: {} (A->B constraints)", edges.len());

    if verbose {
        println!("[DEBUG] Sample edges (first 5):");
        for (i, j, count) in edges.iter().take(5) {
            let tx_a = &execution_order[*i];
            let tx_b = &execution_order[*j];
            println!(
                "[DEBUG]   Edge: tx[{}] -> tx[{}], count={}/{}",
                i, j, count, num_nodes
            );
            println!("[DEBUG]     A: {}", &tx_a[..30.min(tx_a.len())]);
            println!("[DEBUG]     B: {}", &tx_b[..30.min(tx_b.len())]);
        }
    }

    // Step 3: Check violations using logged batch keys
    println!("[VALIDATE] Checking for violations...");

    let violations: Vec<Violation> = edges
        .par_iter()
        .filter_map(|(i, j, count)| {
            let tx_a = &execution_order[*i]; // >= γn nodes saw A before B
            let tx_b = &execution_order[*j];

            let exec_pos_a = exec_position[tx_a];
            let exec_pos_b = exec_position[tx_b];

            // If B is not executed before A, no problem.
            if exec_pos_b >= exec_pos_a {
                return None;
            }

            // B executed before A; OK only if same batch key (scc_id, sub_dag_id)
            let key_a = *batch_of_tx.get(tx_a)?;
            let key_b = *batch_of_tx.get(tx_b)?;

            if key_a == key_b {
                return None;
            }

            Some(Violation {
                tx_should_be_first: tx_a.clone(),
                tx_executed_first: tx_b.clone(),
                exec_pos_should_be_first: exec_pos_a,
                exec_pos_executed_first: exec_pos_b,
                count_correct: *count,
                total_nodes: num_nodes,
                batch_should_be_first: key_a,
                batch_executed_first: key_b,
            })
        })
        .collect();

    if violations.is_empty() {
        println!("[VALIDATE] ✓ No violations found");
    } else {
        println!("[VALIDATE] ✗ Found {} violations", violations.len());

        if verbose {
            if let Some(v) = violations.first() {
                println!("\n[DEBUG] First violation detail:");
                println!(
                    "[DEBUG]   tx_A (should be first): {}",
                    &v.tx_should_be_first[..30.min(v.tx_should_be_first.len())]
                );
                println!(
                    "[DEBUG]   tx_B (executed first):  {}",
                    &v.tx_executed_first[..30.min(v.tx_executed_first.len())]
                );
                println!(
                    "[DEBUG]   Exec positions: A@{}, B@{}",
                    v.exec_pos_should_be_first, v.exec_pos_executed_first
                );
                println!(
                    "[DEBUG]   Batch keys: A(scc={}, sub_dag={}), B(scc={}, sub_dag={})",
                    v.batch_should_be_first.0,
                    v.batch_should_be_first.1,
                    v.batch_executed_first.0,
                    v.batch_executed_first.1
                );
                println!(
                    "[DEBUG]   Count A<B across nodes: {} / {} (threshold {:.2})",
                    v.count_correct, v.total_nodes, threshold
                );
            }
        }
    }

    ValidationResult {
        is_valid: violations.is_empty(),
        num_txs,
        num_nodes,
        gamma,
        threshold,
        num_batches,
        largest_batch,
        single_tx_batches,
        violations,
        execution_converged: true, // set by caller
        validation_time_secs: start.elapsed().as_secs_f64(),
    }
}

fn main() {
    let args = Args::parse();

    if args.gamma <= 0.5 || args.gamma > 1.0 {
        eprintln!("ERROR: gamma must be in (0.5, 1.0], got {}", args.gamma);
        std::process::exit(1);
    }

    // Parse logs
    let (receive_orders, execution_order, batch_of_tx, converged) = parse_all_logs(&args.logs_dir);

    if execution_order.is_empty() {
        eprintln!("ERROR: No transactions found in execution logs");
        std::process::exit(1);
    }

    // FILTER: Remove crashed nodes (nodes with no receive orders)
    let active_receive_orders: HashMap<String, HashMap<String, u32>> = receive_orders
        .into_iter()
        .filter(|(_, orders)| !orders.is_empty())
        .collect();

    println!(
        "[FILTER] Keeping {} active nodes (n-f)",
        active_receive_orders.len()
    );

    // Validate with only active nodes
    let mut result = validate_batch_order_fairness(
        &active_receive_orders,
        &execution_order,
        &batch_of_tx,
        args.gamma,
        args.verbose,
    );
    result.execution_converged = converged;

    // Output
    let output = format!("{}", result);

    if let Some(output_path) = args.output {
        std::fs::write(&output_path, &output).expect("Failed to write output file");
        println!("\nResults written to {:?}", output_path);
    } else {
        println!("{}", output);
    }

    std::process::exit(if result.is_valid { 0 } else { 1 });
}

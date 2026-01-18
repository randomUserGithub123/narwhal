# Build
`cargo build --release` (build on server if you want to run there)

# Logging
Search `LOGGING` in `local_order_maker.rs` and `global_order.rs` and uncomment, then run benchmark.

# Run
`./target/release/validate_gamma_batch_fairness -d ./narwhal/benchmark/logs/`
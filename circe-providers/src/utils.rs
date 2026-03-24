    /// Calculates effective concurrency configuration based on parameter sets.
    ///
    /// The concurrency model balances parallelism with resource constraints:
    ///
    /// **DataFusion Partitions:**
    /// ```text
    /// num_partitions = min(num_param_sets, num_cpus)
    /// ```
    /// - Don't create more partitions than parameter sets (no empty partitions)
    /// - Don't create more partitions than CPUs (efficient CPU utilization)
    ///
    /// **Concurrency Per Partition:**
    /// ```text
    /// max_per_partition = max_concurrency / num_partitions
    /// concurrency_per_partition = max(1, min(max_per_partition, params_per_partition))
    /// ```
    /// - Minimum 1: Always process at least one query at a time
    /// - Maximum derived from cap: Ensures total doesn't exceed max_concurrency
    /// - Scales with workload: Uses actual params_per_partition when within bounds
    ///
    /// **Total Concurrent Queries:**
    /// ```text
    /// total = num_partitions × concurrency_per_partition ≤ max_concurrency
    /// ```
    ///
    /// **Examples (10 CPU system, max_concurrency=100):**
    /// - 5 param sets → 5 partitions × 1 buffered = 5 concurrent queries
    /// - 50 param sets → 10 partitions × 5 buffered = 50 concurrent queries
    /// - 200 param sets → 10 partitions × 10 buffered = 100 concurrent queries (capped)
    ///
    /// # Arguments
    /// * `num_param_sets` - Total number of parameter sets to execute
    /// * `max_concurrency` - Maximum total concurrent queries allowed
    ///
    /// # Returns
    /// * `(num_partitions, concurrency_per_partition)`
    // fn effective_concurrency(num_param_sets: usize, max_concurrency: usize) -> (usize, usize) {
    //     let num_param_sets = num_param_sets.max(1);
    //     let num_partitions = num_param_sets.min(num_cpus::get());
    //     let params_per_partition = num_param_sets / num_partitions;

    //     // Calculate max per partition to respect total concurrency cap
    //     let max_per_partition = (max_concurrency / num_partitions).max(1);
    //     let concurrency_per_partition = 1_usize.max(max_per_partition.min(params_per_partition));

    //     (num_partitions, concurrency_per_partition)
    // }

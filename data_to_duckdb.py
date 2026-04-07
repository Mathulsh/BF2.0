from rd import collect_redis_results_to_duckdb_parallel, redis_list

collect_redis_results_to_duckdb_parallel(
    # redis_host=redis_host,
    # redis_port=redis_port,
    # redis_password=redis_password,
    redis_list=redis_list,
    duckdb_path="results.duckdb",
    batch_size=5000,
    idle_timeout=30,
    stop_event=None,
    check_interval=0.05,
)

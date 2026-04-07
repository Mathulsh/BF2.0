from rd import collect_redis_results_to_duckdb, redis_list

collect_redis_results_to_duckdb(
    # redis_host=redis_host,
    # redis_port=redis_port,
    # redis_password=redis_password,
    redis_list=redis_list,
    duckdb_path="results.duckdb",
    batch_size=5000,          # ✅ 降低批量大小（很关键）
    idle_timeout=30,
)

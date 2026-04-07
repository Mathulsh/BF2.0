'''功能函数:redis读写模块'''
import redis, pandas as pd
import pickle, time, duckdb
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Redis配置——hz
REDIS_CONFIGS = [
    {"host": "10.64.199.62", "port": 41882},
    {"host": "10.64.199.65", "port": 41979},
    {"host": "10.64.199.63", "port": 41980},
    {"host": "10.64.199.30", "port": 41995},
]

# Redis配置——bj
# REDIS_CONFIGS = [
#     {"host": "172.19.123.200", "port": 40069},
#     {"host": "172.19.123.200", "port": 40071},
#     {"host": "172.19.123.200", "port": 40072},
#     {"host": "172.19.123.200", "port": 40073},
# ]

# Redis配置——本地
# REDIS_CONFIGS = [
#     {"host": "127.0.0.1", "port": 6379},
#     {"host": "127.0.0.1", "port": 6380},
#     {"host": "127.0.0.1", "port": 6381},
#     {"host": "127.0.0.1", "port": 6382},
# ]

def create_redis_clients():
    """创建Redis客户端列表，配置健康检查和超时参数"""
    clients = []
    for config in REDIS_CONFIGS:
        try:
            client = redis.Redis(
                host=config["host"],
                port=config["port"],
                decode_responses=False,
                socket_connect_timeout=10,
                socket_timeout=30,
                health_check_interval=30,
                retry_on_timeout=True,
                max_connections=50,
            )
            # 测试连接
            client.ping()
            clients.append(client)
            logger.info(f"Redis {config['host']}:{config['port']} 连接成功")
        except Exception as e:
            logger.warning(f"Redis {config['host']}:{config['port']} 连接失败: {e}")
            # 创建占位符，后续会尝试重连
            clients.append(None)
    return clients

def get_healthy_redis_clients():
    """获取健康的Redis客户端列表，自动剔除不可用的"""
    healthy = []
    for i, client in enumerate(redis_list):
        if client is None:
            # 尝试重新连接
            try:
                config = REDIS_CONFIGS[i]
                client = redis.Redis(
                    host=config["host"],
                    port=config["port"],
                    decode_responses=False,
                    socket_connect_timeout=10,
                    socket_timeout=30,
                    health_check_interval=30,
                    retry_on_timeout=True,
                )
                client.ping()
                redis_list[i] = client
                healthy.append(client)
                logger.info(f"Redis {config['host']}:{config['port']} 重连成功")
            except Exception as e:
                pass
        else:
            try:
                client.ping()
                healthy.append(client)
            except Exception:
                logger.warning(f"Redis 实例 {i} 不健康，暂时剔除")
                redis_list[i] = None
    return healthy if healthy else []

# 初始化Redis实例
redis_list = create_redis_clients()
idx = 0

def push_to_redis(combs):
    pipes = [r.pipeline() for r in redis_list]

    for cb in combs:
        task = {"features": list(cb)}

        i = hash(cb) % len(redis_list)
        pipes[i].rpush("mylist", pickle.dumps(task))

    for pipe in pipes:
        pipe.execute()

def read_one_from_redis(timeout=None) -> tuple[list[list[str]] | None, bytes | None, Exception | None, redis.Redis | None]:
    """从多个 Redis 轮询读取任务（真正负载均衡版，带故障转移）
    
    Args:
        timeout: 超时时间（秒），None表示永不超时
    """
    global idx
    start_time = time.time()
    
    while True:
        # 检查超时
        if timeout is not None and time.time() - start_time > timeout:
            return None, None, Exception("队列为空"), None
        
        # 获取健康的Redis客户端
        healthy_clients = get_healthy_redis_clients()
        if not healthy_clients:
            logger.error("所有 Redis 实例均不可用，等待重试...")
            time.sleep(1)
            continue
        
        n = len(healthy_clients)
        
        # 🎯 从当前 idx 开始轮询
        for i in range(n):
            r = healthy_clients[(idx + i) % n]

            try:
                raw_item = r.lmove(
                    "mylist",
                    "mylist:processing",
                    "LEFT",
                    "RIGHT",
                )
                if raw_item is None:
                    continue

                # ✅ 更新轮转起点（关键）
                idx = (idx + i + 1) % n

                # ---------- bytes处理 ----------
                if isinstance(raw_item, (bytes, bytearray)):
                    data_bytes = raw_item
                elif isinstance(raw_item, str):
                    data_bytes = raw_item.encode()
                else:
                    raise TypeError(f"Unexpected data type from redis: {type(raw_item)!r}")


                data = pickle.loads(data_bytes)

                # ---------- batch兼容 ----------
                # 保持特征为整数（与DataFrame列名匹配）
                if isinstance(data, list):
                    tasks = [list(d["features"]) for d in data]
                else:
                    tasks = [list(data["features"])]

                return tasks, data_bytes, None, r

            except (redis.ConnectionError, redis.TimeoutError) as e:
                logger.warning(f"Redis 连接错误，将尝试其他实例: {e}")
                continue
            except Exception as e:
                return None, None, e, None

        # ❗所有 Redis 都空 → sleep
        time.sleep(0.01)
        
def push_result_to_redis(
    *,
    r: redis.Redis | None = None,
    result_data: dict | None,
    raw_task: bytes | None,
    max_retries: int = 3,
) -> bool:
    """
    推送结果到Redis，支持自动重试和故障转移
    
    Returns:
        bool: 是否成功推送
    """
    if result_data is None or raw_task is None:
        return False

    payload = pickle.dumps(result_data, protocol=pickle.HIGHEST_PROTOCOL)

    # 准备ACK值
    if isinstance(raw_task, memoryview):
        value = raw_task.tobytes()
    elif isinstance(raw_task, (bytes, bytearray)):
        value = bytes(raw_task)
    elif isinstance(raw_task, str):
        value = raw_task.encode()
    else:
        raise TypeError(f"Unexpected raw_task type: {type(raw_task)!r}")

    # 🎯 结果写入：使用指定Redis或负载均衡
    if r is not None:
        # 优先尝试原Redis，失败则切换到其他健康实例
        candidates = [r] + [c for c in redis_list if c is not None and c != r]
    else:
        candidates = [c for c in redis_list if c is not None]

    for attempt in range(max_retries):
        for target_redis in candidates:
            if target_redis is None:
                continue
            try:
                # 1️⃣ 写结果
                pipe = target_redis.pipeline(transaction=True)
                pipe.rpush("results", payload)

                # 2️⃣ ACK - 从processing队列移除
                pipe.lrem("mylist:processing", 1, value) # type: ignore
                
                # 3️⃣ 执行
                pipe.execute()
                return True
                
            except (redis.ConnectionError, redis.TimeoutError) as e:
                logger.warning(f"Redis 写入失败(尝试 {attempt+1}/{max_retries}): {e}")
                time.sleep(0.5 * (attempt + 1))  # 指数退避
                continue
            except Exception as e:
                logger.error(f"Redis 写入未知错误: {e}")
                raise
    
    logger.error(f"所有 Redis 实例写入失败，结果可能丢失: {result_data}")
    return False


def safe_ack_processing(r: redis.Redis | None, raw_task: bytes | None, max_retries: int = 3) -> bool:
    """
    安全地从processing队列移除任务，带重试机制
    用于异常处理时清理队列，避免任务卡住
    
    Returns:
        bool: 是否成功
    """
    if raw_task is None:
        return True
    
    # 准备值
    if isinstance(raw_task, memoryview):
        value = raw_task.tobytes()
    elif isinstance(raw_task, (bytes, bytearray)):
        value = bytes(raw_task)
    elif isinstance(raw_task, str):
        value = raw_task.encode()
    else:
        logger.error(f"未知的raw_task类型: {type(raw_task)}")
        return False
    
    # 候选Redis实例
    if r is not None:
        candidates = [r] + [c for c in redis_list if c is not None and c != r]
    else:
        candidates = [c for c in redis_list if c is not None]
    
    for attempt in range(max_retries):
        for target_redis in candidates:
            if target_redis is None:
                continue
            try:
                target_redis.lrem("mylist:processing", 1, value) # type: ignore
                return True
            except (redis.ConnectionError, redis.TimeoutError) as e:
                logger.warning(f"ACK失败(尝试 {attempt+1}/{max_retries}): {e}")
                time.sleep(0.3 * (attempt + 1))
                continue
            except Exception as e:
                logger.error(f"ACK未知错误: {e}")
                return False
    
    logger.error("无法从processing队列移除任务，任务可能滞留")
    return False
    
def collect_redis_results_to_duckdb(
    redis_list,
    duckdb_path="results.duckdb",
    batch_size=5000,          # ✅ 降低批量大小（很关键）
    idle_timeout=30,          # ✅ 空闲多久才退出（秒）
):
    con = duckdb.connect(duckdb_path)

    con.execute("""
        CREATE TABLE IF NOT EXISTS results (
            features INTEGER[],
            mean_f1_macro DOUBLE,
            mean_accuracy DOUBLE,
        )
    """)

    print("🚀 Fast collector started")

    total_count = 0
    t_start = time.time()
    last_data_time = time.time()   # ✅ 记录最后一次收到数据时间

    while True:
        any_data = False

        for r in redis_list:
            pipe = r.pipeline(transaction=False)

            for _ in range(batch_size):
                pipe.rpop("results")

            items = pipe.execute()
            items = [x for x in items if x is not None]

            if not items:
                continue

            any_data = True
            last_data_time = time.time()   # ✅ 更新“活跃时间”

            rows = []
            for item in items:
                data = pickle.loads(item)
                rows.append((data["features"], float(data["mean_f1_macro"]), float(data["mean_accuracy"])))

            n = len(rows)
            total_count += n

            df = pd.DataFrame(rows, columns=["features", "mean_f1_macro", "mean_accuracy"])
            con.register("tmp_df", df)
            con.execute("INSERT INTO results SELECT * FROM tmp_df")
            con.unregister("tmp_df")

            elapsed = time.time() - t_start
            print(f"  +{n:<7,} → 累计 {total_count:>12,} 条 | {elapsed:6.1f}s", flush=True)

        # ✅ 核心退出逻辑：长时间没有新数据才退出
        if not any_data:
            idle_time = time.time() - last_data_time
            if idle_time > idle_timeout:
                print(f"⏹️ 空闲 {idle_time:.1f}s，停止收集")
                break
            time.sleep(0.2)

    elapsed = time.time() - t_start
    print(f"\n✅ 收集完成 | 总计 {total_count:,} 条 | 耗时 {elapsed:.1f}s")
    
    # 🎯 Top10 结果（按 mean_f1_macro 排序）
    print("\n🏆 Top 10 results:")
    top10 = con.execute("""
        SELECT *
        FROM results
        ORDER BY mean_f1_macro DESC
        LIMIT 10
    """).fetchall()
    for i, (features, score) in enumerate(top10, 1):
        print(f"{i:>2}. score={score:.2f} | features={features}")
        
    # 🎯 Top10 结果（按 mean_accuracy 排序）
    print("\n🏆 Top 10 results:")
    top10 = con.execute("""
        SELECT *
        FROM results
        ORDER BY mean_accuracy DESC
        LIMIT 10
    """).fetchall()
    for i, (features, _, accuracy) in enumerate(top10, 1):
        print(f"{i:>2}. accuracy={accuracy:.2f} | features={features}")


# ==================== 并行收集优化版 ====================

def collect_redis_results_to_duckdb_parallel(
    redis_list,
    duckdb_path="results.duckdb",
    batch_size=5000,
    idle_timeout=30,
    stop_event=None,
    check_interval=0.05,
):
    """
    并行收集结果到 DuckDB - 优化版（支持created_at字段）
    
    Args:
        redis_list: Redis 客户端列表
        duckdb_path: DuckDB 数据库路径
        batch_size: 每批处理大小
        idle_timeout: 空闲超时时间（秒）
        stop_event: 多进程事件，用于通知收集器停止
        check_interval: 检查 stop_event 的间隔（秒）
    """
    from datetime import datetime
    
    con = duckdb.connect(duckdb_path)

    # 检查表结构，如果不匹配则重建
    try:
        con.execute("SELECT created_at FROM results LIMIT 1")
        has_created_at = True
    except:
        has_created_at = False
    
    if not has_created_at:
        # 尝试添加列，如果失败则删除重建
        try:
            con.execute("ALTER TABLE results ADD COLUMN created_at TIMESTAMP")
            print("✅ 已添加 created_at 列")
        except:
            # 删除旧表重建
            con.execute("DROP TABLE IF EXISTS results")
            con.execute("""
                CREATE TABLE results (
                    features INTEGER[],
                    mean_f1_macro DOUBLE,
                    mean_accuracy DOUBLE,
                    created_at TIMESTAMP
                )
            """)
            print("✅ 已重建表结构（4列）")
    
    # 创建索引加速查询（如果不存在）
    try:
        con.execute("CREATE INDEX IF NOT EXISTS idx_f1 ON results(mean_f1_macro DESC)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_acc ON results(mean_accuracy DESC)")
    except Exception:
        pass

    print("🚀 Fast collector started (parallel mode)")

    total_count = 0
    t_start = time.time()
    last_data_time = time.time()
    
    # 使用 executemany 批量插入，比 DataFrame 方式更快
    insert_buffer = []
    max_buffer = 10000  # 最大缓冲大小

    while True:
        any_data = False
        
        # 检查是否需要停止
        if stop_event is not None and stop_event.is_set():
            # 先处理完缓冲区再退出
            if insert_buffer:
                con.executemany("INSERT INTO results VALUES (?, ?, ?, ?)", insert_buffer)
                total_count += len(insert_buffer)
            print(f"🛑 收到停止信号，收集器退出 | 总计 {total_count:,} 条")
            break

        for r in redis_list:
            # 快速批量弹出 - 使用 pipeline 减少网络往返
            pipe = r.pipeline(transaction=False)
            for _ in range(batch_size):
                pipe.rpop("results")
            items = pipe.execute()
            items = [x for x in items if x is not None]

            if not items:
                continue

            any_data = True
            last_data_time = time.time()

            # 批量反序列化并入缓冲（添加created_at）
            now = datetime.now()
            for item in items:
                data = pickle.loads(item)
                insert_buffer.append((
                    data["features"], 
                    float(data["mean_f1_macro"]), 
                    float(data["mean_accuracy"]),
                    now
                ))
            
            buffer_size = len(insert_buffer)
            
            # 缓冲满则批量写入
            if buffer_size >= max_buffer:
                con.executemany("INSERT INTO results VALUES (?, ?, ?, ?)", insert_buffer)
                total_count += buffer_size
                insert_buffer = []
                
                elapsed = time.time() - t_start
                print(f"  +{buffer_size:<7,} → 累计 {total_count:>12,} 条 | {elapsed:6.1f}s", flush=True)

        # 处理剩余缓冲数据（定期写入，避免数据滞留内存太久）
        if insert_buffer and (any_data or (time.time() - last_data_time > 1)):
            con.executemany("INSERT INTO results VALUES (?, ?, ?, ?)", insert_buffer)
            total_count += len(insert_buffer)
            insert_buffer = []

        # 退出逻辑
        if not any_data:
            idle_time = time.time() - last_data_time
            
            # 如果收到停止信号且空闲，退出
            if stop_event is not None and stop_event.is_set():
                if insert_buffer:
                    con.executemany("INSERT INTO results VALUES (?, ?, ?, ?)", insert_buffer)
                    total_count += len(insert_buffer)
                print(f"🛑 收到停止信号，空闲 {idle_time:.1f}s，停止收集")
                break
            
            # 长时间空闲退出
            if idle_time > idle_timeout:
                if insert_buffer:
                    con.executemany("INSERT INTO results VALUES (?, ?, ?)", insert_buffer)
                    total_count += len(insert_buffer)
                print(f"⏹️ 空闲 {idle_time:.1f}s，停止收集")
                break
                
            time.sleep(check_interval)

    elapsed = time.time() - t_start
    print(f"\n✅ 收集完成 | 总计 {total_count:,} 条 | 耗时 {elapsed:.1f}s")
    
    # 🎯 Top10 结果
    try:
        print("\n🏆 Top 10 results (by F1-macro):")
        top10 = con.execute("""
            SELECT features, mean_f1_macro
            FROM results
            ORDER BY mean_f1_macro DESC
            LIMIT 10
        """).fetchall()
        for i, (features, score) in enumerate(top10, 1):
            print(f"{i:>2}. score={score:.2f} | features={features}")
            
        print("\n🏆 Top 10 results (by Accuracy):")
        top10 = con.execute("""
            SELECT features, mean_accuracy
            FROM results
            ORDER BY mean_accuracy DESC
            LIMIT 10
        """).fetchall()
        for i, (features, accuracy) in enumerate(top10, 1):
            print(f"{i:>2}. accuracy={accuracy:.2f} | features={features}")
    except Exception as e:
        print(f"查询Top10失败: {e}")


# ==================== 批量读取优化 ====================

def read_batch_from_redis(batch_size=10, timeout=None) -> tuple[list[list[str]] | None, list[bytes] | None, Exception | None, redis.Redis | None]:
    """
    批量从 Redis 读取任务（提速版）
    
    Args:
        batch_size: 批量读取数量
        timeout: 超时时间（秒），None表示永不超时
    
    Returns:
        - tasks: 任务列表（每个任务是特征列表）
        - raw_items: 原始字节列表（用于ACK）
        - error: 错误信息（超时返回 Exception("队列为空")）
        - redis_client: Redis 客户端（用于ACK）
    """
    global idx
    start_time = time.time()
    
    while True:
        # 检查超时
        if timeout is not None and time.time() - start_time > timeout:
            return None, None, Exception("队列为空"), None
        
        healthy_clients = get_healthy_redis_clients()
        if not healthy_clients:
            logger.error("所有 Redis 实例均不可用，等待重试...")
            time.sleep(1)
            continue
        
        n = len(healthy_clients)
        
        for i in range(n):
            r = healthy_clients[(idx + i) % n]
            
            try:
                # 使用 pipeline 批量读取
                pipe = r.pipeline()
                for _ in range(batch_size):
                    pipe.lmove("mylist", "mylist:processing", "LEFT", "RIGHT")
                
                raw_items = pipe.execute()
                raw_items = [x for x in raw_items if x is not None]
                
                if not raw_items:
                    continue
                
                # 更新轮转起点
                idx = (idx + i + 1) % n
                
                # 批量处理
                tasks = []
                valid_raw_items = []
                
                for raw_item in raw_items:
                    # bytes处理
                    if isinstance(raw_item, (bytes, bytearray)):
                        data_bytes = raw_item
                    elif isinstance(raw_item, str):
                        data_bytes = raw_item.encode()
                    else:
                        continue
                    
                    try:
                        data = pickle.loads(data_bytes)
                        if isinstance(data, list):
                            # 保持特征为整数（与DataFrame列名匹配）
                            tasks.extend([list(d["features"]) for d in data])
                        else:
                            tasks.append(list(data["features"]))
                        valid_raw_items.append(data_bytes)
                    except Exception as e:
                        logger.warning(f"解析任务失败: {e}")
                        continue
                
                if tasks:
                    return tasks, valid_raw_items, None, r
                
            except (redis.ConnectionError, redis.TimeoutError) as e:
                logger.warning(f"Redis 连接错误: {e}")
                continue
            except Exception as e:
                return None, None, e, None
        
        # 所有 Redis 都空 → sleep
        time.sleep(0.01)


def push_result_to_redis_batch(
    r: redis.Redis | None,
    results_data: list[dict],
    raw_tasks: list[bytes],
    max_retries: int = 3,
) -> bool:
    """
    批量推送结果到 Redis
    
    Args:
        r: Redis 客户端
        results_data: 结果数据列表
        raw_tasks: 原始任务字节列表（用于ACK）
        max_retries: 最大重试次数
    
    Returns:
        bool: 是否成功
    """
    if not results_data or not raw_tasks or len(results_data) != len(raw_tasks):
        return False
    
    # 准备ACK值
    ack_values = []
    for raw_task in raw_tasks:
        if isinstance(raw_task, memoryview):
            ack_values.append(raw_task.tobytes())
        elif isinstance(raw_task, (bytes, bytearray)):
            ack_values.append(bytes(raw_task))
        elif isinstance(raw_task, str):
            ack_values.append(raw_task.encode())
        else:
            ack_values.append(None)
    
    # 候选Redis实例
    if r is not None:
        candidates = [r] + [c for c in redis_list if c is not None and c != r]
    else:
        candidates = [c for c in redis_list if c is not None]
    
    for attempt in range(max_retries):
        for target_redis in candidates:
            if target_redis is None:
                continue
            try:
                pipe = target_redis.pipeline(transaction=True)
                
                # 批量写结果
                for result_data in results_data:
                    payload = pickle.dumps(result_data, protocol=pickle.HIGHEST_PROTOCOL)
                    pipe.rpush("results", payload)
                
                # 批量ACK
                for ack_val in ack_values:
                    if ack_val is not None:
                        pipe.lrem("mylist:processing", 1, ack_val)
                
                pipe.execute()
                return True
                
            except (redis.ConnectionError, redis.TimeoutError) as e:
                logger.warning(f"Redis 批量写入失败(尝试 {attempt+1}/{max_retries}): {e}")
                time.sleep(0.5 * (attempt + 1))
                continue
            except Exception as e:
                logger.error(f"Redis 批量写入未知错误: {e}")
                raise
    
    logger.error(f"所有 Redis 实例批量写入失败")
    return False

"""结合que_push.py, train.py & data_to_duckdb.py写成一个main.py主运行脚本
潜在风险：redis中任务堆积过多，内存溢出(超1TB时)"""
import time
import pickle
import numpy as np
import os
import sys
import signal
import duckdb
from datetime import datetime
from itertools import combinations, islice
from multiprocessing import Process, Event
from sklearn.ensemble import RandomForestClassifier
from math import comb as math_comb
from sklearn.model_selection import cross_validate, StratifiedKFold
from rd import (
    push_to_redis, 
    read_batch_from_redis,
    push_result_to_redis_batch,
    safe_ack_processing,
    redis_list, 
    collect_redis_results_to_duckdb_parallel,
    get_healthy_redis_clients,
)

# ========== 配置 ==========
DB_PATH = "results.duckdb"
TABLE_NAME = "results"

# 全局变量用于控制程序运行状态
should_stop = False
current_batch_num = 0

def signal_handler(signum, frame):
    """处理中断信号"""
    global should_stop, current_batch_num
    print(f"\n收到中断信号，正在保存进度...")
    should_stop = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def batch_generator(iterable, batch_size):
    """分割可迭代对象为指定大小的批次避免内存溢出"""
    iterator = iter(iterable)
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            break
        yield batch

def push_combinations_to_redis():
    """推送数据到Redis的运行脚本"""
    completed_flag_file = "push_completed.txt"
    if os.path.exists(completed_flag_file):
        print("检测到推送已完成，跳过推送步骤。")
        return
    
    print("Starting to push combinations to Redis...")
    time_start = time.time()

    progress_file = "push_progress.txt"
    start_batch_index = 0
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            try:
                last_completed_batch = int(f.read().strip())
                start_batch_index = last_completed_batch
                print(f"检测到上次推送进度，从第 {start_batch_index + 1} 批次开始推送...")
            except ValueError:
                start_batch_index = 0
                print("进度文件格式错误，重新开始...")
    else:
        print("开始新的推送任务...")

    # 特征范围1-98（与DataFrame列0-98匹配，排除0和99）
    whole_numbers: list[int] = list(range(1, 99))  # 1-98
    total = math_comb(len(whole_numbers), 4)
    comb = combinations(whole_numbers, 4)
    print(f"总组合数: {total:,} (C({len(whole_numbers)}, 4) = C(98, 4))")
    batch_size = 50000
    
    def batch_generator_with_start(iterable, batch_size, start_index=0):
        iterator = iter(iterable)
        for _ in range(start_index):
            batch = list(islice(iterator, batch_size))
            if not batch:
                break
        
        current_index = start_index
        while True:
            batch = list(islice(iterator, batch_size))
            if not batch:
                break
            yield current_index, batch
            current_index += 1
    
    for i, batch in batch_generator_with_start(comb, batch_size, start_batch_index):
        if should_stop:
            print(f"程序被中断，批次 {i + 1} 未完成，进度已保存")
            with open(progress_file, 'w') as f:
                f.write(str(i))
            sys.exit(0)
                
        current_batch_num = i + 1
        print(f"Pushing batch {i + 1} with {len(batch)} combinations...")
        push_to_redis(batch)
        print(f"Batch {i + 1} completed")
        
        with open(progress_file, 'w') as f:
            f.write(str(i + 1))

    if os.path.exists(progress_file):
        os.remove(progress_file)
    
    with open(completed_flag_file, "w") as f:
        f.write("completed")
        
    print("All combinations have been pushed to Redis!")
    time_end = time.time()
    print(f"Time cost for pushing to Redis: {time_end - time_start} seconds")    


# ========== 数据库初始化（仅用于collector） ==========
def init_db():
    """初始化DuckDB数据库"""
    import time
    max_retries = 5
    for i in range(max_retries):
        try:
            con = duckdb.connect(DB_PATH)
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    features INTEGER[],
                    mean_f1_macro DOUBLE,
                    mean_accuracy DOUBLE,
                    created_at TIMESTAMP
                )
            """)
            con.close()
            return True
        except Exception as e:
            if "Conflicting lock" in str(e):
                print(f"⚠️ 数据库被锁定，等待重试... ({i+1}/{max_retries})")
                print(f"   提示: 可运行 'lsof {DB_PATH}' 查看占用进程，或删除 {DB_PATH}")
                time.sleep(2)
            else:
                raise
    return False

# ========== 同步写入数据库（多进程时禁用，由collector处理） ==========
def sync_write_to_db(results_batch):
    """workers不直接写DB，由collector统一处理"""
    pass


# ========== 实时打印统计 ==========
def print_stats(worker_id, task_count, result_data, elapsed):
    """实时打印任务统计"""
    features = result_data["features"]
    f1 = result_data["mean_f1_macro"]
    acc = result_data["mean_accuracy"]
    
    print(f"📝 Worker[{worker_id}] Task[{task_count}] Features={features} | "
          f"F1={f1:.2f} Acc={acc:.2f} | Time={elapsed:.1f}s")


# ========== 训练模型（Batch模式） ==========
def train_models(worker_id=0, batch_size=5):
    """训练模型 - 批量读取模式（提速版）"""
    print(f"Worker {worker_id}: Starting training process (batch mode, batch_size={batch_size})...")

    data = pickle.load(open("data_99_3cls_train.pkl", "rb"))
    y = data.values[:, -1]
    
    cv = list(StratifiedKFold(n_splits=5, shuffle=True, random_state=42).split(np.zeros(len(y)), y))
    scoring = ["f1_macro", "accuracy"]
    model = RandomForestClassifier(random_state=0, n_jobs=1)
    
    task_count = 0
    empty_count = 0
    db_write_count = 0
    time_start = time.time()
    
    while True:
        # 批量读取任务
        tasks, raw_tasks, err, source_redis = read_batch_from_redis(batch_size=batch_size, timeout=5)

        if err is not None:
            healthy = get_healthy_redis_clients()
            if not healthy:
                print(f"Worker {worker_id}: 所有Redis不可用，训练进程退出")
                break
            
            total_mylist = sum(r.llen("mylist") for r in redis_list if r)
            total_processing = sum(r.llen("mylist:processing") for r in redis_list if r)
            
            if total_mylist == 0 and total_processing == 0:
                empty_count += 1
                if empty_count >= 3:
                    elapsed = time.time() - time_start
                    print(f"Worker {worker_id}: ✅ 队列为空且无处理中任务，退出 | 处理 {task_count} 个任务 | 耗时 {elapsed:.1f}s")
                    break
            else:
                empty_count = 0
            continue
            
        if source_redis is None or tasks is None or raw_tasks is None:
            continue
        
        empty_count = 0

        # 批量处理结果
        results_batch = []
        valid_raw_tasks = []
        
        for i, task in enumerate(tasks):
            try:
                # 检查特征是否存在
                missing_cols = [c for c in task if c not in data.columns]
                if missing_cols:
                    print(f"Worker {worker_id}: ⚠️ 跳过任务，缺失列: {missing_cols}")
                    if i < len(raw_tasks):
                        safe_ack_processing(r=source_redis, raw_task=raw_tasks[i])
                    continue
                
                # 计算
                X = data.loc[:, task].values
                scores = cross_validate(model, X, y, cv=cv, scoring=scoring, n_jobs=1)
                
                result_data = {
                    "features": task,
                    "mean_f1_macro": float(scores["test_f1_macro"].mean().round(2)),
                    "mean_accuracy": float(scores["test_accuracy"].mean().round(2)),
                }
                
                results_batch.append(result_data)
                valid_raw_tasks.append(raw_tasks[i])
                
                # 实时打印
                task_count += 1
                elapsed = time.time() - time_start
                print_stats(worker_id, task_count, result_data, elapsed)

            except Exception as e:
                print(f"Worker {worker_id}: ❌ 任务处理错误 {task}: {e}")
                if i < len(raw_tasks):
                    safe_ack_processing(r=source_redis, raw_task=raw_tasks[i])
                continue
        
        # 批量推送结果到Redis
        if results_batch:
            push_result_to_redis_batch(
                r=source_redis,
                results_data=results_batch,
                raw_tasks=valid_raw_tasks,
            )
            
            # 同步写入数据库
            sync_write_to_db(results_batch)
            db_write_count += len(results_batch)
            
            if db_write_count % 100 == 0:
                print(f"Worker {worker_id}: 📤 已推送 {db_write_count} 条到Redis (由collector写入DB)")


def collect_results_parallel(stop_event):
    """并行收集结果到 DuckDB"""
    print("Starting parallel collector...")
    # 初始化数据库
    if not init_db():
        print("❌ Collector 无法初始化数据库")
        return
    print(f"✅ Collector 数据库初始化完成: {DB_PATH}")
    
    collect_redis_results_to_duckdb_parallel(
        redis_list=redis_list,
        duckdb_path=DB_PATH,
        stop_event=stop_event,
    )


def main():
    """Main function to orchestrate all processes"""
    print("let's go BF!")
    time_start = time.time()
    
    # 参数解析
    num_workers = 1
    task_batch_size = 5
    
    if len(sys.argv) > 1:
        try:
            num_workers = int(sys.argv[1])
        except ValueError:
            pass
    
    if len(sys.argv) > 2:
        try:
            task_batch_size = int(sys.argv[2])
        except ValueError:
            pass
    
    print(f"启动 {num_workers} 个 worker (batch_size={task_batch_size})")

    # Step 0: 检查Redis健康状态
    healthy_count = len(get_healthy_redis_clients())
    if healthy_count == 0:
        print("❌ 错误: 所有 Redis 实例均不可用")
        sys.exit(1)
    print(f"✅ Redis 健康检查通过，{healthy_count}/{len(redis_list)} 个实例可用")
    
    # Step 1: Push combinations to Redis
    push_combinations_to_redis()
    
    # Step 2: 启动训练 workers 和并行收集器
    stop_event = Event()
    
    # 启动收集器进程（与训练并行）- 作为备用，主要写入由worker完成
    collector_process = Process(target=collect_results_parallel, args=(stop_event,))
    collector_process.start()
    print(f"Collector started, pid={collector_process.pid}")
    
    # 启动训练 workers
    workers = []
    for i in range(num_workers):
        p = Process(target=train_models, args=(i, task_batch_size))
        p.start()
        workers.append(p)
        print(f"Worker {i} started, pid={p.pid}")

    # 等待所有 worker 完成
    for p in workers:
        p.join()
    
    time_end = time.time()
    print(f"Time cost for training: {time_end - time_start} seconds")
    
    # 通知收集器停止
    print("All workers finished, signaling collector to stop...")
    stop_event.set()
    
    # 等待收集器完成
    collector_process.join(timeout=120)
    if collector_process.is_alive():
        print("Collector still running, terminating...")
        collector_process.terminate()
        collector_process.join()
    
    print("All done!")
    
if __name__ == "__main__":
    main()

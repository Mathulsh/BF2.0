"""训练所有结果的运行脚本 - Batch多进程模式

使用方法:
    python train.py           # 默认: 1个worker, batch_size=5
    python train.py 10        # 10个worker, batch_size=5
    python train.py 10 5      # 10个worker, batch_size=5
    
环境变量:
    DB_PATH: DuckDB数据库路径 (默认: results.duckdb)
"""
import sys
import time
import pickle
import signal
from datetime import datetime
from multiprocessing import Process, Event

import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import StratifiedKFold, cross_validate

from rd import (
    read_batch_from_redis, 
    push_result_to_redis_batch, 
    safe_ack_processing, 
    redis_list,
    get_healthy_redis_clients,
)
from pandas import DataFrame

# ========== 配置 ==========
DB_PATH = "results.duckdb"
TABLE_NAME = "results"

# 全局控制变量
should_stop = False

def signal_handler(signum, frame):
    """处理中断信号"""
    global should_stop
    print(f"\n收到中断信号，正在停止...")
    should_stop = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ========== Worker 训练函数 ==========
def train_worker(worker_id=0, batch_size=5):
    """单个Worker的训练循环"""
    print(f"Worker {worker_id}: Starting training process (batch_size={batch_size})...")
    
    # 加载数据
    data: DataFrame = pickle.load(open("./data_99_3cls_train.pkl", "rb"))
    y = data.values[:, -1]
    
    # 预先生成CV划分
    cv = list(StratifiedKFold(n_splits=5, shuffle=True, random_state=42).split(np.zeros(len(y)), y))
    scoring = ["f1_macro", "accuracy"]
    model = RandomForestClassifier(random_state=0, n_jobs=1)
    
    task_count = 0
    empty_count = 0
    time_start = time.time()
    
    while not should_stop:
        # 批量读取任务
        tasks, raw_tasks, err, r = read_batch_from_redis(batch_size=batch_size, timeout=5)
        
        if err is not None:
            # 检查是否真的没任务了
            total_mylist = sum(r.llen("mylist") for r in redis_list if r)
            total_processing = sum(r.llen("mylist:processing") for r in redis_list if r)
            
            if total_mylist == 0 and total_processing == 0:
                empty_count += 1
                if empty_count >= 3:
                    elapsed = time.time() - time_start
                    print(f"Worker {worker_id}: ✅ 完成 | 处理 {task_count} 个任务 | 耗时 {elapsed:.1f}s")
                    break
            else:
                empty_count = 0
            continue
        
        if tasks is None or raw_tasks is None or r is None:
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
                        safe_ack_processing(r=r, raw_task=raw_tasks[i])
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
                features = result_data["features"]
                f1 = result_data["mean_f1_macro"]
                acc = result_data["mean_accuracy"]
                
                print(f"📝 Worker[{worker_id}] Task[{task_count}] Features={features} | "
                      f"F1={f1:.2f} Acc={acc:.2f} | Time={elapsed:.1f}s")
                
            except Exception as e:
                print(f"Worker {worker_id}: ❌ 任务处理错误 {task}: {e}")
                if i < len(raw_tasks):
                    safe_ack_processing(r=r, raw_task=raw_tasks[i])
                continue
        
        # 批量推送结果到Redis
        if results_batch:
            push_result_to_redis_batch(
                r=r,
                results_data=results_batch,
                raw_tasks=valid_raw_tasks,
            )
            
            if task_count % 100 == 0:
                print(f"Worker {worker_id}: 📤 已推送 {task_count} 条到Redis")

# ========== 主函数 ==========
def main():
    """主函数：解析参数并启动workers和collector"""
    print("🚀 BF Training System - Batch Multi-Process Mode")
    print("=" * 60)
    
    # 参数解析
    num_workers = 1
    batch_size = 5
    
    if len(sys.argv) > 1:
        try:
            num_workers = int(sys.argv[1])
        except ValueError:
            pass
    
    if len(sys.argv) > 2:
        try:
            batch_size = int(sys.argv[2])
        except ValueError:
            pass
    
    print(f"配置: {num_workers} workers, batch_size={batch_size}")
    
    # 检查Redis健康
    healthy_count = len(get_healthy_redis_clients())
    if healthy_count == 0:
        print("❌ 错误: 所有 Redis 实例均不可用")
        sys.exit(1)
    print(f"✅ Redis 健康: {healthy_count}/{len(redis_list)} 个实例可用")
    
    # 启动时间
    time_start = time.time()
    
    # 创建停止事件
    stop_event = Event()
    
    # 启动Worker进程
    workers = []
    for i in range(num_workers):
        p = Process(target=train_worker, args=(i, batch_size))
        p.start()
        workers.append(p)
        print(f"Worker {i} started, pid={p.pid}")
    
    print("=" * 60)
    print("所有进程已启动，正在训练...")
    print("按 Ctrl+C 停止")
    print("=" * 60)
    
    # 等待所有Worker完成
    for p in workers:
        p.join()
    
    time_end = time.time()
    print(f"\n⏱️ 训练总耗时: {time_end - time_start:.1f} 秒")
        
    print("✅ 训练完成!")


if __name__ == "__main__":
    main()

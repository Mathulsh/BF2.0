# BF2 - 分布式特征组合计算系统

高性能分布式机器学习特征组合计算与评估系统，支持多Redis实例负载均衡、多进程并行计算、实时结果收集。

## 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                        BF2 系统架构                           │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐   │
│  │  Redis 0    │     │  Redis 1    │     │  Redis 2    │   │
│  └──────┬──────┘     └──────┬──────┘     └──────┬──────┘   │
│         │                   │                   │          │
│         └───────────────────┼───────────────────┘          │
│                             │                              │
│                    ┌────────┴────────┐                     │
│                    │   任务队列调度     │                    │
│                    │  (轮询+故障转移)   │                    │
│                    └────────┬────────┘                     │
│                             │                              │
│  ┌──────────────────────────┼──────────────────────────┐  │
│  │                          │                          │  │
│  │  ┌─────────────────┐     │     ┌─────────────────┐  │  │
│  │  │   Worker 0      │     │     │   Worker 1      │  │  │
│  │  │  (训练进程)      │◄────┴────►│  (训练进程)      │  │  │
│  │  │  batch计算      │           │  batch计算      │  │  │
│  │  │  推送Redis      │           │  推送Redis      │  │  │
│  │  └─────────────────┘           └─────────────────┘  │  │
│  │           .                           .              │  │
│  │           .                           .              │  │
│  │  ┌─────────────────┐                 .              │  │
│  │  │   Worker N      │                                │  │
│  │  │  (训练进程)      │                                │  │
│  │  └─────────────────┘                                │  │
│  │           │                                          │  │
│  │           └──────────────────┐                       │  │
│  │                              ▼                       │  │
│  │                    ┌─────────────────┐              │  │
│  │                    │   Collector     │              │  │
│  │                    │   (收集进程)     │              │  │
│  │                    │  Redis → DuckDB │              │  │
│  │                    └────────┬────────┘              │  │
│  └─────────────────────────────┼────────────────────────┘  │
│                                ▼                           │
│                      ┌─────────────────┐                   │
│                      │   results.duckdb │                   │
│                      │   (结果数据库)   │                   │
│                      └─────────────────┘                   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 核心模块

| 文件 | 功能说明 |
|------|---------|
| `rd3.py` | Redis操作核心模块，包含批量读写、故障转移、结果收集 |
| `main.py` | 主运行脚本（推送+训练+收集完整流程） |
| `train.py` | 独立训练脚本（多Worker模式） |
| `data_to_duckdb.py` | 独立收集脚本 |

### Redis数据流

```
┌──────────┐     lmove      ┌──────────────┐     计算完成      ┌──────────┐
│  mylist  │ ─────────────► │ mylist:proc  │ ───────────────► │ results  │
│  (任务)  │   LEFT→RIGHT   │  (处理中)    │   推送结果       │ (结果)   │
└──────────┘                └──────────────┘                  └──────────┘
                                   │
                                   ▼ ACK
                            ┌──────────────┐
                            │  删除任务    │
                            └──────────────┘
```

## 快速开始

### 1. 环境准备

```bash
# 进入项目目录
cd BF2

# 激活虚拟环境
source .venv/bin/activate  # Linux/Mac
# 或
.venv\Scripts\activate     # Windows
```

### 2. 运行完整流程

```bash
# 方式一：使用 main.py（推送+训练+收集）
python main.py 10 5

# 参数说明：
#   4  - Worker进程数
#   5  - 批量读取大小(batch_size)
```

### 3. 仅运行训练（多Worker模式）

```bash
# 方式二：使用 train.py（仅训练+收集）
python train.py 10 5

# 参数说明：
#   10 - Worker进程数
#   5  - 批量读取大小(batch_size)
```

### 4. 独立运行收集器

```bash
# 从Redis收集结果到DuckDB
python -c "from rd3 import collect_redis_results_to_duckdb_parallel, redis_list; collect_redis_results_to_duckdb_parallel(redis_list)"
```

## 使用说明

### 参数说明

| 参数 | 默认值 | 说明 |
|-----|--------|------|
| `num_workers` | 1 | Worker进程数，建议设置为CPU核心数 |
| `batch_size` | 5 | 每次从Redis批量读取的任务数，网络延迟高时可增大 |

### 运行模式对比

| 模式 | 命令 | 适用场景 |
|-----|------|---------|
| 完整流程 | `python main.py 10 5` | 首次运行，自动推送任务并开始训练 |
| 仅训练 | `python train.py 10 5` | 任务已推送，只需启动Workers训练 |
| 仅收集 | `python -c "..."` | 训练已完成，收集残留结果到数据库 |

### 输出示例

```
🚀 BF2 Training System - Batch Multi-Process Mode
============================================================
配置: 10 workers, batch_size=5
✅ Redis 健康: 3/3 个实例可用
Collector started, pid=12345
Worker 0 started, pid=12346
...
============================================================
所有进程已启动，正在训练...
按 Ctrl+C 停止
============================================================
📝 Worker[0] Task[1] Features=[1, 5, 28, 62] | F1=0.85 Acc=0.90 | Time=1.2s
📝 Worker[3] Task[1] Features=[2, 3, 15, 40] | F1=0.82 Acc=0.88 | Time=1.3s
Worker 0: 📤 已推送 100 条到Redis
🚀 Fast collector started (parallel mode)
  +5,000    → 累计       5,000 条 |    2.5s
```

## 数据库说明

### 表结构

```sql
CREATE TABLE results (
    features INTEGER[],      -- 特征组合，如 [1, 5, 28, 62]
    mean_f1_macro DOUBLE,    -- F1分数
    mean_accuracy DOUBLE,    -- 准确率
    created_at TIMESTAMP     -- 创建时间
);
```

### 常用查询

```sql
-- 查看Top 10结果（按F1排序）
SELECT * FROM results ORDER BY mean_f1_macro DESC LIMIT 10;

-- 统计结果数量
SELECT COUNT(*) FROM results;

-- 查看最近插入的结果
SELECT * FROM results ORDER BY created_at DESC LIMIT 10;
```

## 故障排查

### 1. 数据库锁定错误

```
duckdb.duckdb.IOException: Could not set lock on file...
```

**解决：**
```bash
# 查找占用进程
lsof results.duckdb

# 杀死进程
kill <PID>

# 或删除数据库重新运行
rm results.duckdb
```

### 2. Redis连接失败

```
❌ 错误: 所有 Redis 实例均不可用
```

**解决：**
- 检查网络连接
- 检查Redis配置（修改 `rd3.py` 中的 `REDIS_CONFIGS`）

### 3. 特征缺失错误

```
⚠️ 跳过任务，缺失列: [99]
```

**解决：**
- 检查数据文件 `data_99_3cls_train.pkl` 的列范围
- 修改 `main.py` 中的特征范围：`whole_numbers = list(range(1, 99))`

### 4. 任务计算但结果未写入

**解决：**
- 检查Collector进程是否运行
- 检查Redis中 `results` 队列长度：`redis-cli LLEN results`

## 性能优化建议

1. **Worker数量**：建议设置为CPU核心数，通常为4-16
2. **Batch大小**：
   - 本地Redis：5-10
   - 远程Redis（高延迟）：20-50
3. **Redis实例**：建议3个以上，实现负载均衡
4. **内存监控**：大量任务时监控Redis内存使用

## 项目文件说明

```
BF2/
├── main.py              # 主运行脚本（完整流程）
├── train.py             # 独立训练脚本（多Worker）
├── rd3.py               # Redis操作核心模块
├── data_to_duckdb.py    # 独立收集脚本
├── data_99_3cls_train.pkl   # 训练数据
├── data_99_3cls_test.pkl    # 测试数据
├── results.duckdb       # 结果数据库（运行后生成）
└── README.md            # 本文档
```

## 技术特点

- **负载均衡**：多Redis实例轮询，自动故障转移
- **批量处理**：Pipeline批量读写，减少网络往返
- **原子操作**：使用Redis事务保证数据一致性
- **断点续传**：支持任务推送中断恢复
- **实时统计**：每批任务实时打印，监控进度

## 许可证

MIT License

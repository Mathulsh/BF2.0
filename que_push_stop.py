'''本机推送数据到Redis的运行脚，redis内存受限时使用'''
from itertools import combinations, islice
import signal
import sys
import os
from rd import push_to_redis # type: ignore

# 添加全局变量用于控制程序运行状态
should_stop = False

def signal_handler(signum, frame):
    """处理中断信号"""
    global should_stop
    print("\n收到中断信号，正在保存进度...")
    should_stop = True

# 注册信号处理器
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# 按顺序生成组合
whole_numbers: list[int] = list(range(1, 44)) # 1-68标准化后的特征列
comb = combinations(whole_numbers, 5)
# 本机跑 
# C(43,4) = 124,100(12w)
# C(43,5) = 962,598(96w) 实际丢失了6824条
# C(68,4) = 814,385种组合(81w)
# 上集群跑
# C(68,5) = 10,424,128种组合(1000w)
# C(98,4) = 3,612,280种组合(360w)
# C(98,5) = 67,910,864种组合(6800w)

def batch_generator(iterable, batch_size, start_index=0):
    """分割可迭代对象为指定大小的批次避免内存溢出"""
    iterator = iter(iterable)
    
    # 跳过已处理的批次
    for _ in range(start_index):
        batch = list(islice(iterator, batch_size))
        if not batch:
            break
    
    while True:
        batch = list(islice(iterator, batch_size)) # 获取最多batch_size个元素迭代器
        # 批次为空则结束循环
        if not batch:
            break
        yield batch # 惰性生成器节省内存

def get_last_processed_batch():
    """从文件中读取上次处理到的批次索引"""
    progress_file = "push_progress.txt"
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            try:
                return int(f.read().strip())
            except ValueError:
                return 0
    return 0

def save_progress(current_batch_index):
    """保存当前处理的批次索引到文件"""
    with open("push_progress.txt", 'w') as f:
        f.write(str(current_batch_index))

if __name__ == "__main__":
    # time_start = time.time()
    
    # 获取上次处理的批次索引，用于断点续传
    start_batch_index = get_last_processed_batch()
    print(f"从第 {start_batch_index + 1} 批次开始处理")
    
    batch_size = 100000 # 每批处理数据量
    for i, batch in enumerate(batch_generator(comb, batch_size, start_batch_index), start_batch_index):
        if should_stop:
            print(f"程序被中断，当前批次 {i+1} 未完成，进度已保存")
            save_progress(i)
            sys.exit(0)
            
        # print(f"Pushing batch {i+1} with {len(batch)} combinations...")
        push_to_redis(batch)
        print(f"Batch {i+1} completed")
        
        # 保存进度
        save_progress(i + 1)
        
        # 查看所有batch
        # batches = peek_all_batches()
        # print(f"总batch数量: {len(batches)}")
        
    # 处理完成，删除进度文件
    if os.path.exists("push_progress.txt"):
        os.remove("push_progress.txt")
        
    # print("All combinations have been pushed to Redis!")
    
    # time_end = time.time()
    # print(f"Time cost: {time_end - time_start} seconds")

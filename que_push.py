'''推送数据到Redis的运行脚本'''
from itertools import combinations, islice
from rd import push_to_redis # type: ignore
import time, os
# 按顺序生成特征组合
whole_numbers: list[int] = list(range(1, 44))
comb = combinations(whole_numbers, 5)

def batch_generator(iterable, batch_size):
    """分割可迭代对象为指定大小的批次避免内存溢出"""
    iterator = iter(iterable)
    while True:
        batch = list(islice(iterator, batch_size)) # 获取最多batch_size个元素迭代器
        # 批次为空则结束循环
        if not batch:
            break
        yield batch # 惰性生成器节省内存

if __name__ == "__main__":
    time_start = time.time()

    # 检查是否运行中断过
    flag_file = "push_progress.txt"
    if os.path.exists(flag_file):
        print("检测到推送已在进行中或已完成，退出程序。")
        exit()

    # 创建标记文件
    with open(flag_file, "w") as f:
        f.write("started")
            
    batch_size = 100000 # 每批处理数据量
    for i, batch in enumerate(batch_generator(comb, batch_size)):
        print(f"Pushing batch {i+1} with {len(batch)} combinations...")
        push_to_redis(batch)
        print(f"Batch {i+1} completed")
        if i == 9:
            break
        
    # 删除中断检验文件
    if os.path.exists(flag_file):
        os.remove(flag_file)
    print("All combinations have been pushed to Redis!")
    
    time_end = time.time()
    print(f"Time cost: {time_end - time_start} seconds")

# 本机跑 
# C(43,4) = 124,100(12w)
# C(43,5) = 962,598(96w) 实际丢失了6824条
# C(68,4) = 814,385种组合(81w)
# 上集群跑
# C(68,5) = 10,424,128种组合(1000w)
# C(98,4) = 3,612,280种组合(360w)
# C(98,5) = 67,910,864种组合(6800w)
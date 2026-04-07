import duckdb
import pandas as pd
from pandas import DataFrame
from rd import push_to_redis

# 连接db数据库
con: duckdb.DuckDBPyConnection = duckdb.connect("/Users/lishihong/projects/Research/HEA/BF/results/duckdb/RF/results_3cls_43x4-train_f1.duckdb")

# 统计表中记录数
count = con.execute("SELECT COUNT(*) FROM results").fetchall()[0][0]
print(f"Number of records in results table: {count}")

# 取top10分数 
top10: DataFrame = con.sql("SELECT * FROM results ORDER BY mean_f1_macro DESC LIMIT 10;").fetchdf()
print("Top 10 records:", top10)

# 合并两个 DuckDB 文件
# con = duckdb.connect("results1.duckdb")
# con.execute("ATTACH 'results.duckdb' AS db2")
# con.execute("""
#     INSERT INTO results
#     SELECT * FROM db2.results
# """)
# con.execute("DETACH db2")
# con.close()

# 查漏数据并推送到Redis,⚠️需修改[]()内数值组合范围和长度,及表名
# query = """
# WITH full_space AS (
#     SELECT [a,b,c,d,e] AS features
#     FROM range(1,44) AS t1(a)
#     CROSS JOIN range(1,44) AS t2(b)
#     CROSS JOIN range(1,44) AS t3(c)
#     CROSS JOIN range(1,44) AS t4(d)
#     CROSS JOIN range(1,44) AS t5(e)
#     WHERE a < b AND b < c AND c < d AND d < e
# )
# SELECT fs.features
# FROM full_space fs
# WHERE NOT EXISTS (
#     SELECT 1 FROM results r WHERE r.features = fs.features
# )
# """
# missing_df = con.execute(query).df()
# # print(f"Missing feature combinations count: {len(missing_df)}, {missing_df}")
# missing_df.to_pickle("missing_features.pkl")
# push_to_redis(
#     tuple(map(int, row)) for row in missing_df["features"]
# )

# 查重操作再cli操作
# df = con.execute("SELECT * FROM results").df()
# dup_df = df[df.duplicated(subset=["features"], keep=False)] # 非常耗时，不建议使用

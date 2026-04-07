'''将csv文件转换为pickle文件的脚本'''
import pickle
import pandas as pd # type: ignore

# 去冗余且标准化后的csv文件
df = pd.read_excel("scaled_data-99.xlsx",sheet_name="test_scaled")
# 装载pickle数据

with open("./BF/data_99_3cls_test.pkl", "wb") as f:
    pickle.dump(df, f)

# 查看pickle数据
# with open("./BF/data_99_3cls_train.pkl", "rb") as f:
#     info = pickle.load(f)
#     print(info)
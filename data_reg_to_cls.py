'''将回归连续标签转换为分类离散标签'''
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler

# ===============================
# 1. 连续 y → 三分类（5, 20）
# ===============================
def continuous_to_3class(y: pd.Series) -> pd.Series:
    """
    将连续标签 y 转换为 3 分类：
    0: y < 5
    1: 5 <= y < 20
    2: y >= 20
    """
    y_cls = pd.cut(
        y,
        bins=[-np.inf, 5, 20, np.inf],
        labels=[0, 1, 2],
        right=False
    )
    return y_cls.astype(int)

def continuous_to_4class(y: pd.Series) -> pd.Series:
    """
    将连续标签 y 转换为 4 分类：
    0: y < 5
    1: 5 <= y < 10
    2: 10 <= y < 20
    3: y >= 20
    """
    y_cls = pd.cut(
        y,
        bins=[-np.inf, 5, 10, 20, np.inf],
        labels=[0, 1, 2, 3],
        right=False
    )
    return y_cls.astype(int)

def continuous_to_5class(y: pd.Series) -> pd.Series:
    """
    将连续标签 y 转换为 5 分类：
    0: y < 5
    1: 5 <= y < 10
    2: 10 <= y < 20
    3: 20 <= y < 30
    4: y >= 30
    """
    y_cls = pd.cut(
        y,
        bins=[-np.inf, 5, 10, 20, 30, np.inf],
        labels=[0, 1, 2, 3, 4],
        right=False
    )
    return y_cls.astype(int)
# ===============================
# 2. 分层划分 + 归一化 + 导出
# ===============================
def export_stratified_class_dataset(
    X: pd.DataFrame,
    y_countinous: pd.Series,
    test_size: float = 0.2,
    random_state: int = 42,
    scaler_type: str = "minmax",
    output_prefix: str = "dataset",
    export_full_dataset: bool = True,
):
    """
    连续 y → 3, 4, 5 分类 → stratified 划分 → 归一化训练/测试集 → 导出 CSV
    如果 export_full_dataset 为 True，则还会导出完整的原始分类数据集（不归一化）
    """

    # --- 连续 → 分类 ---
    y_class = continuous_to_3class(y_countinous) # ⚠️ 根据分类需求修改相应函数

    # --- 基本检查 ---
    print("分类标签分布：")
    print(y_class.value_counts().sort_index())

    # --- 分层划分 ---
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y_class,
        test_size=test_size,
        random_state=random_state,
        stratify=y_class
    )

    # --- 特征归一化（只 fit 训练集）---
    if scaler_type == "minmax":
        scaler = MinMaxScaler()
    else:
        raise ValueError("目前仅支持 minmax")

    X_train_scaled = pd.DataFrame(
        scaler.fit_transform(X_train),
        index=X_train.index,
        columns=X_train.columns
    )

    X_test_scaled = pd.DataFrame(
        scaler.transform(X_test),
        index=X_test.index,
        columns=X_test.columns
    )

    # --- 合并 X + y，方便导出 ---
    train_df = X_train_scaled.copy()
    train_df["label"] = y_train

    test_df = X_test_scaled.copy()
    test_df["label"] = y_test

    # --- 导出 ---
    train_path = f"{output_prefix}_train.csv"
    test_path = f"{output_prefix}_test.csv"

    train_df.to_csv(train_path, index=False)
    test_df.to_csv(test_path, index=False)

    print(f"\n导出完成：")
    print()
    print(f"训练集：{train_path}  (n={len(train_df)})")
    print(f"测试集：{test_path}   (n={len(test_df)})")

    # --- 如果需要，导出完整数据集（不归一化）---
    if export_full_dataset:
        full_dataset_df = X.copy()
        full_dataset_df["label"] = y_class
        
        # 导出完整数据集
        full_dataset_path = f"{output_prefix}.csv"
        full_dataset_df.to_csv(full_dataset_path, index=False)
        
        print(f"完整分类数据集（未归一化）：{full_dataset_path}  (n={len(full_dataset_df)})")

    return train_df, test_df

# X: pd.DataFrame
# y: 连续值 pd.Series
df = pd.read_csv(r'data/77.csv')
X: pd.DataFrame = df.iloc[:, :-1]
y_countinous: pd.Series = pd.Series(df.iloc[:, -1])

train_df, test_df = export_stratified_class_dataset(
    X=X,
    y_countinous=y_countinous,
    test_size=0.2,
    random_state=42,
    export_full_dataset=True,
    output_prefix="./data/77_3cls",
    scaler_type="minmax",
)

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
深度学习模块
LSTM/Transformer 用于时序预测
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, List, Tuple, Union
from loguru import logger
import sys
import os
import pickle
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TimeSeriesDataset:
    """时序数据集，用于深度学习模型"""

    def __init__(
        self,
        data: np.ndarray,
        sequence_length: int = 20,
        horizon: int = 1,
        stride: int = 1
    ):
        """
        初始化时序数据集

        Args:
            data: 特征数据 (n_samples, n_features)
            sequence_length: 输入序列长度
            horizon: 预测步长
            stride: 滑动步长
        """
        self.data = data
        self.sequence_length = sequence_length
        self.horizon = horizon
        self.stride = stride

        self._create_sequences()

    def _create_sequences(self):
        """创建序列数据"""
        n_samples = len(self.data)
        n_features = self.data.shape[1] if len(self.data.shape) > 1 else 1

        sequences = []
        targets = []

        for i in range(0, n_samples - self.sequence_length - self.horizon + 1, self.stride):
            seq = self.data[i:i + self.sequence_length]
            target = self.data[i + self.sequence_length + self.horizon - 1, 0]  # 预测第一个特征（pct_chg）

            sequences.append(seq)
            targets.append(target)

        self.sequences = np.array(sequences)
        self.targets = np.array(targets)

        logger.debug(f"创建 {len(self.sequences)} 个序列，形状: {self.sequences.shape}")

    def __len__(self):
        return len(self.sequences)

    def __getitem__(self, idx):
        return self.sequences[idx], self.targets[idx]

    def get_data(self) -> Tuple[np.ndarray, np.ndarray]:
        """获取所有数据"""
        return self.sequences, self.targets


class LSTMModel:
    """LSTM 时序预测模型"""

    def __init__(
        self,
        input_dim: int,
        hidden_dim: int = 64,
        num_layers: int = 2,
        output_dim: int = 1,
        dropout: float = 0.2,
        bidirectional: bool = False
    ):
        """
        初始化 LSTM 模型

        Args:
            input_dim: 输入特征维度
            hidden_dim: 隐藏层维度
            num_layers: LSTM 层数
            output_dim: 输出维度
            dropout: Dropout 比例
            bidirectional: 是否双向
        """
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        self.output_dim = output_dim
        self.dropout = dropout
        self.bidirectional = bidirectional
        self.model = None
        self.device = None
        self._is_pytorch = False

    def _build_model(self):
        """构建 PyTorch 模型"""
        try:
            import torch
            import torch.nn as nn

            self._is_pytorch = True
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

            class _LSTMNet(nn.Module):
                def __init__(self, input_dim, hidden_dim, num_layers, output_dim, dropout, bidirectional):
                    super().__init__()
                    self.lstm = nn.LSTM(
                        input_dim, hidden_dim, num_layers,
                        batch_first=True,
                        dropout=dropout if num_layers > 1 else 0,
                        bidirectional=bidirectional
                    )
                    lstm_output_dim = hidden_dim * 2 if bidirectional else hidden_dim
                    self.fc = nn.Sequential(
                        nn.Linear(lstm_output_dim, hidden_dim),
                        nn.ReLU(),
                        nn.Dropout(dropout),
                        nn.Linear(hidden_dim, output_dim)
                    )

                def forward(self, x):
                    # x: (batch, seq_len, features)
                    lstm_out, _ = self.lstm(x)
                    # 取最后一个时间步
                    out = lstm_out[:, -1, :]
                    out = self.fc(out)
                    return out

            self.model = _LSTMNet(
                self.input_dim, self.hidden_dim, self.num_layers,
                self.output_dim, self.dropout, self.bidirectional
            )
            self.model.to(self.device)

            logger.info(f"LSTM 模型已构建，设备: {self.device}")

        except ImportError:
            logger.warning("PyTorch 未安装，使用简化 LSTM 实现")
            self._is_pytorch = False

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        epochs: int = 100,
        batch_size: int = 32,
        learning_rate: float = 0.001,
        validation_split: float = 0.2,
        early_stopping_patience: int = 10,
        verbose: int = 1
    ) -> Dict[str, Any]:
        """
        训练模型

        Args:
            X: 训练序列 (n_samples, sequence_length, n_features)
            y: 目标值 (n_samples,)
            epochs: 训练轮数
            batch_size: 批大小
            learning_rate: 学习率
            validation_split: 验证集比例
            early_stopping_patience: 早停耐心值
            verbose: 日志级别

        Returns:
            训练历史
        """
        if self.model is None:
            self._build_model()

        if not self._is_pytorch:
            # 简化实现：使用传统 ML 模型
            return self._fit_simple(X, y, epochs, batch_size, learning_rate)

        import torch
        import torch.nn as nn
        from torch.utils.data import DataLoader, TensorDataset

        # 划分数据
        n_samples = len(X)
        n_val = int(n_samples * validation_split)

        X_train, X_val = X[:-n_val], X[-n_val:]
        y_train, y_val = y[:-n_val], y[-n_val:]

        # 转换为 Tensor
        X_train_t = torch.FloatTensor(X_train).to(self.device)
        y_train_t = torch.FloatTensor(y_train).unsqueeze(1).to(self.device)
        X_val_t = torch.FloatTensor(X_val).to(self.device)
        y_val_t = torch.FloatTensor(y_val).unsqueeze(1).to(self.device)

        # DataLoader
        train_dataset = TensorDataset(X_train_t, y_train_t)
        train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

        # 损失函数和优化器
        criterion = nn.MSELoss()
        optimizer = torch.optim.Adam(self.model.parameters(), lr=learning_rate)
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer, mode='min', factor=0.5, patience=5
        )

        # 训练
        history = {"train_loss": [], "val_loss": []}
        best_val_loss = float('inf')
        patience_counter = 0

        for epoch in range(epochs):
            # 训练模式
            self.model.train()
            train_loss = 0.0

            for X_batch, y_batch in train_loader:
                optimizer.zero_grad()
                outputs = self.model(X_batch)
                loss = criterion(outputs, y_batch)
                loss.backward()
                optimizer.step()
                train_loss += loss.item()

            train_loss /= len(train_loader)

            # 验证
            self.model.eval()
            with torch.no_grad():
                val_outputs = self.model(X_val_t)
                val_loss = criterion(val_outputs, y_val_t).item()

            history["train_loss"].append(train_loss)
            history["val_loss"].append(val_loss)

            scheduler.step(val_loss)

            # 早停
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                patience_counter = 0
                # 保存最佳模型
                self.best_state = self.model.state_dict().copy()
            else:
                patience_counter += 1
                if patience_counter >= early_stopping_patience:
                    if verbose > 0:
                        logger.info(f"早停于 epoch {epoch + 1}")
                    break

            if verbose > 0 and (epoch + 1) % 10 == 0:
                logger.info(f"Epoch {epoch + 1}/{epochs} - Train Loss: {train_loss:.4f}, Val Loss: {val_loss:.4f}")

        # 加载最佳模型
        if hasattr(self, 'best_state'):
            self.model.load_state_dict(self.best_state)

        return history

    def _fit_simple(self, X: np.ndarray, y: np.ndarray, epochs: int, batch_size: int, learning_rate: float) -> Dict:
        """简化训练（无 PyTorch）"""
        from sklearn.ensemble import GradientBoostingRegressor

        # 展平序列特征
        X_flat = X.reshape(X.shape[0], -1)

        self.simple_model = GradientBoostingRegressor(
            n_estimators=epochs,
            learning_rate=learning_rate,
            max_depth=5,
            random_state=42
        )
        self.simple_model.fit(X_flat, y)

        return {"train_loss": [0], "val_loss": [0], "method": "GradientBoosting"}

    def predict(self, X: np.ndarray) -> np.ndarray:
        """
        预测

        Args:
            X: 输入序列 (n_samples, sequence_length, n_features)

        Returns:
            预测值 (n_samples,)
        """
        if not self._is_pytorch and hasattr(self, 'simple_model'):
            X_flat = X.reshape(X.shape[0], -1)
            return self.simple_model.predict(X_flat)

        import torch

        self.model.eval()
        with torch.no_grad():
            X_t = torch.FloatTensor(X).to(self.device)
            predictions = self.model(X_t).cpu().numpy()

        return predictions.flatten()

    def save(self, path: str):
        """保存模型"""
        if self._is_pytorch:
            import torch
            torch.save({
                "model_state": self.model.state_dict(),
                "config": {
                    "input_dim": self.input_dim,
                    "hidden_dim": self.hidden_dim,
                    "num_layers": self.num_layers,
                    "output_dim": self.output_dim,
                    "dropout": self.dropout,
                    "bidirectional": self.bidirectional,
                }
            }, path)
        else:
            with open(path, "wb") as f:
                pickle.dump({
                    "simple_model": self.simple_model,
                    "config": {
                        "input_dim": self.input_dim,
                    }
                }, f)

        logger.info(f"LSTM 模型已保存: {path}")

    def load(self, path: str) -> "LSTMModel":
        """加载模型"""
        if self._is_pytorch:
            import torch
            checkpoint = torch.load(path, map_location=self.device)
            config = checkpoint["config"]
            self.input_dim = config["input_dim"]
            self.hidden_dim = config["hidden_dim"]
            self.num_layers = config["num_layers"]
            self.output_dim = config["output_dim"]
            self.dropout = config["dropout"]
            self.bidirectional = config["bidirectional"]
            self._build_model()
            self.model.load_state_dict(checkpoint["model_state"])
        else:
            with open(path, "rb") as f:
                data = pickle.load(f)
                self.simple_model = data["simple_model"]

        logger.info(f"LSTM 模型已加载: {path}")
        return self


class TransformerModel:
    """Transformer 时序预测模型"""

    def __init__(
        self,
        input_dim: int,
        d_model: int = 64,
        nhead: int = 4,
        num_encoder_layers: int = 2,
        dim_feedforward: int = 128,
        dropout: float = 0.1,
        output_dim: int = 1
    ):
        """
        初始化 Transformer 模型

        Args:
            input_dim: 输入特征维度
            d_model: 模型维度
            nhead: 注意力头数
            num_encoder_layers: 编码器层数
            dim_feedforward: 前馈网络维度
            dropout: Dropout 比例
            output_dim: 输出维度
        """
        self.input_dim = input_dim
        self.d_model = d_model
        self.nhead = nhead
        self.num_encoder_layers = num_encoder_layers
        self.dim_feedforward = dim_feedforward
        self.dropout = dropout
        self.output_dim = output_dim
        self.model = None
        self.device = None
        self._is_pytorch = False

    def _build_model(self):
        """构建 PyTorch 模型"""
        try:
            import torch
            import torch.nn as nn

            self._is_pytorch = True
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

            class _TransformerNet(nn.Module):
                def __init__(self, input_dim, d_model, nhead, num_encoder_layers, dim_feedforward, dropout, output_dim):
                    super().__init__()

                    # 输入嵌入
                    self.input_embedding = nn.Linear(input_dim, d_model)

                    # 位置编码
                    self.pos_encoder = PositionalEncoding(d_model, dropout)

                    # Transformer 编码器
                    encoder_layer = nn.TransformerEncoderLayer(
                        d_model=d_model,
                        nhead=nhead,
                        dim_feedforward=dim_feedforward,
                        dropout=dropout,
                        batch_first=True
                    )
                    self.transformer_encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_encoder_layers)

                    # 输出层
                    self.fc = nn.Sequential(
                        nn.Linear(d_model, d_model // 2),
                        nn.ReLU(),
                        nn.Dropout(dropout),
                        nn.Linear(d_model // 2, output_dim)
                    )

                def forward(self, x):
                    # x: (batch, seq_len, features)
                    x = self.input_embedding(x)
                    x = self.pos_encoder(x)
                    x = self.transformer_encoder(x)
                    # 取最后一个时间步
                    x = x[:, -1, :]
                    x = self.fc(x)
                    return x

            self.model = _TransformerNet(
                self.input_dim, self.d_model, self.nhead,
                self.num_encoder_layers, self.dim_feedforward,
                self.dropout, self.output_dim
            )
            self.model.to(self.device)

            logger.info(f"Transformer 模型已构建，设备: {self.device}")

        except ImportError:
            logger.warning("PyTorch 未安装，使用简化 Transformer 实现")
            self._is_pytorch = False

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        epochs: int = 100,
        batch_size: int = 32,
        learning_rate: float = 0.001,
        validation_split: float = 0.2,
        early_stopping_patience: int = 10,
        verbose: int = 1
    ) -> Dict[str, Any]:
        """训练模型（同 LSTM）"""
        if self.model is None:
            self._build_model()

        if not self._is_pytorch:
            return self._fit_simple(X, y, epochs, batch_size, learning_rate)

        import torch
        import torch.nn as nn
        from torch.utils.data import DataLoader, TensorDataset

        # 划分数据
        n_samples = len(X)
        n_val = int(n_samples * validation_split)

        X_train, X_val = X[:-n_val], X[-n_val:]
        y_train, y_val = y[:-n_val], y[-n_val:]

        # 转换为 Tensor
        X_train_t = torch.FloatTensor(X_train).to(self.device)
        y_train_t = torch.FloatTensor(y_train).unsqueeze(1).to(self.device)
        X_val_t = torch.FloatTensor(X_val).to(self.device)
        y_val_t = torch.FloatTensor(y_val).unsqueeze(1).to(self.device)

        # DataLoader
        train_dataset = TensorDataset(X_train_t, y_train_t)
        train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

        # 损失函数和优化器
        criterion = nn.MSELoss()
        optimizer = torch.optim.Adam(self.model.parameters(), lr=learning_rate, weight_decay=1e-5)
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer, mode='min', factor=0.5, patience=5
        )

        # 训练
        history = {"train_loss": [], "val_loss": []}
        best_val_loss = float('inf')
        patience_counter = 0

        for epoch in range(epochs):
            self.model.train()
            train_loss = 0.0

            for X_batch, y_batch in train_loader:
                optimizer.zero_grad()
                outputs = self.model(X_batch)
                loss = criterion(outputs, y_batch)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
                optimizer.step()
                train_loss += loss.item()

            train_loss /= len(train_loader)

            self.model.eval()
            with torch.no_grad():
                val_outputs = self.model(X_val_t)
                val_loss = criterion(val_outputs, y_val_t).item()

            history["train_loss"].append(train_loss)
            history["val_loss"].append(val_loss)

            scheduler.step(val_loss)

            if val_loss < best_val_loss:
                best_val_loss = val_loss
                patience_counter = 0
                self.best_state = self.model.state_dict().copy()
            else:
                patience_counter += 1
                if patience_counter >= early_stopping_patience:
                    if verbose > 0:
                        logger.info(f"早停于 epoch {epoch + 1}")
                    break

            if verbose > 0 and (epoch + 1) % 10 == 0:
                logger.info(f"Epoch {epoch + 1}/{epochs} - Train Loss: {train_loss:.4f}, Val Loss: {val_loss:.4f}")

        if hasattr(self, 'best_state'):
            self.model.load_state_dict(self.best_state)

        return history

    def _fit_simple(self, X: np.ndarray, y: np.ndarray, epochs: int, batch_size: int, learning_rate: float) -> Dict:
        """简化训练"""
        from sklearn.ensemble import GradientBoostingRegressor

        X_flat = X.reshape(X.shape[0], -1)
        self.simple_model = GradientBoostingRegressor(
            n_estimators=epochs,
            learning_rate=learning_rate,
            max_depth=5,
            random_state=42
        )
        self.simple_model.fit(X_flat, y)

        return {"train_loss": [0], "val_loss": [0], "method": "GradientBoosting"}

    def predict(self, X: np.ndarray) -> np.ndarray:
        """预测"""
        if not self._is_pytorch and hasattr(self, 'simple_model'):
            X_flat = X.reshape(X.shape[0], -1)
            return self.simple_model.predict(X_flat)

        import torch

        self.model.eval()
        with torch.no_grad():
            X_t = torch.FloatTensor(X).to(self.device)
            predictions = self.model(X_t).cpu().numpy()

        return predictions.flatten()

    def save(self, path: str):
        """保存模型"""
        if self._is_pytorch:
            import torch
            torch.save({
                "model_state": self.model.state_dict(),
                "config": {
                    "input_dim": self.input_dim,
                    "d_model": self.d_model,
                    "nhead": self.nhead,
                    "num_encoder_layers": self.num_encoder_layers,
                    "dim_feedforward": self.dim_feedforward,
                    "dropout": self.dropout,
                    "output_dim": self.output_dim,
                }
            }, path)
        else:
            with open(path, "wb") as f:
                pickle.dump({
                    "simple_model": self.simple_model,
                    "config": {"input_dim": self.input_dim}
                }, f)

        logger.info(f"Transformer 模型已保存: {path}")

    def load(self, path: str) -> "TransformerModel":
        """加载模型"""
        if self._is_pytorch:
            import torch
            checkpoint = torch.load(path, map_location=self.device)
            config = checkpoint["config"]
            self.input_dim = config["input_dim"]
            self.d_model = config["d_model"]
            self.nhead = config["nhead"]
            self.num_encoder_layers = config["num_encoder_layers"]
            self.dim_feedforward = config["dim_feedforward"]
            self.dropout = config["dropout"]
            self.output_dim = config["output_dim"]
            self._build_model()
            self.model.load_state_dict(checkpoint["model_state"])
        else:
            with open(path, "rb") as f:
                data = pickle.load(f)
                self.simple_model = data["simple_model"]

        logger.info(f"Transformer 模型已加载: {path}")
        return self


class PositionalEncoding:
    """位置编码（用于 Transformer）"""

    def __init__(self, d_model: int, dropout: float = 0.1, max_len: int = 5000):
        import torch
        import torch.nn as nn

        self.dropout = nn.Dropout(p=dropout)

        pe = torch.zeros(max_len, d_model)
        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
        div_term = torch.exp(torch.arange(0, d_model, 2).float() * (-np.log(10000.0) / d_model))
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        pe = pe.unsqueeze(0)
        self.register_buffer = lambda name, val: setattr(self, name, val)
        self.register_buffer('pe', pe)

    def __call__(self, x):
        import torch
        x = x + self.pe[:, :x.size(1), :]
        return self.dropout(x)


class DeepLearningPredictor:
    """深度学习预测器"""

    def __init__(
        self,
        model_type: str = "lstm",
        sequence_length: int = 20,
        hidden_dim: int = 64,
        num_layers: int = 2,
        dropout: float = 0.2,
        learning_rate: float = 0.001,
        epochs: int = 100,
        batch_size: int = 32,
        early_stopping_patience: int = 10
    ):
        """
        初始化深度学习预测器

        Args:
            model_type: 模型类型 (lstm/transformer)
            sequence_length: 输入序列长度
            hidden_dim: 隐藏层维度
            num_layers: 层数
            dropout: Dropout 比例
            learning_rate: 学习率
            epochs: 训练轮数
            batch_size: 批大小
            early_stopping_patience: 早停耐心值
        """
        self.model_type = model_type
        self.sequence_length = sequence_length
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        self.dropout = dropout
        self.learning_rate = learning_rate
        self.epochs = epochs
        self.batch_size = batch_size
        self.early_stopping_patience = early_stopping_patience

        self.models: Dict[str, Any] = {}  # horizon -> model
        self.scaler = None

    def prepare_data(
        self,
        features: pd.DataFrame,
        target_col: str = "pct_chg",
        horizon: int = 1
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        准备训练数据

        Args:
            features: 特征 DataFrame
            target_col: 目标列名
            horizon: 预测步长

        Returns:
            X, y 序列
        """
        from sklearn.preprocessing import StandardScaler

        # 提取数值特征
        numeric_cols = features.select_dtypes(include=[np.number]).columns.tolist()
        if target_col in numeric_cols:
            numeric_cols.remove(target_col)

        X_raw = features[numeric_cols].values

        # 标准化
        if self.scaler is None:
            self.scaler = StandardScaler()
            X_scaled = self.scaler.fit_transform(X_raw)
        else:
            X_scaled = self.scaler.transform(X_raw)

        # 创建目标（如果是预测未来，需要 shift）
        y_raw = features[target_col].values if target_col in features.columns else X_raw[:, 0]

        # 创建序列
        dataset = TimeSeriesDataset(
            np.column_stack([y_raw.reshape(-1, 1), X_scaled]),
            sequence_length=self.sequence_length,
            horizon=horizon
        )

        return dataset.get_data()

    def train(
        self,
        features: pd.DataFrame,
        horizons: List[str] = ["1d", "5d", "20d"],
        target_col: str = "pct_chg",
        verbose: int = 1
    ) -> Dict[str, Any]:
        """
        训练模型

        Args:
            features: 特征 DataFrame
            horizons: 预测周期列表
            target_col: 目标列名
            verbose: 日志级别

        Returns:
            训练结果
        """
        from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

        results = {"horizons": {}}
        horizon_days = {"1d": 1, "5d": 5, "20d": 20}

        for horizon in horizons:
            logger.info(f"训练 {horizon} 模型...")

            days = horizon_days.get(horizon, 1)

            # 准备数据
            X, y = self.prepare_data(features, target_col, days)

            if len(X) < 100:
                logger.warning(f"{horizon}: 样本数不足 ({len(X)})，跳过")
                continue

            # 创建模型
            input_dim = X.shape[2]

            if self.model_type == "lstm":
                model = LSTMModel(
                    input_dim=input_dim,
                    hidden_dim=self.hidden_dim,
                    num_layers=self.num_layers,
                    dropout=self.dropout
                )
            else:
                model = TransformerModel(
                    input_dim=input_dim,
                    d_model=self.hidden_dim,
                    num_encoder_layers=self.num_layers,
                    dropout=self.dropout
                )

            # 训练
            history = model.fit(
                X, y,
                epochs=self.epochs,
                batch_size=self.batch_size,
                learning_rate=self.learning_rate,
                early_stopping_patience=self.early_stopping_patience,
                verbose=verbose
            )

            # 评估
            train_size = int(len(X) * 0.8)
            X_test, y_test = X[train_size:], y[train_size:]
            y_pred = model.predict(X_test)

            metrics = {
                "mse": mean_squared_error(y_test, y_pred),
                "mae": mean_absolute_error(y_test, y_pred),
                "r2": r2_score(y_test, y_pred),
            }

            self.models[horizon] = model
            results["horizons"][horizon] = {
                "metrics": metrics,
                "history": history,
                "input_dim": input_dim,
            }

            logger.info(f"{horizon}: MSE={metrics['mse']:.4f}, MAE={metrics['mae']:.4f}, R2={metrics['r2']:.4f}")

        return results

    def predict(
        self,
        features: pd.DataFrame,
        horizons: List[str] = None
    ) -> Dict[str, np.ndarray]:
        """
        预测

        Args:
            features: 特征 DataFrame
            horizons: 预测周期列表

        Returns:
            各周期预测结果
        """
        if horizons is None:
            horizons = list(self.models.keys())

        predictions = {}

        for horizon in horizons:
            if horizon not in self.models:
                continue

            model = self.models[horizon]
            X, _ = self.prepare_data(features)

            # 只预测最新数据
            X_latest = X[-1:]

            pred = model.predict(X_latest)
            predictions[f"pred_{horizon}"] = pred[0]

        return predictions

    def save(self, path: str):
        """保存模型"""
        data = {
            "model_type": self.model_type,
            "sequence_length": self.sequence_length,
            "hidden_dim": self.hidden_dim,
            "num_layers": self.num_layers,
            "models": {},
            "scaler": self.scaler,
        }

        # 单独保存每个模型
        for horizon, model in self.models.items():
            model_path = f"{path}_{horizon}.pkl"
            model.save(model_path)
            data["models"][horizon] = model_path

        with open(path, "wb") as f:
            pickle.dump(data, f)

        logger.info(f"深度学习预测器已保存: {path}")

    def load(self, path: str) -> "DeepLearningPredictor":
        """加载模型"""
        with open(path, "rb") as f:
            data = pickle.load(f)

        self.model_type = data["model_type"]
        self.sequence_length = data["sequence_length"]
        self.hidden_dim = data["hidden_dim"]
        self.num_layers = data["num_layers"]
        self.scaler = data.get("scaler")

        # 加载各周期模型
        for horizon, model_path in data.get("models", {}).items():
            if self.model_type == "lstm":
                model = LSTMModel(input_dim=1)
            else:
                model = TransformerModel(input_dim=1)

            model.load(model_path)
            self.models[horizon] = model

        logger.info(f"深度学习预测器已加载: {path}")
        return self


if __name__ == "__main__":
    # 测试
    print("深度学习模块测试")
    print("=" * 50)

    # 生成测试数据
    np.random.seed(42)
    n_samples = 1000
    n_features = 10

    features = pd.DataFrame(
        np.random.randn(n_samples, n_features),
        columns=[f"feature_{i}" for i in range(n_features)]
    )
    features["pct_chg"] = np.random.randn(n_samples) * 2

    # 测试 LSTM
    print("\n测试 LSTM 模型...")
    predictor = DeepLearningPredictor(
        model_type="lstm",
        sequence_length=20,
        hidden_dim=32,
        num_layers=2,
        epochs=10,
        batch_size=32
    )

    result = predictor.train(features, horizons=["1d"], verbose=0)
    print(f"训练结果: {result}")

    # 测试预测
    predictions = predictor.predict(features.tail(100))
    print(f"预测结果: {predictions}")

    print("\n测试成功!")
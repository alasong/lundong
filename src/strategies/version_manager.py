"""
策略版本管理器
- 版本创建、切换、回滚
- 版本历史查询
- 参数版本化存储
"""

import hashlib
import json
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass

try:
    from data.database import get_database, SQLiteDatabase
except ImportError:
    try:
        from data.database import get_database, SQLiteDatabase
    except ImportError:
        from src.data.database import get_database, SQLiteDatabase


@dataclass
class VersionInfo:
    """版本信息数据类"""

    id: int
    strategy_name: str
    version: str
    params: dict
    description: str
    code_hash: Optional[str]
    created_at: datetime
    is_active: bool


class StrategyVersionManager:
    def __init__(self, db: SQLiteDatabase = None):
        self.db = db or get_database()

    def create_version(
        self, strategy_name: str, params: dict, description: str = ""
    ) -> str:
        """创建新版本，返回版本号 (如 v1.0.0)"""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT version FROM strategy_versions WHERE strategy_name = ? ORDER BY id DESC LIMIT 1",
                    (strategy_name,),
                )
                result = cursor.fetchone()

                if result:
                    current_version = result[0]
                    # 解析版本号，例如 v1.0.0 -> [1, 0, 0]
                    version_parts = current_version.replace("v", "").split(".")
                    major, minor, patch = map(int, version_parts)
                    # 递增补丁版本
                    new_version = f"v{major}.{minor}.{patch + 1}"
                else:
                    new_version = "v1.0.0"  # 初始版本

                # 计算参数哈希
                params_str = json.dumps(params, sort_keys=True)
                code_hash = hashlib.sha256(params_str.encode()).hexdigest()

                # 插入新版本记录
                cursor.execute(
                    """
                    INSERT INTO strategy_versions 
                    (strategy_name, version, params_json, description, code_hash, is_active)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        strategy_name,
                        new_version,
                        json.dumps(params),
                        description,
                        code_hash,
                        0,
                    ),
                )
                conn.commit()

                return new_version
        except Exception as e:
            raise e

    def get_version(
        self, strategy_name: str, version: str = None
    ) -> Optional[VersionInfo]:
        """获取版本信息，version=None 返回当前激活版本"""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()

                if version is None:
                    # 获取当前激活版本
                    cursor.execute(
                        """
                        SELECT id, strategy_name, version, params_json, description, 
                               code_hash, created_at, is_active
                        FROM strategy_versions 
                        WHERE strategy_name = ? AND is_active = 1
                        ORDER BY id DESC 
                        LIMIT 1
                        """,
                        (strategy_name,),
                    )
                else:
                    # 获取指定版本
                    cursor.execute(
                        """
                        SELECT id, strategy_name, version, params_json, description, 
                               code_hash, created_at, is_active
                        FROM strategy_versions 
                        WHERE strategy_name = ? AND version = ?
                        """,
                        (strategy_name, version),
                    )

                result = cursor.fetchone()
                if result:
                    (
                        id,
                        strategy_name,
                        version,
                        params_json,
                        description,
                        code_hash,
                        created_at,
                        is_active,
                    ) = result
                    return VersionInfo(
                        id=id,
                        strategy_name=strategy_name,
                        version=version,
                        params=json.loads(params_json),
                        description=description,
                        code_hash=code_hash,
                        created_at=datetime.fromisoformat(created_at)
                        if isinstance(created_at, str)
                        else created_at,
                        is_active=bool(is_active),
                    )
                return None
        except Exception as e:
            raise e

    def list_versions(self, strategy_name: str) -> List[VersionInfo]:
        """列出所有版本历史"""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT id, strategy_name, version, params_json, description, 
                           code_hash, created_at, is_active
                    FROM strategy_versions 
                    WHERE strategy_name = ?
                    ORDER BY id DESC
                    """,
                    (strategy_name,),
                )

                results = []
                for row in cursor.fetchall():
                    (
                        id,
                        strategy_name,
                        version,
                        params_json,
                        description,
                        code_hash,
                        created_at,
                        is_active,
                    ) = row
                    results.append(
                        VersionInfo(
                            id=id,
                            strategy_name=strategy_name,
                            version=version,
                            params=json.loads(params_json),
                            description=description,
                            code_hash=code_hash,
                            created_at=datetime.fromisoformat(created_at)
                            if isinstance(created_at, str)
                            else created_at,
                            is_active=bool(is_active),
                        )
                    )
                return results
        except Exception as e:
            raise e

    def activate_version(self, strategy_name: str, version: str) -> bool:
        """激活指定版本"""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()

                # 先将当前激活版本设为非激活
                cursor.execute(
                    "UPDATE strategy_versions SET is_active = 0 WHERE strategy_name = ? AND is_active = 1",
                    (strategy_name,),
                )

                # 激活指定版本
                cursor.execute(
                    "UPDATE strategy_versions SET is_active = 1 WHERE strategy_name = ? AND version = ?",
                    (strategy_name, version),
                )

                if cursor.rowcount == 0:
                    # 如果没有找到指定版本，则回滚
                    conn.rollback()
                    return False

                conn.commit()
                return True
        except Exception as e:
            raise e

    def rollback(self, strategy_name: str, steps: int = 1) -> Optional[str]:
        """回滚到前N个版本"""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()

                # 获取当前激活版本
                cursor.execute(
                    """
                    SELECT id, version 
                    FROM strategy_versions 
                    WHERE strategy_name = ? AND is_active = 1
                    ORDER BY id DESC 
                    LIMIT 1
                    """,
                    (strategy_name,),
                )
                current_result = cursor.fetchone()

                if not current_result:
                    return None

                current_id, current_version = current_result

                # 获取回滚目标版本
                cursor.execute(
                    """
                    SELECT version 
                    FROM strategy_versions 
                    WHERE strategy_name = ? AND id < ?
                    ORDER BY id DESC 
                    LIMIT 1 OFFSET ?
                    """,
                    (strategy_name, current_id, steps - 1),
                )
                target_result = cursor.fetchone()

                if not target_result:
                    return None

                target_version = target_result[0]

                # 将当前版本设为非激活
                cursor.execute(
                    "UPDATE strategy_versions SET is_active = 0 WHERE strategy_name = ? AND is_active = 1",
                    (strategy_name,),
                )

                # 激活目标版本
                cursor.execute(
                    "UPDATE strategy_versions SET is_active = 1 WHERE strategy_name = ? AND version = ?",
                    (strategy_name, target_version),
                )

                conn.commit()
                return target_version
        except Exception as e:
            raise e

    def compare_versions(self, strategy_name: str, v1: str, v2: str) -> dict:
        """比较两个版本的参数差异"""
        try:
            version1 = self.get_version(strategy_name, v1)
            version2 = self.get_version(strategy_name, v2)

            if not version1 or not version2:
                raise ValueError(f"One or both versions not found: {v1}, {v2}")

            params1 = version1.params
            params2 = version2.params

            # 比较参数差异
            diff = {}
            all_keys = set(params1.keys()) | set(params2.keys())

            for key in all_keys:
                val1 = params1.get(key)
                val2 = params2.get(key)

                if val1 != val2:
                    diff[key] = {"v1": val1, "v2": val2}

            return {
                "strategy_name": strategy_name,
                "v1": v1,
                "v2": v2,
                "diff": diff,
                "common_params": {
                    k: params1[k]
                    for k in params1
                    if k in params2 and params1[k] == params2[k]
                },
            }
        except Exception as e:
            raise e

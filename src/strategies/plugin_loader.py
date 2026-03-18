"""
策略插件加载器
- 扫描插件目录
- 动态导入策略类
- 注册到 StrategyFactory
- 支持热重载
"""

import importlib.util
import hashlib
import json
from pathlib import Path
from typing import Dict, List, Type, Optional
from loguru import logger

from .base_strategy import BaseStrategy
from .strategy_factory import StrategyFactory


class PluginLoader:
    """策略插件加载器"""

    PLUGIN_DIR = Path(__file__).parent / "plugins"
    ARCHIVED_DIR = Path(__file__).parent / "archived"

    def __init__(self, db=None):
        self.db = db
        self._loaded_plugins: Dict[str, dict] = {}  # name -> metadata
        self._file_hashes: Dict[str, str] = {}  # path -> hash

        # 确保目录存在
        self.PLUGIN_DIR.mkdir(exist_ok=True)
        self.ARCHIVED_DIR.mkdir(exist_ok=True)

        # 创建 __init__.py 文件
        (self.PLUGIN_DIR / "__init__.py").touch(exist_ok=True)
        (self.ARCHIVED_DIR / "__init__.py").touch(exist_ok=True)

    def scan_plugins(self) -> List[str]:
        """扫描插件目录，返回发现的插件名列表"""
        plugins = []
        for plugin_dir in self.PLUGIN_DIR.iterdir():
            if plugin_dir.is_dir() and not plugin_dir.name.startswith("."):
                metadata_file = plugin_dir / "metadata.json"
                if metadata_file.exists():
                    try:
                        with open(metadata_file, "r", encoding="utf-8") as f:
                            metadata = json.load(f)
                        if metadata.get("enabled", True):
                            plugins.append(plugin_dir.name)
                    except Exception as e:
                        logger.error(
                            f"Failed to load metadata for plugin {plugin_dir.name}: {e}"
                        )
        return plugins

    def load_plugin(self, plugin_name: str) -> bool:
        """加载单个插件"""
        try:
            plugin_dir = self.PLUGIN_DIR / plugin_name
            if not plugin_dir.exists():
                logger.error(f"Plugin directory does not exist: {plugin_dir}")
                return False

            metadata_file = plugin_dir / "metadata.json"
            if not metadata_file.exists():
                logger.error(f"Metadata file not found for plugin: {plugin_name}")
                return False

            # 加载元数据
            metadata = self._load_plugin_metadata(plugin_dir)
            if not metadata:
                logger.error(f"Failed to load metadata for plugin: {plugin_name}")
                return False

            # 检查是否启用
            if not metadata.get("enabled", True):
                logger.info(f"Plugin {plugin_name} is disabled, skipping load")
                return False

            # 检查文件哈希是否有变化
            if self._has_file_changed(plugin_dir):
                logger.info(f"Plugin {plugin_name} has changed, reloading...")

            # 导入策略类
            strategy_path = plugin_dir / metadata.get("entry_file", "strategy.py")
            strategy_class = self._import_strategy_class(strategy_path)
            if not strategy_class:
                logger.error(
                    f"Failed to import strategy class for plugin: {plugin_name}"
                )
                return False

            # 注册到 StrategyFactory
            StrategyFactory.register_strategy(plugin_name, strategy_class)

            # 保存插件信息
            self._loaded_plugins[plugin_name] = metadata

            # 更新文件哈希
            self._update_file_hashes(plugin_dir)

            logger.success(f"Successfully loaded plugin: {plugin_name}")
            return True

        except Exception as e:
            logger.error(f"Error loading plugin {plugin_name}: {e}")
            return False

    def load_all_plugins(self) -> Dict[str, bool]:
        """加载所有插件，返回 {name: success}"""
        results = {}
        plugin_names = self.scan_plugins()

        for plugin_name in plugin_names:
            results[plugin_name] = self.load_plugin(plugin_name)

        return results

    def unload_plugin(self, plugin_name: str) -> bool:
        """卸载插件"""
        try:
            if plugin_name not in self._loaded_plugins:
                logger.warning(f"Plugin {plugin_name} is not loaded")
                return False

            # 从 StrategyFactory 注销
            StrategyFactory.unregister_strategy(plugin_name)

            # 清除插件信息
            del self._loaded_plugins[plugin_name]

            logger.success(f"Successfully unloaded plugin: {plugin_name}")
            return True

        except Exception as e:
            logger.error(f"Error unloading plugin {plugin_name}: {e}")
            return False

    def reload_plugin(self, plugin_name: str) -> bool:
        """热重载插件"""
        # 先卸载
        if plugin_name in self._loaded_plugins:
            self.unload_plugin(plugin_name)

        # 再加载
        return self.load_plugin(plugin_name)

    def get_plugin_info(self, plugin_name: str) -> Optional[dict]:
        """获取插件信息"""
        return self._loaded_plugins.get(plugin_name)

    def list_plugins(self, include_archived: bool = False) -> List[dict]:
        """列出所有插件"""
        plugins = []

        # 扫描正常插件
        for plugin_dir in self.PLUGIN_DIR.iterdir():
            if plugin_dir.is_dir() and not plugin_dir.name.startswith("."):
                metadata_file = plugin_dir / "metadata.json"
                if metadata_file.exists():
                    try:
                        metadata = self._load_plugin_metadata(plugin_dir)
                        if metadata:
                            metadata["location"] = "active"
                            plugins.append(metadata)
                    except Exception as e:
                        logger.error(
                            f"Failed to load metadata for plugin {plugin_dir.name}: {e}"
                        )

        # 扫描归档插件
        if include_archived:
            for plugin_dir in self.ARCHIVED_DIR.iterdir():
                if plugin_dir.is_dir() and not plugin_dir.name.startswith("."):
                    metadata_file = plugin_dir / "metadata.json"
                    if metadata_file.exists():
                        try:
                            metadata = self._load_plugin_metadata(plugin_dir)
                            if metadata:
                                metadata["location"] = "archived"
                                plugins.append(metadata)
                        except Exception as e:
                            logger.error(
                                f"Failed to load metadata for archived plugin {plugin_dir.name}: {e}"
                            )

        return plugins

    def check_updates(self) -> List[str]:
        """检查哪些插件文件有更新（通过文件哈希）"""
        updates = []
        for plugin_dir in self.PLUGIN_DIR.iterdir():
            if plugin_dir.is_dir() and not plugin_dir.name.startswith("."):
                if self._has_file_changed(plugin_dir):
                    updates.append(plugin_dir.name)
        return updates

    def _load_plugin_metadata(self, plugin_dir: Path) -> dict:
        """加载插件元数据 (metadata.json)"""
        metadata_file = plugin_dir / "metadata.json"
        if not metadata_file.exists():
            raise FileNotFoundError(f"Metadata file not found: {metadata_file}")

        with open(metadata_file, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        # 验证必要字段
        required_fields = ["name", "strategy_class", "entry_file"]
        for field in required_fields:
            if field not in metadata:
                raise ValueError(
                    f"Missing required field '{field}' in metadata: {metadata_file}"
                )

        # 验证插件名称匹配
        if metadata["name"] != plugin_dir.name:
            raise ValueError(
                f"Plugin name in metadata ({metadata['name']}) does not match directory name ({plugin_dir.name})"
            )

        return metadata

    def _import_strategy_class(
        self, strategy_path: Path
    ) -> Optional[Type[BaseStrategy]]:
        """动态导入策略类"""
        if not strategy_path.exists():
            logger.error(f"Strategy file does not exist: {strategy_path}")
            return None

        try:
            # 生成模块名
            module_name = f"strategies.plugins.{strategy_path.parent.name}.strategy_{hash(str(strategy_path))}"

            # 加载模块
            spec = importlib.util.spec_from_file_location(module_name, strategy_path)
            if spec is None or spec.loader is None:
                logger.error(f"Could not load spec for {strategy_path}")
                return None

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # 获取策略类
            strategy_class_name = (
                strategy_path.parent.name.replace("_", " ").title().replace(" ", "")
            )
            if hasattr(module, strategy_class_name):
                strategy_class = getattr(module, strategy_class_name)
            else:
                # 尝试从 metadata 获取类名
                metadata_file = strategy_path.parent / "metadata.json"
                if metadata_file.exists():
                    with open(metadata_file, "r", encoding="utf-8") as f:
                        metadata = json.load(f)
                    strategy_class_name = metadata.get("strategy_class")
                    if strategy_class_name and hasattr(module, strategy_class_name):
                        strategy_class = getattr(module, strategy_class_name)
                    else:
                        logger.error(
                            f"Strategy class {strategy_class_name} not found in {strategy_path}"
                        )
                        return None
                else:
                    logger.error(
                        f"No metadata file to determine strategy class in {strategy_path}"
                    )
                    return None

            # 验证是否为 BaseStrategy 的子类
            if not issubclass(strategy_class, BaseStrategy):
                logger.error(f"{strategy_class} is not a subclass of BaseStrategy")
                return None

            return strategy_class

        except Exception as e:
            logger.error(f"Error importing strategy from {strategy_path}: {e}")
            return None

    def _get_file_hash(self, file_path: Path) -> str:
        """计算文件哈希"""
        with open(file_path, "rb") as f:
            return hashlib.md5(f.read()).hexdigest()

    def _update_file_hashes(self, plugin_dir: Path):
        """更新插件目录中所有文件的哈希值"""
        for file_path in plugin_dir.rglob("*"):
            if file_path.is_file():
                file_hash = self._get_file_hash(file_path)
                self._file_hashes[str(file_path)] = file_hash

    def _has_file_changed(self, plugin_dir: Path) -> bool:
        """检查插件目录中的文件是否有变化"""
        for file_path in plugin_dir.rglob("*"):
            if file_path.is_file():
                current_hash = self._get_file_hash(file_path)
                stored_hash = self._file_hashes.get(str(file_path))

                if stored_hash is None or stored_hash != current_hash:
                    return True
        return False

"""事故未确认升级引擎。

引擎状态/时间线与接收方收件箱分别使用独立的 SQLite 文件，
两者之间仅通过进程内 deliver(message) 交互。
"""

from .engine import EscalationEngine
from .inbox import Inbox

__all__ = ["EscalationEngine", "Inbox"]

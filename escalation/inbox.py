"""接收方收件箱：独立 SQLite 文件，独立提交，提供只读查询。"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional

_SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    message_id   TEXT PRIMARY KEY,
    incident_id  TEXT NOT NULL,
    round        INTEGER NOT NULL,
    recipient    TEXT NOT NULL,
    scheduled_at INTEGER NOT NULL,
    inbox_seq    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_incident ON messages (incident_id, inbox_seq);
"""


@dataclass(frozen=True)
class Message:
    """投递给收件箱的一条通知。"""

    message_id: str
    incident_id: str
    round: int
    recipient: str
    scheduled_at: int


@dataclass(frozen=True)
class InboxRecord:
    """收件箱中已落库的一条记录（只读视图）。"""

    message_id: str
    incident_id: str
    round: int
    recipient: str
    scheduled_at: int
    inbox_seq: int


class Inbox:
    """接收器。deliver 由引擎进程内调用；查询方法只读，供测试使用。

    对同一 message_id 只保留一条记录（重复投递被忽略）。
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def deliver(self, message: Message) -> None:
        cur = self._conn.execute(
            "SELECT inbox_seq FROM messages WHERE message_id = ?",
            (message.message_id,),
        )
        if cur.fetchone() is not None:
            return
        cur = self._conn.execute("SELECT COALESCE(MAX(inbox_seq), 0) FROM messages")
        next_seq = cur.fetchone()[0] + 1
        self._conn.execute(
            "INSERT INTO messages (message_id, incident_id, round, recipient, scheduled_at, inbox_seq)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                message.message_id,
                message.incident_id,
                message.round,
                message.recipient,
                message.scheduled_at,
                next_seq,
            ),
        )
        self._conn.commit()

    def messages(self, incident_id: Optional[str] = None) -> list[InboxRecord]:
        if incident_id is None:
            cur = self._conn.execute(
                "SELECT message_id, incident_id, round, recipient, scheduled_at, inbox_seq"
                " FROM messages ORDER BY inbox_seq"
            )
        else:
            cur = self._conn.execute(
                "SELECT message_id, incident_id, round, recipient, scheduled_at, inbox_seq"
                " FROM messages WHERE incident_id = ? ORDER BY inbox_seq",
                (incident_id,),
            )
        return [InboxRecord(*row) for row in cur.fetchall()]

    def close(self) -> None:
        self._conn.close()

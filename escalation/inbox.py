"""Receiver inbox backed by its own SQLite file.

The inbox is an independent component: it commits on its own connection
and exposes read-only queries for tests.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class Message:
    message_id: str
    incident_id: str
    round: int
    recipient: str
    scheduled_at: int
    deadline: int


class Inbox:
    """Receives delivered messages and stores them in its own SQLite file."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL UNIQUE,
                incident_id TEXT NOT NULL,
                round INTEGER NOT NULL,
                recipient TEXT NOT NULL,
                scheduled_at INTEGER NOT NULL,
                deadline INTEGER NOT NULL
            )
            """
        )
        self._conn.commit()

    def deliver(self, message: Message) -> None:
        """Store a message; the same message_id is kept only once."""
        self._conn.execute(
            """
            INSERT OR IGNORE INTO messages
                (message_id, incident_id, round, recipient, scheduled_at, deadline)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                message.message_id,
                message.incident_id,
                message.round,
                message.recipient,
                message.scheduled_at,
                message.deadline,
            ),
        )
        self._conn.commit()

    def list_messages(self, incident_id: str | None = None) -> list[Message]:
        """Read-only query: messages in first-received order."""
        if incident_id is None:
            rows = self._conn.execute(
                "SELECT message_id, incident_id, round, recipient, scheduled_at, deadline"
                " FROM messages ORDER BY seq"
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT message_id, incident_id, round, recipient, scheduled_at, deadline"
                " FROM messages WHERE incident_id = ? ORDER BY seq",
                (incident_id,),
            ).fetchall()
        return [
            Message(
                message_id=row[0],
                incident_id=row[1],
                round=row[2],
                recipient=row[3],
                scheduled_at=row[4],
                deadline=row[5],
            )
            for row in rows
        ]

    def get(self, message_id: str) -> Message | None:
        """Read-only query: fetch a single message by id."""
        row = self._conn.execute(
            "SELECT message_id, incident_id, round, recipient, scheduled_at, deadline"
            " FROM messages WHERE message_id = ?",
            (message_id,),
        ).fetchone()
        if row is None:
            return None
        return Message(
            message_id=row[0],
            incident_id=row[1],
            round=row[2],
            recipient=row[3],
            scheduled_at=row[4],
            deadline=row[5],
        )

    def close(self) -> None:
        self._conn.close()

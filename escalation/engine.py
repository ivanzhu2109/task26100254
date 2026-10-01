"""Escalation engine: state and timeline in its own SQLite file.

Time is always injected by the caller (integer epoch seconds).
Round r covers [declared_at + r*600, declared_at + (r+1)*600).
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from .inbox import Inbox, Message

ROUND_SECONDS = 600


@dataclass(frozen=True)
class TimelineEvent:
    seq: int
    at: int
    type: str
    round: int | None
    detail: str


@dataclass(frozen=True)
class IncidentState:
    incident_id: str
    roster: tuple[str, ...]
    declared_at: int
    acknowledged: bool
    ack_round: int | None
    ack_at: int | None
    current_round: int


class EscalationEngine:
    def __init__(self, db_path: str, inbox: Inbox) -> None:
        self._conn = sqlite3.connect(db_path)
        self._inbox = inbox
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS incidents (
                incident_id TEXT PRIMARY KEY,
                roster_json TEXT NOT NULL,
                declared_at INTEGER NOT NULL,
                acked INTEGER NOT NULL DEFAULT 0,
                ack_round INTEGER,
                ack_at INTEGER
            );
            CREATE TABLE IF NOT EXISTS rounds (
                incident_id TEXT NOT NULL,
                round INTEGER NOT NULL,
                recipient TEXT NOT NULL,
                start_at INTEGER NOT NULL,
                end_at INTEGER NOT NULL,
                message_id TEXT NOT NULL,
                delivered INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (incident_id, round)
            );
            CREATE TABLE IF NOT EXISTS ack_requests (
                request_id TEXT PRIMARY KEY,
                incident_id TEXT NOT NULL,
                round INTEGER NOT NULL,
                at INTEGER NOT NULL,
                result INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                incident_id TEXT NOT NULL,
                at INTEGER NOT NULL,
                type TEXT NOT NULL,
                round INTEGER,
                detail TEXT NOT NULL DEFAULT ''
            );
            """
        )
        self._conn.commit()

    def _add_event(
        self, incident_id: str, at: int, type: str, round: int | None, detail: str = ""
    ) -> None:
        self._conn.execute(
            "INSERT INTO events (incident_id, at, type, round, detail) VALUES (?, ?, ?, ?, ?)",
            (incident_id, at, type, round, detail),
        )

    def _generate_round(self, incident_id: str, roster: list[str], declared_at: int, r: int) -> None:
        recipient = roster[r % len(roster)]
        start_at = declared_at + r * ROUND_SECONDS
        end_at = start_at + ROUND_SECONDS
        message_id = f"{incident_id}#{r}"
        self._conn.execute(
            "INSERT INTO rounds (incident_id, round, recipient, start_at, end_at, message_id)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (incident_id, r, recipient, start_at, end_at, message_id),
        )
        self._add_event(
            incident_id,
            start_at,
            "notification_committed",
            r,
            f"recipient={recipient} message_id={message_id}",
        )

    def _current_round(self, incident_id: str) -> int:
        row = self._conn.execute(
            "SELECT MAX(round) FROM rounds WHERE incident_id = ?", (incident_id,)
        ).fetchone()
        return row[0] if row[0] is not None else -1

    def _state(self, incident_id: str) -> IncidentState:
        row = self._conn.execute(
            "SELECT roster_json, declared_at, acked, ack_round, ack_at"
            " FROM incidents WHERE incident_id = ?",
            (incident_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown incident: {incident_id}")
        return IncidentState(
            incident_id=incident_id,
            roster=tuple(json.loads(row[0])),
            declared_at=row[1],
            acknowledged=bool(row[2]),
            ack_round=row[3],
            ack_at=row[4],
            current_round=self._current_round(incident_id),
        )

    def declare(self, incident_id: str, roster: list[str], at: int) -> IncidentState:
        """Declare an incident; retries with the same id must not reset anything."""
        row = self._conn.execute(
            "SELECT 1 FROM incidents WHERE incident_id = ?", (incident_id,)
        ).fetchone()
        if row is not None:
            return self._state(incident_id)
        if not roster:
            raise ValueError("roster must not be empty")
        if len(set(roster)) != len(roster):
            raise ValueError("roster members must be distinct")
        self._conn.execute(
            "INSERT INTO incidents (incident_id, roster_json, declared_at) VALUES (?, ?, ?)",
            (incident_id, json.dumps(list(roster)), at),
        )
        self._add_event(incident_id, at, "declared", None, f"roster={list(roster)}")
        self._generate_round(incident_id, list(roster), at, 0)
        self._conn.commit()
        return self._state(incident_id)

    def acknowledge(self, incident_id: str, round: int, request_id: str, at: int) -> bool:
        """Try to acknowledge a round; retries with the same request_id
        return the first committed result."""
        prior = self._conn.execute(
            "SELECT result FROM ack_requests WHERE request_id = ?", (request_id,)
        ).fetchone()
        if prior is not None:
            return bool(prior[0])

        result = False
        row = self._conn.execute(
            "SELECT acked FROM incidents WHERE incident_id = ?", (incident_id,)
        ).fetchone()
        if row is not None and not row[0]:
            round_row = self._conn.execute(
                "SELECT start_at, end_at FROM rounds WHERE incident_id = ? AND round = ?",
                (incident_id, round),
            ).fetchone()
            if (
                round_row is not None
                and round == self._current_round(incident_id)
                and round_row[0] <= at < round_row[1]
            ):
                result = True

        self._conn.execute(
            "INSERT INTO ack_requests (request_id, incident_id, round, at, result)"
            " VALUES (?, ?, ?, ?, ?)",
            (request_id, incident_id, round, at, int(result)),
        )
        if result:
            self._conn.execute(
                "UPDATE incidents SET acked = 1, ack_round = ?, ack_at = ? WHERE incident_id = ?",
                (round, at, incident_id),
            )
            self._add_event(incident_id, at, "acknowledged", round, f"request_id={request_id}")
        self._conn.commit()
        return result

    def advance(self, now: int) -> None:
        """Generate all rounds due up to `now` and deliver committed notifications."""
        incidents = self._conn.execute(
            "SELECT incident_id, roster_json, declared_at, acked FROM incidents"
        ).fetchall()
        for incident_id, roster_json, declared_at, acked in incidents:
            if acked:
                continue
            due = (now - declared_at) // ROUND_SECONDS
            current = self._current_round(incident_id)
            roster = json.loads(roster_json)
            for r in range(current + 1, due + 1):
                self._generate_round(incident_id, roster, declared_at, r)
        self._conn.commit()

        pending = self._conn.execute(
            "SELECT incident_id, round, recipient, start_at, end_at, message_id"
            " FROM rounds WHERE delivered = 0 ORDER BY incident_id, round"
        ).fetchall()
        for incident_id, r, recipient, start_at, end_at, message_id in pending:
            self._inbox.deliver(
                Message(
                    message_id=message_id,
                    incident_id=incident_id,
                    round=r,
                    recipient=recipient,
                    scheduled_at=start_at,
                    deadline=end_at,
                )
            )
            self._conn.execute(
                "UPDATE rounds SET delivered = 1 WHERE incident_id = ? AND round = ?",
                (incident_id, r),
            )
            self._add_event(incident_id, now, "delivered", r, f"message_id={message_id}")
            self._conn.commit()

    def inspect(self, incident_id: str) -> tuple[IncidentState, list[TimelineEvent]]:
        """Read incident state and the ordered timeline."""
        state = self._state(incident_id)
        rows = self._conn.execute(
            "SELECT seq, at, type, round, detail FROM events WHERE incident_id = ? ORDER BY seq",
            (incident_id,),
        ).fetchall()
        timeline = [
            TimelineEvent(seq=row[0], at=row[1], type=row[2], round=row[3], detail=row[4])
            for row in rows
        ]
        return state, timeline

    def close(self) -> None:
        self._conn.close()

"""升级引擎：状态与时间线存于独立 SQLite 文件。"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Optional, Protocol, Sequence

from .inbox import Message

ROUND_SECONDS = 600

_SCHEMA = """
CREATE TABLE IF NOT EXISTS incidents (
    incident_id TEXT PRIMARY KEY,
    roster      TEXT NOT NULL,
    declared_at INTEGER NOT NULL,
    acked       INTEGER NOT NULL DEFAULT 0,
    acked_round INTEGER,
    acked_at    INTEGER
);
CREATE TABLE IF NOT EXISTS rounds (
    incident_id TEXT NOT NULL,
    round       INTEGER NOT NULL,
    recipient   TEXT NOT NULL,
    start_at    INTEGER NOT NULL,
    end_at      INTEGER NOT NULL,
    message_id  TEXT NOT NULL,
    delivered   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (incident_id, round)
);
CREATE TABLE IF NOT EXISTS events (
    incident_id TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    at          INTEGER NOT NULL,
    type        TEXT NOT NULL,
    detail      TEXT NOT NULL,
    PRIMARY KEY (incident_id, seq)
);
CREATE TABLE IF NOT EXISTS ack_requests (
    request_id  TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL,
    round       INTEGER NOT NULL,
    at          INTEGER NOT NULL,
    result      INTEGER NOT NULL
);
"""


class DeliverFn(Protocol):
    def __call__(self, message: Message) -> None: ...


@dataclass(frozen=True)
class TimelineEvent:
    seq: int
    at: int
    type: str
    detail: dict


@dataclass(frozen=True)
class IncidentState:
    incident_id: str
    roster: tuple[str, ...]
    declared_at: int
    acknowledged: bool
    acked_round: Optional[int]
    acked_at: Optional[int]
    current_round: int
    timeline: tuple[TimelineEvent, ...]


class EscalationEngine:
    """事故未确认升级引擎。

    时间由调用方以整数秒注入；轮次 r 的时间区间为
    [declared_at + r*600, declared_at + (r+1)*600)，含起点、不含终点。
    """

    def __init__(self, db_path: str, deliver: DeliverFn) -> None:
        self._db_path = db_path
        self._deliver = deliver
        self._conn = sqlite3.connect(db_path)
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    # ---- 内部辅助 ----

    def _next_seq(self, incident_id: str) -> int:
        cur = self._conn.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM events WHERE incident_id = ?",
            (incident_id,),
        )
        return cur.fetchone()[0] + 1

    def _add_event(self, incident_id: str, at: int, type: str, detail: dict) -> None:
        self._conn.execute(
            "INSERT INTO events (incident_id, seq, at, type, detail) VALUES (?, ?, ?, ?, ?)",
            (incident_id, self._next_seq(incident_id), at, type, json.dumps(detail)),
        )

    def _create_round(self, incident_id: str, roster: Sequence[str], declared_at: int, round: int) -> None:
        recipient = roster[round % len(roster)]
        start_at = declared_at + round * ROUND_SECONDS
        message_id = f"{incident_id}:{round}"
        self._conn.execute(
            "INSERT INTO rounds (incident_id, round, recipient, start_at, end_at, message_id)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (incident_id, round, recipient, start_at, start_at + ROUND_SECONDS, message_id),
        )
        self._add_event(
            incident_id,
            start_at,
            "notified",
            {
                "round": round,
                "recipient": recipient,
                "message_id": message_id,
                "scheduled_at": start_at,
            },
        )

    # ---- 四个操作 ----

    def declare(self, incident_id: str, roster: Sequence[str], at: int) -> None:
        roster = list(roster)
        if not roster:
            raise ValueError("roster must not be empty")
        if len(set(roster)) != len(roster):
            raise ValueError("roster members must be distinct")
        cur = self._conn.execute(
            "SELECT 1 FROM incidents WHERE incident_id = ?", (incident_id,)
        )
        if cur.fetchone() is not None:
            return  # 幂等重试：不重置名单、起点或轮次
        self._conn.execute(
            "INSERT INTO incidents (incident_id, roster, declared_at) VALUES (?, ?, ?)",
            (incident_id, json.dumps(roster), at),
        )
        self._add_event(incident_id, at, "declared", {"roster": roster})
        self._create_round(incident_id, roster, at, 0)
        self._conn.commit()

    def acknowledge(self, incident_id: str, round: int, request_id: str, at: int) -> bool:
        cur = self._conn.execute(
            "SELECT result FROM ack_requests WHERE request_id = ?", (request_id,)
        )
        row = cur.fetchone()
        if row is not None:
            return bool(row[0])  # 幂等重试：返回首次已提交的结果

        success = False
        cur = self._conn.execute(
            "SELECT acked FROM incidents WHERE incident_id = ?", (incident_id,)
        )
        inc = cur.fetchone()
        if inc is not None and not inc[0]:
            cur = self._conn.execute(
                "SELECT start_at, end_at FROM rounds WHERE incident_id = ?"
                " ORDER BY round DESC LIMIT 1",
                (incident_id,),
            )
            cur_round = cur.fetchone()
            cur = self._conn.execute(
                "SELECT start_at, end_at FROM rounds WHERE incident_id = ? AND round = ?",
                (incident_id, round),
            )
            req_round = cur.fetchone()
            if (
                cur_round is not None
                and req_round is not None
                and req_round == cur_round
                and req_round[0] <= at < req_round[1]
            ):
                success = True

        self._conn.execute(
            "INSERT INTO ack_requests (request_id, incident_id, round, at, result)"
            " VALUES (?, ?, ?, ?, ?)",
            (request_id, incident_id, round, at, int(success)),
        )
        if success:
            self._conn.execute(
                "UPDATE incidents SET acked = 1, acked_round = ?, acked_at = ?"
                " WHERE incident_id = ?",
                (round, at, incident_id),
            )
            self._add_event(
                incident_id, at, "acknowledged", {"round": round, "request_id": request_id}
            )
        self._conn.commit()
        return success

    def advance(self, now: int) -> None:
        # 1) 补齐截至 now 应产生的轮次（逐轮补齐，按声明时刻的调度，不重新计时）
        cur = self._conn.execute(
            "SELECT incident_id, roster, declared_at FROM incidents WHERE acked = 0"
        )
        for incident_id, roster_json, declared_at in cur.fetchall():
            roster = json.loads(roster_json)
            cur2 = self._conn.execute(
                "SELECT round, end_at FROM rounds WHERE incident_id = ?"
                " ORDER BY round DESC LIMIT 1",
                (incident_id,),
            )
            last_round, last_end = cur2.fetchone()
            while last_end <= now:
                last_round += 1
                self._create_round(incident_id, roster, declared_at, last_round)
                last_end = declared_at + (last_round + 1) * ROUND_SECONDS
        self._conn.commit()

        # 2) 投递所有已提交但未交付的通知（含已确认事故在确认前生成的轮次）
        cur = self._conn.execute(
            "SELECT incident_id, round, recipient, start_at, message_id"
            " FROM rounds WHERE delivered = 0 ORDER BY incident_id, round"
        )
        for incident_id, round, recipient, start_at, message_id in cur.fetchall():
            self._deliver(
                Message(
                    message_id=message_id,
                    incident_id=incident_id,
                    round=round,
                    recipient=recipient,
                    scheduled_at=start_at,
                )
            )
            self._conn.execute(
                "UPDATE rounds SET delivered = 1 WHERE incident_id = ? AND round = ?",
                (incident_id, round),
            )
            self._conn.commit()

    def inspect(self, incident_id: str) -> IncidentState:
        cur = self._conn.execute(
            "SELECT roster, declared_at, acked, acked_round, acked_at"
            " FROM incidents WHERE incident_id = ?",
            (incident_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise KeyError(f"unknown incident: {incident_id}")
        roster_json, declared_at, acked, acked_round, acked_at = row
        cur = self._conn.execute(
            "SELECT COALESCE(MAX(round), -1) FROM rounds WHERE incident_id = ?",
            (incident_id,),
        )
        current_round = cur.fetchone()[0]
        cur = self._conn.execute(
            "SELECT seq, at, type, detail FROM events WHERE incident_id = ? ORDER BY seq",
            (incident_id,),
        )
        timeline = tuple(
            TimelineEvent(seq=seq, at=at, type=type, detail=json.loads(detail))
            for seq, at, type, detail in cur.fetchall()
        )
        return IncidentState(
            incident_id=incident_id,
            roster=tuple(json.loads(roster_json)),
            declared_at=declared_at,
            acknowledged=bool(acked),
            acked_round=acked_round,
            acked_at=acked_at,
            current_round=current_round,
            timeline=timeline,
        )

    def close(self) -> None:
        self._conn.close()

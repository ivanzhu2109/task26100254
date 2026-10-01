# task26100254 — 事故未确认升级引擎

Python 3.11+ / SQLite 实现的事故升级引擎与本地收件箱接收器。

## 结构

- `escalation/engine.py` — 升级引擎。状态与时间线存于一个 SQLite 文件（引擎库）。
- `escalation/inbox.py` — 接收器收件箱。独立 SQLite 文件、独立提交，通过进程内
  `deliver(message)` 接收通知，并提供只读查询 `list_messages()` / `get()`。
- `tests/test_engine.py` — 自动化测试（unittest，无第三方依赖）。

## 规则摘要

- 轮次 `r` 覆盖 `[declared_at + r*600, declared_at + (r+1)*600)`，含起点、不含终点；
  收件人为 `roster[r % len(roster)]`。
- `declare` 幂等：同一 `incident_id` 重试不重置名单、起点或轮次。
- `acknowledge` 仅在事故未确认、请求轮次为当前已生成轮次、且 `at` 落在该轮区间
  内时成功；同一 `request_id` 重试返回首次已提交的结果（即使已过期）。
- `advance(now)` 逐轮补齐截至 `now` 的轮次（不跳过、不重计时），并投递所有已提交
  未交付的通知；确认后不再生成新轮次，但已提交通知仍会完成交付。
- `message_id` 为 `"{incident_id}#{round}"`，跨重试稳定；收件箱按 `message_id`
  去重，同一事故首次入箱顺序按轮次递增。

## 运行测试

```sh
python3 -m unittest discover -s tests -v
```

## 使用示例

```python
from escalation import EscalationEngine, Inbox

inbox = Inbox("inbox.sqlite")
engine = EscalationEngine("engine.sqlite", inbox)

engine.declare("inc-1", ["alice", "bob", "carol"], at=1_700_000_000)
engine.advance(1_700_000_000 + 3 * 600)      # 补齐轮次 1..3 并投递通知
engine.acknowledge("inc-1", 3, "req-9", at=1_700_000_000 + 3 * 600 + 30)
state, timeline = engine.inspect("inc-1")
messages = inbox.list_messages("inc-1")       # 只读查询
```

# 事故未确认升级引擎

Python 3.11 + SQLite 实现的升级引擎。引擎状态/时间线与接收方收件箱分别使用
独立的 SQLite 文件，两者之间仅通过进程内 `deliver(message)` 交互。

## 结构

- `escalation/engine.py` — `EscalationEngine`：`declare` / `acknowledge` / `advance` / `inspect`
- `escalation/inbox.py` — `Inbox` 接收器：独立 SQLite 提交，只读查询 `messages()`
- `tests/test_escalation.py` — 自动化测试（unittest，无第三方依赖）

## 语义

- 时间由参数注入（整数秒）。轮次 r 区间为 `[declared_at + r*600, declared_at + (r+1)*600)`，
  含起点、不含终点；收件人为 `roster[r % len(roster)]`。
- `declare` 幂等：同一 incident_id 重试不重置名单、起点或轮次。
- `acknowledge` 仅在未确认、请求轮次为当前已生成轮次、at 落在该轮区间内时成功；
  同一 request_id 重试返回首次已提交的结果。
- `advance(now)` 逐轮补齐截至 now 的轮次（从声明时刻调度，不按实际执行时间重新计时），
  并交付所有已提交未交付的通知（含已确认事故在确认前生成的通知）。
- 收件箱按 `message_id` 去重；同一事故的通知首次入箱顺序按轮次递增。

## 运行

```bash
python3.11 -m unittest discover -s tests -v
```

## 使用示例

```python
from escalation import EscalationEngine, Inbox

inbox = Inbox("inbox.sqlite")
engine = EscalationEngine("engine.sqlite", inbox.deliver)

engine.declare("inc-1", ["alice", "bob", "carol"], at=1_700_000_000)
engine.advance(now=1_700_000_000 + 5 * 600 + 1)   # 停顿后补齐轮次 0..5
ok = engine.acknowledge("inc-1", 5, "req-1", at=1_700_000_000 + 5 * 600 + 10)
state = engine.inspect("inc-1")
records = inbox.messages("inc-1")
```

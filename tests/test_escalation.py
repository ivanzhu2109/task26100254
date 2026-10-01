import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from escalation import EscalationEngine, Inbox
from escalation.engine import ROUND_SECONDS

T0 = 1_700_000_000
ROSTER = ["alice", "bob", "carol"]


class EscalationTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        engine_db = os.path.join(self._tmp.name, "engine.sqlite")
        inbox_db = os.path.join(self._tmp.name, "inbox.sqlite")
        self.inbox = Inbox(inbox_db)
        self.addCleanup(self.inbox.close)
        self.engine = EscalationEngine(engine_db, self.inbox.deliver)
        self.addCleanup(self.engine.close)


class TestDeclare(EscalationTestCase):
    def test_declare_creates_round_zero_and_notification(self):
        self.engine.declare("inc-1", ROSTER, T0)
        state = self.engine.inspect("inc-1")
        self.assertFalse(state.acknowledged)
        self.assertEqual(state.current_round, 0)
        self.assertEqual(state.roster, tuple(ROSTER))
        self.assertEqual(state.declared_at, T0)
        types = [e.type for e in state.timeline]
        self.assertEqual(types, ["declared", "notified"])
        notified = state.timeline[1]
        self.assertEqual(notified.detail["round"], 0)
        self.assertEqual(notified.detail["recipient"], "alice")
        self.assertEqual(notified.detail["scheduled_at"], T0)

    def test_declare_retry_does_not_reset(self):
        self.engine.declare("inc-1", ROSTER, T0)
        self.engine.advance(T0 + 3 * ROUND_SECONDS)
        # 重试：不同名单、不同起点，不得重置任何状态
        self.engine.declare("inc-1", ["x", "y"], T0 + 9999)
        state = self.engine.inspect("inc-1")
        self.assertEqual(state.roster, tuple(ROSTER))
        self.assertEqual(state.declared_at, T0)
        self.assertEqual(state.current_round, 3)

    def test_declare_rejects_empty_or_duplicate_roster(self):
        with self.assertRaises(ValueError):
            self.engine.declare("inc-1", [], T0)
        with self.assertRaises(ValueError):
            self.engine.declare("inc-1", ["a", "a"], T0)


class TestAdvanceAfterStall(EscalationTestCase):
    def test_catchup_after_multi_period_stall(self):
        self.engine.declare("inc-1", ROSTER, T0)
        # 停顿 5 个完整周期 + 1 秒，再补齐
        now = T0 + 5 * ROUND_SECONDS + 1
        self.engine.advance(now)

        state = self.engine.inspect("inc-1")
        # 应生成轮次 0..5（第 5 轮在 T0+3000 开始，<= now）
        self.assertEqual(state.current_round, 5)

        records = self.inbox.messages("inc-1")
        # 无缺失、无重复
        self.assertEqual([r.round for r in records], [0, 1, 2, 3, 4, 5])
        self.assertEqual(len({r.message_id for r in records}), 6)
        # 首次入箱顺序按轮次递增
        seqs = [r.inbox_seq for r in records]
        self.assertEqual(seqs, sorted(seqs))
        # 收件人循环名单，计划时间无漂移（从声明时刻起算，非实际执行时间）
        for r in records:
            self.assertEqual(r.recipient, ROSTER[r.round % len(ROSTER)])
            self.assertEqual(r.scheduled_at, T0 + r.round * ROUND_SECONDS)
        # 时间线与收件箱一致
        notified = [e for e in state.timeline if e.type == "notified"]
        self.assertEqual(len(notified), 6)
        for event, record in zip(notified, records):
            self.assertEqual(event.detail["recipient"], record.recipient)
            self.assertEqual(event.detail["scheduled_at"], record.scheduled_at)
            self.assertEqual(event.detail["message_id"], record.message_id)

        # 再次 advance 不产生新轮次、不重复投递
        self.engine.advance(now)
        self.assertEqual(self.engine.inspect("inc-1").current_round, 5)
        self.assertEqual(len(self.inbox.messages("inc-1")), 6)

    def test_advance_is_incremental(self):
        self.engine.declare("inc-1", ROSTER, T0)
        self.engine.advance(T0 + ROUND_SECONDS)      # 补齐轮次 1
        self.engine.advance(T0 + 2 * ROUND_SECONDS)  # 补齐轮次 2
        records = self.inbox.messages("inc-1")
        self.assertEqual([r.round for r in records], [0, 1, 2])


class TestAcknowledge(EscalationTestCase):
    def test_ack_one_second_before_deadline_succeeds(self):
        self.engine.declare("inc-1", ROSTER, T0)
        ok = self.engine.acknowledge("inc-1", 0, "req-1", T0 + ROUND_SECONDS - 1)
        self.assertTrue(ok)
        state = self.engine.inspect("inc-1")
        self.assertTrue(state.acknowledged)
        self.assertEqual(state.acked_round, 0)
        self.assertEqual(state.acked_at, T0 + ROUND_SECONDS - 1)
        ack_events = [e for e in state.timeline if e.type == "acknowledged"]
        self.assertEqual(len(ack_events), 1)

    def test_ack_exactly_at_deadline_fails_even_without_advance(self):
        self.engine.declare("inc-1", ROSTER, T0)
        # 恰好到截止时刻，且 advance 尚未执行（当前已生成轮次仍是 0）
        ok = self.engine.acknowledge("inc-1", 0, "req-1", T0 + ROUND_SECONDS)
        self.assertFalse(ok)
        self.assertFalse(self.engine.inspect("inc-1").acknowledged)

    def test_ack_request_id_retry_returns_first_result(self):
        self.engine.declare("inc-1", ROSTER, T0)
        # 首次成功
        self.assertTrue(self.engine.acknowledge("inc-1", 0, "req-1", T0 + 10))
        # 同一 request_id 重试：即使已过期也返回首次结果
        self.assertTrue(self.engine.acknowledge("inc-1", 0, "req-1", T0 + 99 * ROUND_SECONDS))
        # 首次失败（过期），重试仍返回失败
        self.assertFalse(self.engine.acknowledge("inc-1", 0, "req-2", T0 + ROUND_SECONDS))
        self.assertFalse(self.engine.acknowledge("inc-1", 0, "req-2", T0 + 1))
        # 只产生一条确认事件
        state = self.engine.inspect("inc-1")
        self.assertEqual(len([e for e in state.timeline if e.type == "acknowledged"]), 1)

    def test_old_round_stays_invalid_after_full_roster_cycle(self):
        # 名单 3 人，轮次 0 与轮次 3 的收件人都是 alice
        self.engine.declare("inc-1", ROSTER, T0)
        self.engine.advance(T0 + 3 * ROUND_SECONDS)  # 生成轮次 1..3，当前轮次为 3
        state = self.engine.inspect("inc-1")
        self.assertEqual(state.current_round, 3)
        notified = {e.detail["round"]: e.detail["recipient"] for e in state.timeline if e.type == "notified"}
        self.assertEqual(notified[0], "alice")
        self.assertEqual(notified[3], "alice")
        # 旧轮次 0 不能因收件人相同而重新有效
        at_in_round3 = T0 + 3 * ROUND_SECONDS + 100
        self.assertFalse(self.engine.acknowledge("inc-1", 0, "req-old", at_in_round3))
        # 当前轮次 3 可以确认
        self.assertTrue(self.engine.acknowledge("inc-1", 3, "req-new", at_in_round3))

    def test_no_new_rounds_after_ack_but_pending_notifications_delivered(self):
        self.engine.declare("inc-1", ROSTER, T0)
        self.engine.advance(T0 + 2 * ROUND_SECONDS)  # 轮次 0..2 已生成并投递
        # 用一个新收件箱验证：确认前已提交的通知仍会交付
        inbox2_db = os.path.join(self._tmp.name, "inbox2.sqlite")
        inbox2 = Inbox(inbox2_db)
        self.addCleanup(inbox2.close)
        engine2 = EscalationEngine(os.path.join(self._tmp.name, "engine2.sqlite"), inbox2.deliver)
        self.addCleanup(engine2.close)

        engine2.declare("inc-2", ROSTER, T0)
        engine2.advance(T0 + 2 * ROUND_SECONDS)  # 生成并投递轮次 0..2
        self.assertTrue(engine2.acknowledge("inc-2", 2, "req-1", T0 + 2 * ROUND_SECONDS + 5))
        # 确认后不再生成新轮次
        engine2.advance(T0 + 100 * ROUND_SECONDS)
        self.assertEqual(engine2.inspect("inc-2").current_round, 2)
        self.assertEqual([r.round for r in inbox2.messages("inc-2")], [0, 1, 2])

        # 场景：通知已提交但尚未交付时确认（直接 declare 后立刻 ack，未 advance）
        inbox3_db = os.path.join(self._tmp.name, "inbox3.sqlite")
        inbox3 = Inbox(inbox3_db)
        self.addCleanup(inbox3.close)
        engine3 = EscalationEngine(os.path.join(self._tmp.name, "engine3.sqlite"), inbox3.deliver)
        self.addCleanup(engine3.close)
        engine3.declare("inc-3", ROSTER, T0)  # 轮次 0 通知已提交，未交付
        self.assertTrue(engine3.acknowledge("inc-3", 0, "req-1", T0 + 1))
        engine3.advance(T0 + 10)  # 此前已提交的通知仍须完成交付
        self.assertEqual([r.round for r in inbox3.messages("inc-3")], [0])


class TestInbox(EscalationTestCase):
    def test_duplicate_message_id_kept_once(self):
        self.engine.declare("inc-1", ROSTER, T0)
        self.engine.advance(T0)
        self.engine.advance(T0)  # 重复推进不重复投递
        records = self.inbox.messages("inc-1")
        self.assertEqual(len(records), 1)
        # 直接重复 deliver 同一 message_id 也只保留一条
        from escalation.inbox import Message
        msg = Message("inc-1:0", "inc-1", 0, "alice", T0)
        self.inbox.deliver(msg)
        self.assertEqual(len(self.inbox.messages("inc-1")), 1)

    def test_message_id_stable_across_retries(self):
        self.engine.declare("inc-1", ROSTER, T0)
        self.engine.advance(T0)
        first = self.inbox.messages("inc-1")[0].message_id
        self.engine.advance(T0 + ROUND_SECONDS)
        records = self.inbox.messages("inc-1")
        self.assertEqual(records[0].message_id, first)
        self.assertEqual(records[0].message_id, "inc-1:0")


if __name__ == "__main__":
    unittest.main()

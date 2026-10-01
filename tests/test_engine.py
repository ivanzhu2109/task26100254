import os
import tempfile
import unittest

from escalation import EscalationEngine, Inbox
from escalation.engine import ROUND_SECONDS

T0 = 1_700_000_000
ROSTER = ["alice", "bob", "carol"]


class EngineTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp()
        self.inbox = Inbox(os.path.join(self.dir, "inbox.sqlite"))
        self.engine = EscalationEngine(os.path.join(self.dir, "engine.sqlite"), self.inbox)

    def tearDown(self) -> None:
        self.engine.close()
        self.inbox.close()

    def declare(self, incident_id="inc", roster=ROSTER, at=T0):
        return self.engine.declare(incident_id, roster, at)


class DeclareTest(EngineTestCase):
    def test_declare_generates_round_zero(self):
        state = self.declare()
        self.assertEqual(state.current_round, 0)
        self.assertFalse(state.acknowledged)
        _, timeline = self.engine.inspect("inc")
        types = [e.type for e in timeline]
        self.assertEqual(types, ["declared", "notification_committed"])

    def test_declare_retry_does_not_reset(self):
        self.declare()
        self.engine.advance(T0 + 5 * ROUND_SECONDS)
        state = self.engine.declare("inc", ["mallory"], at=T0 + 99_999)
        self.assertEqual(state.roster, tuple(ROSTER))
        self.assertEqual(state.declared_at, T0)
        self.assertEqual(state.current_round, 5)

    def test_declare_rejects_empty_or_duplicate_roster(self):
        with self.assertRaises(ValueError):
            self.engine.declare("empty", [], T0)
        with self.assertRaises(ValueError):
            self.engine.declare("dup", ["a", "a"], T0)


class AdvanceCatchUpTest(EngineTestCase):
    def test_stall_then_catch_up_no_gap_no_dup_no_drift(self):
        self.declare()
        # scheduler stalls for 7 full periods plus a bit
        self.engine.advance(T0 + 7 * ROUND_SECONDS + 123)
        msgs = self.inbox.list_messages("inc")
        self.assertEqual([m.round for m in msgs], list(range(8)))
        self.assertEqual([m.recipient for m in msgs], [ROSTER[r % 3] for r in range(8)])
        for m in msgs:
            self.assertEqual(m.scheduled_at, T0 + m.round * ROUND_SECONDS)
            self.assertEqual(m.deadline, m.scheduled_at + ROUND_SECONDS)
            self.assertEqual(m.message_id, f"inc#{m.round}")
        # advancing again must not duplicate anything
        self.engine.advance(T0 + 7 * ROUND_SECONDS + 200)
        self.assertEqual(len(self.inbox.list_messages("inc")), 8)

    def test_incremental_advances_match_bulk_catch_up(self):
        self.declare()
        for step in range(1, 6):
            self.engine.advance(T0 + step * ROUND_SECONDS)
        msgs = self.inbox.list_messages("inc")
        self.assertEqual([m.round for m in msgs], [0, 1, 2, 3, 4, 5])

    def test_advance_before_first_round_end_only_delivers_round_zero(self):
        self.declare()
        self.engine.advance(T0 + 1)
        msgs = self.inbox.list_messages("inc")
        self.assertEqual([m.round for m in msgs], [0])


class AcknowledgeTest(EngineTestCase):
    def test_ack_success(self):
        self.declare()
        self.assertTrue(self.engine.acknowledge("inc", 0, "req-1", T0 + 1))
        state, timeline = self.engine.inspect("inc")
        self.assertTrue(state.acknowledged)
        self.assertEqual(state.ack_round, 0)
        ack_events = [e for e in timeline if e.type == "acknowledged"]
        self.assertEqual(len(ack_events), 1)

    def test_ack_one_second_before_deadline_succeeds(self):
        self.declare()
        self.assertTrue(
            self.engine.acknowledge("inc", 0, "req-1", T0 + ROUND_SECONDS - 1)
        )

    def test_ack_exactly_at_deadline_fails_even_before_advance(self):
        self.declare()
        # advance has NOT run; round 1 is not generated yet, but round 0
        # must already be expired at its exact deadline.
        self.assertFalse(self.engine.acknowledge("inc", 0, "req-1", T0 + ROUND_SECONDS))

    def test_ack_wrong_round_fails(self):
        self.declare()
        self.assertFalse(self.engine.acknowledge("inc", 1, "req-1", T0 + 1))
        self.assertFalse(self.engine.acknowledge("inc", 7, "req-2", T0 + 1))

    def test_ack_unknown_incident_fails(self):
        self.assertFalse(self.engine.acknowledge("nope", 0, "req-1", T0))

    def test_ack_retry_returns_first_committed_result_after_expiry(self):
        self.declare()
        self.assertTrue(self.engine.acknowledge("inc", 0, "req-1", T0 + 10))
        # far in the future: round 0 long expired, incident already acked
        self.assertTrue(
            self.engine.acknowledge("inc", 0, "req-1", T0 + 100 * ROUND_SECONDS)
        )

    def test_failed_ack_retry_returns_false(self):
        self.declare()
        self.assertFalse(self.engine.acknowledge("inc", 0, "req-1", T0 + ROUND_SECONDS))
        # retry inside the original window still returns the committed failure
        self.assertFalse(self.engine.acknowledge("inc", 0, "req-1", T0 + 1))

    def test_second_ack_after_success_fails(self):
        self.declare()
        self.assertTrue(self.engine.acknowledge("inc", 0, "req-1", T0 + 1))
        self.assertFalse(self.engine.acknowledge("inc", 0, "req-2", T0 + 2))
        _, timeline = self.engine.inspect("inc")
        self.assertEqual(len([e for e in timeline if e.type == "acknowledged"]), 1)


class RosterCycleTest(EngineTestCase):
    def test_old_round_stays_invalid_when_recipient_repeats(self):
        self.declare()
        # let the roster cycle fully: rounds 0..2 generated, round 0 expired
        self.engine.advance(T0 + 2 * ROUND_SECONDS + 1)
        # round 0 and round 3 share recipient alice; round 0 is history
        self.assertFalse(self.engine.acknowledge("inc", 0, "req-old", T0 + 2 * ROUND_SECONDS + 2))
        # now generate round 3 (alice again) and ack it inside its window
        self.engine.advance(T0 + 3 * ROUND_SECONDS + 1)
        self.assertFalse(
            self.engine.acknowledge("inc", 0, "req-old-2", T0 + 3 * ROUND_SECONDS + 2)
        )
        self.assertTrue(
            self.engine.acknowledge("inc", 3, "req-new", T0 + 3 * ROUND_SECONDS + 2)
        )

    def test_full_cycle_recipients(self):
        self.declare()
        self.engine.advance(T0 + 5 * ROUND_SECONDS + 1)
        msgs = self.inbox.list_messages("inc")
        self.assertEqual(
            [m.recipient for m in msgs],
            ["alice", "bob", "carol", "alice", "bob", "carol"],
        )


class PostAckTest(EngineTestCase):
    def test_no_new_rounds_after_ack_but_pending_notifications_delivered(self):
        self.declare()
        # generate rounds 0..2 but they stay undelivered only if advance
        # never runs; advance both generates and delivers, so ack first.
        self.engine.advance(T0 + 2 * ROUND_SECONDS)
        self.assertTrue(
            self.engine.acknowledge("inc", 2, "req-1", T0 + 2 * ROUND_SECONDS + 5)
        )
        self.engine.advance(T0 + 30 * ROUND_SECONDS)
        state, _ = self.engine.inspect("inc")
        self.assertEqual(state.current_round, 2)
        msgs = self.inbox.list_messages("inc")
        self.assertEqual([m.round for m in msgs], [0, 1, 2])

    def test_committed_before_ack_still_delivered(self):
        self.declare()
        self.engine.advance(T0 + ROUND_SECONDS)  # rounds 0,1 committed+delivered
        self.assertTrue(self.engine.acknowledge("inc", 1, "req-1", T0 + ROUND_SECONDS + 1))
        self.engine.advance(T0 + 50 * ROUND_SECONDS)
        self.assertEqual(len(self.inbox.list_messages("inc")), 2)


class InboxTest(EngineTestCase):
    def test_deliver_dedupes_by_message_id(self):
        from escalation import Message

        msg = Message("m1", "inc", 0, "alice", T0, T0 + ROUND_SECONDS)
        self.inbox.deliver(msg)
        self.inbox.deliver(msg)
        self.assertEqual(len(self.inbox.list_messages()), 1)
        self.assertEqual(self.inbox.get("m1"), msg)
        self.assertIsNone(self.inbox.get("missing"))

    def test_message_ids_stable_across_advances(self):
        self.declare()
        self.engine.advance(T0 + ROUND_SECONDS)
        first = {m.message_id for m in self.inbox.list_messages("inc")}
        self.engine.advance(T0 + ROUND_SECONDS + 1)
        second = {m.message_id for m in self.inbox.list_messages("inc")}
        self.assertEqual(first, second)


class InspectTest(EngineTestCase):
    def test_timeline_is_ordered_and_matches_inbox(self):
        self.declare()
        self.engine.advance(T0 + 3 * ROUND_SECONDS + 7)
        self.assertTrue(
            self.engine.acknowledge("inc", 3, "req-1", T0 + 3 * ROUND_SECONDS + 9)
        )
        state, timeline = self.engine.inspect("inc")
        seqs = [e.seq for e in timeline]
        self.assertEqual(seqs, sorted(seqs))
        committed = [e for e in timeline if e.type == "notification_committed"]
        self.assertEqual([e.round for e in committed], [0, 1, 2, 3])
        msgs = self.inbox.list_messages("inc")
        for event, msg in zip(committed, msgs):
            self.assertEqual(event.at, msg.scheduled_at)
            self.assertEqual(event.round, msg.round)
        self.assertTrue(state.acknowledged)

    def test_inspect_unknown_incident_raises(self):
        with self.assertRaises(KeyError):
            self.engine.inspect("nope")


class PersistenceTest(EngineTestCase):
    def test_state_survives_reopen(self):
        self.declare()
        self.engine.advance(T0 + ROUND_SECONDS)
        self.engine.close()
        self.inbox.close()

        self.inbox = Inbox(os.path.join(self.dir, "inbox.sqlite"))
        self.engine = EscalationEngine(os.path.join(self.dir, "engine.sqlite"), self.inbox)
        state, timeline = self.engine.inspect("inc")
        self.assertEqual(state.current_round, 1)
        self.assertEqual(len(self.inbox.list_messages("inc")), 2)
        self.assertEqual(timeline[0].type, "declared")


if __name__ == "__main__":
    unittest.main()

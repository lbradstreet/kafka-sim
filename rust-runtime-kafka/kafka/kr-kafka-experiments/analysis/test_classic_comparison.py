"""Independent corruption checks for the classic comparison evidence reader."""
import copy
import unittest

from classic_comparison import validate


def fixture():
    origin = 9_223_372_036_854_775_801
    action = {"at_ns": 2_000_000_000, "action": {"MoveLeader": {"topic": 0, "partition": 0, "broker": 2}}}
    entries = [
        {"now_ns": origin, "event": {"ConnectionOpened": {"connection": 1, "broker": 1}}},
        {"now_ns": origin + 1_000_000_000, "event": {"FaultDecision": {"hook": {
            "now_ns": 1_000_000_000, "phase": "BeforeAppend", "api": 0, "broker": 1}}}},
        {"now_ns": origin + 1_100_000_000, "event": {"BrokerCommit": {"connection": 1, "records": 1}}},
        {"now_ns": origin + 2_000_000_000, "event": {"ScheduledControl": copy.deepcopy(action)}},
    ]
    return {
        "schema": "kr-classic-comparison/v1", "replay_verified": True,
        "offered": 3, "accepted": 1, "refused": 2, "acked": 1, "failed": 0,
        "manifest": {"start_ns": origin, "experiment": {
            "loads": [{"template": {"first_id": 1}, "shape": {"OpenLoop": {
                "start_ns": 0, "end_ns": 3_000_000_000, "rate_per_s": 1}}}],
            "scheduled_actions": [action], "polling_pauses": []},
            "faults": {"environment": [{"start_ns": 0, "end_ns": 2_000_000_000,
                "phase": "BeforeAppend", "api": 0, "broker": 1, "probability_ppm": 1_000_000}]}},
        "source_evidence": [{"load": 0, "reserved": 3, "offered": 3, "cancelled": 0, "accepted": 1, "refused": 2}],
        "external_history": [{"kind": "offer", "load": 0, "id": i + 1, "at_ns": i * 1_000_000_000,
                              "due_ns": i * 1_000_000_000} for i in range(3)]
                            + [{"kind": "admission", "id": 1, "at_ns": 0, "accepted": True},
                               {"kind": "consumed", "id": 1, "at_ns": 1_200_000_000}],
        "deliveries": [{"id": 1, "success": True, "partition": 0, "offset": 0, "at_ns": 1_200_000_000}],
        "environment": {"now_ns": 3_000_000_000, "history": {"entries": entries},
                        "fault_stats": {"decisions": 1, "environment_firings": [1]},
                        "log": [{"id": 1, "topic_id": [1] * 16, "partition": 0, "offset": 0}]},
    }


class ComparisonEvidenceTest(unittest.TestCase):
    def test_full_width_clock_and_exact_source_schedule(self):
        result = validate(fixture())
        self.assertEqual([], result["coverage_gaps"])
        self.assertEqual(1_200_000_000, result["ack_latency_p99_ns"])

    def test_corruptions_are_rejected(self):
        mutations = [
            lambda r: r["environment"]["history"]["entries"].pop(1),
            lambda r: r["environment"]["history"]["entries"][-1].update(now_ns=r["manifest"]["start_ns"] + 2_000_000_001),
            lambda r: r["external_history"][1].update(due_ns=999_999_999),
            lambda r: r["deliveries"][0].update(offset=1),
            lambda r: r.update(replay_verified=False),
            lambda r: r["environment"]["log"].append(copy.deepcopy(r["environment"]["log"][0])),
            lambda r: r["deliveries"].append(copy.deepcopy(r["deliveries"][0])),
            lambda r: r["deliveries"].clear(),
            lambda r: r["environment"]["log"][0].update(id=99),
            lambda r: r["deliveries"][0].update(callback_ns=1_200_000_001),
            lambda r: r["manifest"]["experiment"]["polling_pauses"].append({"start_ns":1_000_000_000, "end_ns":2_000_000_000}),
        ]
        for mutation in mutations:
            report = fixture()
            mutation(report)
            with self.assertRaises(ValueError):
                validate(report)

    def test_absent_fault_opportunity_is_explicitly_not_comparable(self):
        report = fixture()
        report["manifest"]["faults"]["environment"][0]["broker"] = 2
        report["environment"]["fault_stats"]["environment_firings"] = [0]
        self.assertEqual(["environment rule 0 has no matching opportunity"], validate(report)["coverage_gaps"])


if __name__ == "__main__":
    unittest.main()

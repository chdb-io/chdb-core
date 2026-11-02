#!/usr/bin/env python3

import datetime as dt
import importlib.util
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).with_name("rerun-killed-jobs.py")
SPEC = importlib.util.spec_from_file_location("rerun_killed_jobs", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

KILLED_LOG = (
    "2026-11-02T06:04:55.6913493Z ##[error]The runner has received a shutdown signal. "
    "This can happen when the runner service is stopped, or a manually started runner is canceled.\n"
)
FAILED_LOG = "2026-11-02T03:00:00.0000000Z ##[error]Process completed with exit code 1.\n"


def at(hour, minute, second=0):
    return dt.datetime(2026, 11, 2, hour, minute, second, tzinfo=dt.timezone.utc)


def run(**overrides):
    value = {
        "id": 100, "name": "Build & Test Linux x86_64", "workflow_id": 7, "run_attempt": 1,
        "status": "completed", "conclusion": "failure", "event": "pull_request",
        "head_branch": "sync/clickhouse-master", "head_sha": "abc123",
        "head_commit": {"message": "Resolve the sync's conflicts"}, "html_url": "https://example/runs/100",
        "head_repository": {"full_name": "chdb-io/chdb-core"},
    }
    value.update(overrides)
    return value


class FakeGitHub:
    def __init__(self, runs, jobs, logs, newest_id=100, open_heads=(("chdb-io/chdb-core", "sync/clickhouse-master"),)):
        self.runs = list(runs)      # successive answers for the run, the last one repeats
        self._jobs = jobs
        self.logs = logs
        self.newest_id = newest_id
        self.open_heads = set(open_heads)
        self.reruns = []
        self.dispatches = []

    def get(self, path):
        if path.startswith("actions/workflows/"):
            return {"workflow_runs": [{"id": self.newest_id}]}
        return self.runs.pop(0) if len(self.runs) > 1 else self.runs[0]

    def jobs(self, run_id, attempt):
        return self._jobs

    def log(self, job_id):
        return self.logs.get(job_id)

    def open_pull_heads(self):
        return self.open_heads

    def rerun_failed(self, run_id):
        self.reruns.append(run_id)

    def dispatch(self, workflow, ref, inputs):
        self.dispatches.append((workflow, ref, inputs))


BUILD = {"id": 1, "name": "Build & Test Universal Wheel (Linux x86_64)", "conclusion": "failure"}
CROSS = {"id": 2, "name": "Build on Linux (cross-compile for macOS x86_64)", "conclusion": "failure"}
TEST = {"id": 3, "name": "Test on macOS x86_64", "conclusion": "skipped"}


class Clock:
    def __init__(self, start):
        self.now = start
        self.slept = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += dt.timedelta(seconds=seconds)


def decide(api, clock, dry_run=False):
    return MODULE.decide(api, 100, "chdb-io", "main", dry_run, clock, clock.sleep)


class RestartWindowTest(unittest.TestCase):
    def test_window(self):
        self.assertEqual(MODULE.seconds_to_wait(at(5, 59, 59)), 0)
        self.assertEqual(MODULE.seconds_to_wait(at(6, 0)), 70 * 60)
        self.assertEqual(MODULE.seconds_to_wait(at(6, 30)), 40 * 60)
        self.assertEqual(MODULE.seconds_to_wait(at(7, 9, 59)), 1)
        self.assertEqual(MODULE.seconds_to_wait(at(7, 10)), 0)


class DecideTest(unittest.TestCase):
    def test_reruns_when_every_failed_job_was_killed(self):
        api = FakeGitHub([run()], [CROSS, TEST], {2: KILLED_LOG})
        clock = Clock(at(9, 0))
        self.assertEqual(decide(api, clock), {"rerun": "true", "dispatch": ""})
        self.assertEqual(api.reruns, [100])
        self.assertEqual(clock.slept, [])

    def test_leaves_a_real_failure_alone(self):
        api = FakeGitHub([run()], [BUILD, CROSS], {1: FAILED_LOG, 2: KILLED_LOG})
        self.assertEqual(decide(api, Clock(at(9, 0)))["rerun"], "false")
        self.assertEqual(api.reruns, [])

    def test_a_missing_log_is_not_a_kill(self):
        api = FakeGitHub([run()], [CROSS], {})
        self.assertEqual(decide(api, Clock(at(9, 0)))["rerun"], "false")

    def test_only_a_failed_first_attempt(self):
        for state in ({"run_attempt": 2}, {"conclusion": "cancelled"}, {"conclusion": "success"}):
            api = FakeGitHub([run(**state)], [CROSS], {2: KILLED_LOG})
            self.assertEqual(decide(api, Clock(at(9, 0)))["rerun"], "false", state)

    def test_waits_out_the_window(self):
        api = FakeGitHub([run()], [CROSS], {2: KILLED_LOG})
        clock = Clock(at(6, 21))
        self.assertEqual(decide(api, clock)["rerun"], "true")
        self.assertEqual(clock.slept, [49 * 60])

    def test_someone_reran_it_during_the_wait(self):
        api = FakeGitHub([run(), run(run_attempt=2, status="in_progress")], [CROSS], {2: KILLED_LOG})
        self.assertEqual(decide(api, Clock(at(6, 21)))["rerun"], "false")
        self.assertEqual(api.reruns, [])

    def test_a_superseded_run_is_left_alone(self):
        api = FakeGitHub([run()], [CROSS], {2: KILLED_LOG}, newest_id=200)
        self.assertEqual(decide(api, Clock(at(9, 0)))["rerun"], "false")
        self.assertEqual(api.reruns, [])

    def test_a_merged_pull_request_is_left_alone(self):
        api = FakeGitHub([run()], [CROSS], {2: KILLED_LOG}, open_heads=())
        self.assertEqual(decide(api, Clock(at(9, 0)))["rerun"], "false")
        self.assertEqual(api.reruns, [])

    def test_a_fork_pull_request_is_matched_by_its_head(self):
        fork = run(head_branch="ci-fix", head_repository={"full_name": "someone/chdb-core-fork"})
        api = FakeGitHub([fork], [CROSS], {2: KILLED_LOG}, open_heads=(("someone/chdb-core-fork", "ci-fix"),))
        self.assertEqual(decide(api, Clock(at(9, 0)))["rerun"], "true")

    def test_a_nightly_is_rerun_even_after_main_moved_on(self):
        nightly = run(event="push", head_branch="main",
                      head_commit={"message": "Merge pull request #307 from chdb-io/sync/clickhouse-master"})
        api = FakeGitHub([nightly], [BUILD], {1: KILLED_LOG}, newest_id=200)
        self.assertEqual(decide(api, Clock(at(9, 0))),
                         {"rerun": "true", "dispatch": "nightly_publish.yml head_sha=abc123"})

    def test_dry_run_neither_waits_nor_reruns(self):
        api = FakeGitHub([run()], [CROSS], {2: KILLED_LOG})
        clock = Clock(at(6, 21))
        self.assertEqual(decide(api, clock, dry_run=True)["rerun"], "false")
        self.assertEqual((api.reruns, clock.slept), ([], []))


class FollowUpTest(unittest.TestCase):
    def test_follow_up(self):
        self.assertEqual(MODULE.follow_up(run(event="push", head_branch="main"), "main"),
                         "nightly_publish.yml head_sha=abc123")
        self.assertEqual(MODULE.follow_up(run(event="release", head_branch="v26.11.1"), "main"),
                         "publish_checksums.yml TAG_NAME=v26.11.1")
        self.assertEqual(MODULE.follow_up(run(), "main"), "")
        self.assertEqual(MODULE.follow_up(run(event="push", head_branch="main",
                                              name="Build & Test Free-Threading Wheels"), "main"), "")

    def test_dispatches_once_the_rerun_ends(self):
        api = FakeGitHub([run(run_attempt=2, status="in_progress"),
                          run(run_attempt=2, status="completed", conclusion="success")], [], {})
        clock = Clock(at(7, 10))
        result = MODULE.follow(api, 100, "nightly_publish.yml head_sha=abc123", "main", 60, clock, clock.sleep)
        self.assertEqual(result, {"done": "true"})
        self.assertEqual(api.dispatches, [("nightly_publish.yml", "main", {"head_sha": "abc123"})])

    def test_hands_over_at_the_deadline(self):
        api = FakeGitHub([run(run_attempt=2, status="in_progress")], [], {})
        clock = Clock(at(7, 10))
        result = MODULE.follow(api, 100, "nightly_publish.yml head_sha=abc123", "main", 10, clock, clock.sleep)
        self.assertEqual(result, {"done": "false"})
        self.assertEqual(api.dispatches, [])


if __name__ == "__main__":
    unittest.main()

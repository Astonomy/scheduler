"""Stage routing and orchestration contracts; never call a real model."""
from datetime import datetime, timedelta, timezone
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from reqflow_scheduler import dispatch, routing, stages
from reqflow_scheduler.cli import main
from reqflow_scheduler.scheduler import continue_task, fingerprint, load_task, tick
from reqflow_scheduler.state_store import lock, read, write


ROUTES = {"version": 1, "analysis": {"provider": "codex", "model": "gpt-6-astra", "reasoning_effort": "high"},
          "implementation": {"provider": "codex", "profile": "deepseek", "model": "deepseek-flash"},
          "verification": {"provider": "codex", "model": "gpt-6.1-sol", "reasoning_effort": "medium"}}


def result(stage, status="PASS", rework="implementation"):
    if stage == "analysis":
        value = {key: ["evidence"] for key in stages.ANALYSIS_FIELDS}
        value["repair_design"] = [dict(issue="Invalid output escapes the repair gate",
            root_cause="The acceptance branch returns before checking persisted output",
            why_existing_checks_missed="The existing regression only asserts that the command exits",
            evidence=["agent.py:validate_output returns True before validating the response"],
            changes=[dict(file="agent.py", symbol="validate_output", current_behavior="Accepts invalid output",
                          proposed_change="Validate the response before returning success; feed failures into repair")],
            regression_tests=[dict(file="tests/test_agent.py", command="python -m unittest discover -s tests -p test_agent.py",
                                   expected_before="Invalid output is accepted and the regression fails",
                                   expected_after="Invalid output triggers repair and valid output remains accepted")])]
        return value
    if stage == "implementation":
        return dict(status="IMPLEMENTED", changes=["change"], tests=["test"], logs=[], issues=[])
    return dict(status=status, requirement_coverage=["checked"], correctness=["checked"],
                test_results=["independent test"], identified_issues=[] if status == "PASS" else ["bug"],
                suggested_fixes=[] if status == "PASS" else ["fix"], rework_stage=rework)


class StageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = datetime(2026, 11, 11, 18, tzinfo=timezone.utc)
        self.task = dict(version=1, id="staged", workspace=str(self.root), schedule_policy="continuous",
                         tree=dict(id="root", prompt="implement feature"), model_routing=copy.deepcopy(ROUTES),
                         orchestration=dict(version=1, max_calls=12, max_cycles=3, max_stage_attempts=2))
        self.quota = patch("reqflow_scheduler.quota_adapter.query", return_value=dict(
            remaining_percent=90, blocked_until=None, resets_at=(self.now + timedelta(hours=2)).isoformat())).start()
        patch("reqflow_scheduler.routing.resolve", side_effect=lambda task, stage: {
            **task["model_routing"][stage], "model_provider": "deepseek" if stage == "implementation" else "openai"}).start()
        self.addCleanup(patch.stopall)
        self.calls = []

    def fake(self, statuses=()):
        statuses = iter(statuses)
        def run(task, ns, save, folder, stop):
            stage = ns["stage"]
            self.calls.append((stage, task, ns.get("session_id")))
            value = result(stage, *next(statuses, ("PASS", "implementation"))) if stage == "verification" else result(stage)
            ns.update(status="completed", session_id="session-" + str(len(self.calls)),
                      final_message=json.dumps(value), usage=dict(input_tokens=10, output_tokens=5),
                      elapsed_seconds=0.1, actual_model=task["route"]["model"],
                      tool_results=[dict(command="python -m unittest", exit_code=0, aggregated_output="OK")])
            save(ns)
        return run

    def execute(self, statuses=()):
        with patch("reqflow_scheduler.dispatch.run", side_effect=self.fake(statuses)):
            return tick(self.task, self.root / "state", self.now)

    def test_normal_flow_handoff_and_independent_sessions(self):
        state = self.execute()
        self.assertEqual(state["status"], "completed")
        self.assertEqual([c[0] for c in self.calls], ["analysis", "implementation", "verification"])
        self.assertTrue(all(c[2] is None for c in self.calls))
        self.assertIn("implementation_plan", self.calls[1][1]["prompt"])
        self.assertIn("independently", self.calls[2][1]["prompt"])
        self.assertEqual(state["usage"]["total"]["calls"], 3)
        self.assertEqual(state["usage"]["by_stage"]["implementation"]["tokens"], 15)

    def test_located_repair_design_reaches_implementation_and_verification(self):
        state = self.execute()
        analysis = result("analysis")
        self.assertEqual(state["nodes"]["root"]["results"]["analysis"], analysis)
        for index in (1, 2):
            prepared = self.calls[index][1]
            folder = Path(prepared["schema_path"]).parent
            self.assertEqual(read(folder / "context.json")["analysis"]["repair_design"], analysis["repair_design"])
            self.assertIn("validate_output", prepared["prompt"])
            self.assertIn("expected_before", prepared["prompt"])
        self.assertEqual(self.calls[0][1]["route"]["model"], "gpt-6-astra")
        self.assertEqual(self.calls[1][1]["route"]["model"], "deepseek-flash")

    def test_incomplete_analysis_never_dispatches_implementation(self):
        complete = result("analysis")
        invalid = []
        legacy = copy.deepcopy(complete)
        legacy.pop("repair_design")
        invalid.append(legacy)
        for key in ("issue", "root_cause", "why_existing_checks_missed", "evidence", "changes", "regression_tests"):
            value = copy.deepcopy(complete)
            value["repair_design"][0][key] = [] if isinstance(value["repair_design"][0][key], list) else " "
            invalid.append(value)
        for group, key in (("changes", "file"), ("changes", "symbol"), ("changes", "proposed_change"),
                           ("regression_tests", "command"), ("regression_tests", "expected_before"),
                           ("regression_tests", "expected_after")):
            value = copy.deepcopy(complete)
            value["repair_design"][0][group][0][key] = ""
            invalid.append(value)
        invalid.append({**complete, "repair_design": []})
        for index, value in enumerate(invalid):
            with self.subTest(case=index):
                self.task["id"] = f"incomplete-{index}"
                self.calls.clear()
                normal = self.fake()
                def run(task, ns, save, folder, stop):
                    normal(task, ns, save, folder, stop)
                    ns["final_message"] = json.dumps(value)
                with patch("reqflow_scheduler.dispatch.run", side_effect=run):
                    state = tick(self.task, self.root / "state", self.now)
                self.assertEqual(state["status"], "blocked")
                self.assertEqual([call[0] for call in self.calls], ["analysis", "analysis"])
                self.assertTrue(state["nodes"]["root"]["output_error"])
                self.assertIn("previous_output_error", self.calls[1][1]["prompt"])

    def test_analysis_without_source_inspection_does_not_handoff(self):
        normal = self.fake()
        def run(task, ns, save, folder, stop):
            normal(task, ns, save, folder, stop)
            ns["tool_results"] = []
        with patch("reqflow_scheduler.dispatch.run", side_effect=run):
            state = tick(self.task, self.root / "state", self.now)
        self.assertEqual(state["status"], "blocked")
        self.assertIn("source inspection", state["nodes"]["root"]["output_error"])
        self.assertEqual([call[0] for call in self.calls], ["analysis", "analysis"])

    def test_analysis_completes_design_before_deepseek_is_dispatched(self):
        normal = self.fake()
        def run(task, ns, save, folder, stop):
            normal(task, ns, save, folder, stop)
            if len(self.calls) == 1:
                value = result("analysis")
                value["repair_design"] = []
                value["risks"] = ["Missing the source trace for the failed acceptance branch"]
                ns["final_message"] = json.dumps(value)
        with patch("reqflow_scheduler.dispatch.run", side_effect=run):
            state = tick(self.task, self.root / "state", self.now)
        self.assertEqual(state["status"], "completed")
        self.assertEqual([call[0] for call in self.calls], ["analysis", "analysis", "implementation", "verification"])
        self.assertIn("continue investigation before implementation", self.calls[1][1]["prompt"])
        self.assertEqual(state["nodes"]["root"]["stage_calls"][0]["status"], "invalid_output")
        self.assertEqual(self.calls[2][1]["route"]["model"], "deepseek-flash")

    def test_recovery_retries_local_startup_failure_without_repeating_analysis(self):
        normal = self.fake()
        def startup_failure(task, ns, save, folder, stop):
            if ns['stage'] == 'analysis':
                return normal(task, ns, save, folder, stop)
            (folder / 'events.jsonl').write_bytes(b'')
            (folder / 'stderr.log').write_text('Error: missing catalog (os error 2)\n')
            ns.update(status='interrupted', session_id=None, pid=None, exit_code=1, reason='startup failed')
            save(ns)
        with patch('reqflow_scheduler.dispatch.run', side_effect=startup_failure):
            saved = tick(self.task, self.root / 'state', self.now)
        analysis = copy.deepcopy(saved['nodes']['root']['results']['analysis'])
        continued = continue_task(self.task, self.root / 'state')
        self.assertEqual(continued['nodes']['root']['results']['analysis'], analysis)
        self.assertIn('no session was created', continued['nodes']['root']['stage_calls'][-1]['recovery'])
        final = self.execute()
        self.assertEqual(final['status'], 'completed')
        self.assertEqual([c[0] for c in self.calls], ['analysis', 'implementation', 'verification'])
        self.assertEqual(final['usage']['total']['calls'], 4)

    def test_recovery_does_not_guess_missing_sessions_from_unknown_failures(self):
        folder = self.root / 'attempt'
        folder.mkdir()
        node = dict(status='interrupted', stage='implementation', attempts=2, exit_code=1,
                    stage_calls=[dict(stage='implementation', run_dir=str(folder))])
        for events, error in [(b'', 'Error: authentication failed'),
                              (b'{"type":"thread.started","thread_id":"lost"}\n', 'Error: missing file (os error 2)')]:
            (folder / 'events.jsonl').write_bytes(events)
            (folder / 'stderr.log').write_text(error)
            saved = dict(status='interrupted', nodes={'root': node})
            path = self.root / 'state/staged/state.json'
            write(path, saved)
            with self.assertRaisesRegex(ValueError, 'no durable session'):
                continue_task(self.task, self.root / 'state')
            self.assertEqual(read(path), saved)

    def test_failed_verification_returns_to_implementation(self):
        state = self.execute([("FAIL", "implementation"), ("PASS", "implementation")])
        self.assertEqual(state["status"], "completed")
        self.assertEqual([c[0] for c in self.calls], ["analysis", "implementation", "verification", "implementation", "verification"])
        self.assertIn("bug", self.calls[3][1]["prompt"])

    def test_design_failure_returns_to_analysis(self):
        self.execute([("FAIL", "analysis"), ("PASS", "implementation")])
        self.assertEqual([c[0] for c in self.calls], ["analysis", "implementation", "verification"] * 2)

    def test_budget_and_cycle_limit_are_durable(self):
        self.task["orchestration"]["max_calls"] = 2
        state = self.execute()
        self.assertEqual(state["status"], "blocked")
        self.assertIn("budget", state["reason"])
        self.execute()
        self.assertEqual(len(self.calls), 2)
        self.task["id"] = "cycles"
        self.task["orchestration"].update(max_calls=30, max_cycles=1)
        state = self.execute([("FAIL", "implementation")])
        self.assertEqual(state["status"], "blocked")
        self.assertIn("cycle", state["reason"])

    def test_unavailable_model_blocks_without_fallback(self):
        with patch("reqflow_scheduler.routing.resolve", side_effect=ValueError("model unavailable")), patch("reqflow_scheduler.dispatch.run") as run:
            state = tick(self.task, self.root / "state", self.now)
        self.assertEqual(state["status"], "blocked")
        self.assertIn("model unavailable", state["reason"])
        run.assert_not_called()

    def test_implementation_configuration_is_checked_before_analysis_spends_tokens(self):
        def resolve(task, stage):
            if stage == "implementation": raise ValueError("implementation credential unavailable")
            return {**task["model_routing"][stage], "model_provider": "openai"}
        with patch("reqflow_scheduler.routing.resolve", side_effect=resolve), patch("reqflow_scheduler.dispatch.run") as run:
            state = tick(self.task, self.root / "state", self.now)
        self.assertEqual(state["status"], "blocked")
        self.assertIn("credential", state["reason"])
        run.assert_not_called()

    def test_quota_exhausted_prevents_next_stage(self):
        self.quota.side_effect = [self.quota.return_value, {**self.quota.return_value, "blocked_until": (self.now + timedelta(hours=2)).isoformat()}]
        state = self.execute()
        self.assertEqual(state["status"], "waiting")
        self.assertEqual(len(self.calls), 1)

    def test_invalid_output_is_bounded_and_never_passes(self):
        def malformed(task, ns, save, folder, stop):
            self.calls.append(ns["stage"])
            ns.update(status="completed", final_message="not json")
        with patch("reqflow_scheduler.dispatch.run", side_effect=malformed):
            state = tick(self.task, self.root / "state", self.now)
        self.assertEqual(state["status"], "blocked")
        self.assertEqual(len(self.calls), 2)
        self.assertIsNone(state["usage"]["total"]["tokens"])

    def test_feedback_preserves_evidence_and_deduplicates(self):
        self.execute()
        event = dict(version=1, task_id="staged", node_id="root", event_id="run-1", test_results={"failed": 2}, logs=["trace"], context={"url": "local"})
        first = stages.feedback(self.task, self.root / "state", event)
        self.assertEqual(first["nodes"]["root"]["stage"], "verification")
        self.assertEqual(first["nodes"]["root"]["feedback"][0], event)
        stages.feedback(self.task, self.root / "state", event)
        saved = read(self.root / "state/staged/state.json")
        self.assertEqual(len(saved["nodes"]["root"]["feedback"]), 1)
        self.assertEqual(saved["nodes"]["root"]["status"], "queued")

    def test_feedback_during_dispatch_is_durable_and_later_verified(self):
        self.execute()
        event = dict(version=1, task_id="staged", node_id="root", event_id="during-run", test_results={"failed": 1}, logs=["trace"], context={})
        path = self.root / "state/staged/state.json"
        with lock("task:" + str(path.resolve())):
            receipt = stages.feedback(self.task, self.root / "state", event)
        self.assertEqual(receipt["status"], "queued")
        self.assertTrue((path.parent / "feedback-inbox.json").exists())
        self.calls.clear()
        state = self.execute([("FAIL", "implementation"), ("PASS", "implementation")])
        self.assertEqual(state["status"], "completed")
        self.assertEqual([c[0] for c in self.calls], ["verification", "implementation", "verification"])
        self.assertEqual(state["nodes"]["root"]["feedback"], [event])

    def test_feedback_id_conflicts_and_cycle_limits_are_rejected(self):
        self.execute()
        event = dict(version=1, task_id="staged", node_id="root", event_id="run1", test_results={"failed": 1}, logs=[], context={})
        stages.feedback(self.task, self.root / "state", event)
        with self.assertRaisesRegex(ValueError, "different evidence"):
            stages.feedback(self.task, self.root / "state", {**event, "logs": ["changed"]})
        wrong = {**event, "task_id": "another"}
        with self.assertRaisesRegex(ValueError, "mismatch"):
            stages.feedback(self.task, self.root / "state", wrong)

    def test_route_drift_cannot_change_model_on_resumed_session(self):
        ns = dict(stage="implementation", session_id="saved", stage_calls=[dict(
            model="deepseek-flash", model_provider="deepseek", profile="deepseek", reasoning_effort=None)])
        with self.assertRaisesRegex(ValueError, "route changed"):
            stages.prepare(self.task, self.task["tree"], ns, self.root / "run", dict(
                model="deepseek-flash", model_provider="another", profile="deepseek"))

    def test_large_review_evidence_is_preserved_without_overflowing_handoff(self):
        raw = 'large tool evidence ' * 100000
        ns = dict(stage='verification', results={'analysis': result('analysis'),
                  'implementation': result('implementation')},
                  artifacts=[dict(stage='implementation', run_dir='previous-run',
                                  tool_results=[dict(command='test', exit_code=1, aggregated_output=raw)])],
                  feedback=[dict(event_id='run-1', logs=raw)])
        folder = self.root / 'review'
        with patch('reqflow_scheduler.stages.evidence', return_value={'diff': {'output': raw}}):
            prepared = stages.prepare(self.task, self.task['tree'], ns, folder)
        self.assertLess(len(prepared['prompt']), 100000)
        saved = read(folder / 'context.json')
        self.assertEqual(saved['prior_artifacts'][0]['tool_results'][0]['aggregated_output'], raw)
        self.assertEqual(saved['feedback'][0]['logs'], raw)
        self.assertEqual(saved['repository_evidence']['diff']['output'], raw)
        self.assertEqual(saved['original_requirements'], self.task['tree']['prompt'])
        self.assertIn('failed_command_count', prepared['prompt'])
        self.assertIn(str(folder / 'context.json').replace('\\', '\\\\'), prepared['prompt'])

    def test_rejected_first_turn_resumes_same_session_with_repaired_context(self):
        previous = self.root / 'rejected'
        previous.mkdir()
        (previous / 'stderr.log').write_text('Error: turn/start failed: {"input_error_code":"input_too_large"}')
        (previous / 'events.jsonl').write_text('{"type":"thread.started","thread_id":"exact-review"}\n')
        route = {**ROUTES['verification'], 'model_provider': 'openai'}
        ns = dict(stage='verification', session_id='exact-review',
                  stage_calls=[dict(**route, run_dir=str(previous))])
        repaired = stages.prepare(self.task, self.task['tree'], ns, self.root / 'retry', route)
        self.assertNotEqual(repaired['prompt'], '继续')
        self.assertIn('Verify independently', repaired['prompt'])
        self.assertEqual(ns['session_id'], 'exact-review')
        command = dispatch.command(repaired, ns['session_id'])
        self.assertIn('exact-review', command)
        (previous / 'events.jsonl').write_text('{"type":"turn.started"}\n')
        normal = stages.prepare(self.task, self.task['tree'], ns, self.root / 'normal', route)
        self.assertEqual(normal['prompt'], '继续')

    def test_missing_input_rejected_thread_restarts_only_unstarted_stage(self):
        calls = []
        for index, error in enumerate(['Error: {"input_error_code":"input_too_large"}',
                                       'Error: no rollout found for thread id empty-review']):
            folder = self.root / str(index)
            folder.mkdir()
            (folder / 'events.jsonl').write_text('{"type":"thread.started","thread_id":"empty-review"}\n' if index == 0 else '')
            (folder / 'stderr.log').write_text(error)
            calls.append(dict(stage='verification', session_id='empty-review', run_dir=str(folder)))
        node = dict(status='interrupted', stage='verification', session_id='empty-review',
                    attempts=2, pid=None, stage_calls=calls, results={'analysis': result('analysis')})
        path = self.root / 'state/staged/state.json'
        saved = dict(status='interrupted', nodes={'root': node})
        write(path, saved)
        resumed = continue_task(self.task, self.root / 'state')['nodes']['root']
        self.assertNotIn('session_id', resumed)
        self.assertEqual(resumed['stage'], 'verification')
        self.assertEqual(resumed['results'], node['results'])
        self.assertEqual([c['session_id'] for c in resumed['stage_calls']], ['empty-review', 'empty-review'])
        self.assertIn('no rollout', resumed['stage_calls'][-1]['recovery'])
        (self.root / '0/events.jsonl').write_text('{"type":"turn.started"}\n')
        write(path, saved)
        valid = continue_task(self.task, self.root / 'state')['nodes']['root']
        self.assertEqual(valid['session_id'], 'empty-review')

    def test_task_routes_load_relative_to_task_and_versions_validate(self):
        config = self.root / "routes.toml"
        config.write_text('[model_routing]\nversion=1\n' + '\n'.join(
            '[model_routing.' + stage + ']\n' + '\n'.join(k+'='+json.dumps(v) for k,v in route.items())
            for stage,route in ROUTES.items() if stage != "version"), encoding="utf-8")
        raw = {k:v for k,v in self.task.items() if k != "model_routing"}
        raw["routing_config"] = "routes.toml"
        file = self.root / "task.json"
        write(file, raw)
        self.assertEqual(load_task(file)["model_routing"], ROUTES)
        raw["orchestration"]["version"] = 2
        write(file, raw)
        with self.assertRaisesRegex(ValueError, "version"): load_task(file)

    def test_agenthook_artifacts_are_bound_to_original_run(self):
        folder = self.root / "artifacts"
        write(folder / "completion.json", dict(status="completed", run_id="run1"))
        write(folder / "run.json", dict(id="run1", requirement_id="upstream-task"))
        write(folder / "analysis.json", dict(run_id="run1", failed=1))
        write(folder / "logs.json", dict(stderr="trace"))
        event = stages.agenthook_event(self.task, folder / "completion.json")
        self.assertEqual(event["context"]["run"]["requirement_id"], "upstream-task")
        self.assertEqual(event["logs"]["stderr"], "trace")
        write(folder / "run.json", dict(id="different-run"))
        with self.assertRaisesRegex(ValueError, "IDs"): stages.agenthook_event(self.task, folder / "completion.json")

    def test_routing_changes_are_execution_changes_but_legacy_hash_is_stable(self):
        old = dict(workspace="x", tree={"id": "root"})
        import hashlib
        expected = hashlib.sha256(json.dumps({k: old.get(k) for k in ("workspace", "tree", "codex", "timeout_seconds")}, sort_keys=True).encode()).hexdigest()
        self.assertEqual(fingerprint(old), expected)
        before = fingerprint(self.task)
        self.task["model_routing"]["analysis"]["model"] = "unavailable"
        self.assertNotEqual(before, fingerprint(self.task))

    def test_command_routes_fresh_and_resumed_calls(self):
        task = {"route": dict(model="deepseek-flash", model_provider="deepseek", profile="deepseek"), "schema_path": "schema.json"}
        for session in (None, "exact-session"):
            cmd = dispatch.command(task, session)
            self.assertIn("deepseek", cmd)
            self.assertIn("deepseek-flash", cmd)
            self.assertIn("--output-schema", cmd)
            self.assertFalse(any("model_reasoning_effort" in s for s in cmd))

    def test_verification_pass_without_independent_command_is_rejected(self):
        fake = self.fake()
        def run(task, ns, save, folder, stop):
            fake(task, ns, save, folder, stop)
            if ns["stage"] == "verification": ns["tool_results"] = []
        with patch("reqflow_scheduler.dispatch.run", side_effect=run):
            state = tick(self.task, self.root / "state", self.now)
        self.assertEqual(state["status"], "blocked")
        self.assertIn("independent", state["nodes"]["root"]["output_error"])

    def test_token_budget_blocks_on_unknown_or_exhausted_usage(self):
        for usage in (None, dict(input_tokens=20, output_tokens=10)):
            self.task["id"] = "unknown" if usage is None else "exhausted"
            self.task["orchestration"]["max_tokens"] = 20
            fake = self.fake()
            def run(task, ns, save, folder, stop):
                fake(task, ns, save, folder, stop)
                ns["usage"] = usage
            with patch("reqflow_scheduler.dispatch.run", side_effect=run):
                state = tick(self.task, self.root / "state", self.now)
            self.assertEqual(state["status"], "blocked")
            self.assertIn("budget", state["reason"])

    def test_implementation_can_request_reanalysis(self):
        fake = self.fake()
        def run(task, ns, save, folder, stop):
            fake(task, ns, save, folder, stop)
            if len(self.calls) == 2:
                output = result("implementation")
                output.update(status="NEEDS_ANALYSIS", issues=["ambiguous requirement"])
                ns["final_message"] = json.dumps(output)
        with patch("reqflow_scheduler.dispatch.run", side_effect=run):
            state = tick(self.task, self.root / "state", self.now)
        self.assertEqual(state["status"], "completed")
        self.assertEqual([c[0] for c in self.calls], ["analysis", "implementation", "analysis", "implementation", "verification"])

    def test_quota_interruption_resumes_exact_stage_session(self):
        fake = self.fake()
        def run(task, ns, save, folder, stop):
            if not self.calls:
                self.calls.append((ns["stage"], task, ns.get("session_id")))
                ns.update(status="interrupted", reason="quota", session_id="interrupted-analysis")
            else:
                fake(task, ns, save, folder, stop)
        exhausted = {**self.quota.return_value, "blocked_until": (self.now + timedelta(hours=2)).isoformat()}
        self.quota.side_effect = [self.quota.return_value, exhausted]
        with patch("reqflow_scheduler.dispatch.run", side_effect=run):
            state = tick(self.task, self.root / "state", self.now)
        self.assertEqual(state["status"], "waiting")
        self.quota.side_effect = None
        self.quota.return_value["resets_at"] = (self.now + timedelta(hours=5)).isoformat()
        with patch("reqflow_scheduler.dispatch.run", side_effect=fake):
            state = tick(self.task, self.root / "state", self.now + timedelta(hours=2))
        self.assertEqual(state["status"], "completed")
        self.assertEqual(self.calls[1][2], "interrupted-analysis")
        self.assertEqual(self.calls[1][1]["prompt"], "继续")

    def test_parallel_siblings_cannot_overbook_calls(self):
        self.task["tree"]["children"] = [dict(id="a", prompt="a"), dict(id="b", prompt="b")]
        self.task["orchestration"]["max_calls"] = 4
        state = self.execute()
        self.assertEqual(state["status"], "blocked")
        self.assertEqual(len(self.calls), 4)

    def test_cli_feedback_usage_and_preflight_are_readable(self):
        import io
        from contextlib import redirect_stdout
        task_file = self.root / "task.json"
        write(task_file, self.task)
        out = io.StringIO()
        with redirect_stdout(out):
            self.assertEqual(main(["models", str(task_file)]), 0)
        self.assertEqual(json.loads(out.getvalue())["routes"]["implementation"]["profile"], "deepseek")
        self.execute()
        out = io.StringIO()
        with redirect_stdout(out):
            self.assertEqual(main(["usage", str(task_file), "--state-dir", str(self.root / "state")]), 0)
        self.assertEqual(json.loads(out.getvalue())["total"]["calls"], 3)

    def test_subprocess_events_reach_structured_handoffs_and_usage(self):
        fake = self.root / "fake.py"
        outputs = {s: result(s) for s in routing.STAGES}
        fake.write_text('import json,sys\nfrom pathlib import Path\n'
            'args=sys.argv[1:]; prompt=sys.stdin.read()\n'
            'schema=json.loads(Path(args[args.index("--output-schema")+1]).read_text())\n'
            'stage="analysis" if "requirements" in schema["properties"] else "implementation" if "changes" in schema["properties"] else "verification"\n'
            'print(json.dumps({"type":"thread.started","thread_id":"session-"+stage}))\n'
            'print(json.dumps({"type":"item.completed","item":{"type":"command_execution","command":"python -m unittest","exit_code":0,"aggregated_output":"OK"}}))\n'
            'outputs=' + repr(outputs) + '\n'
            'print(json.dumps({"type":"item.completed","item":{"type":"agent_message","text":json.dumps(outputs[stage])}}))\n'
            'print(json.dumps({"type":"turn.completed","usage":{"input_tokens":7,"output_tokens":3}}))\n', encoding="utf-8")
        self.task["codex"] = [sys.executable, str(fake)]
        state = tick(self.task, self.root / "state", self.now)
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["usage"]["total"]["tokens"], 30)
        self.assertTrue((self.root / "state/staged/runs/root/3/result.json").is_file())

    def test_crash_between_turn_completion_and_handoff_cannot_complete_task(self):
        with patch("reqflow_scheduler.dispatch.run", side_effect=self.fake()), patch(
                "reqflow_scheduler.stages.finish", side_effect=OSError("handoff interrupted")):
            with self.assertRaises(OSError): tick(self.task, self.root / "state", self.now)
        saved = read(self.root / "state/staged/state.json")
        self.assertEqual(saved["nodes"]["root"]["status"], "turn_completed")
        continue_task(self.task, self.root / "state")
        state = self.execute()
        self.assertEqual(state["status"], "completed")
        self.assertEqual([c[0] for c in self.calls], ["analysis", "implementation", "verification"])


class RoutingTests(unittest.TestCase):
    def test_configuration_and_reasoning_validation(self):
        routing.validate(ROUTES)
        for change in ({"provider": "direct-api"}, {"reasoning_effort": "invented"}, {"model": ""}):
            config = copy.deepcopy(ROUTES)
            config["analysis"].update(change)
            with self.assertRaises(ValueError): routing.validate(config)

    def test_real_profile_resolution_without_secrets_or_calls(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            (home / "config.toml").write_text('[model_providers.deepseek]\nenv_key="DEEPSEEK_TEST_KEY"\nwire_api="responses"\n')
            (home / "deepseek.config.toml").write_text('model="deepseek-flash"\nmodel_provider="deepseek"\nmodel_reasoning_effort="high"\n')
            task = {"model_routing": ROUTES}
            with patch.dict("os.environ", {"CODEX_HOME": directory, "DEEPSEEK_TEST_KEY": "private"}):
                route = routing.resolve(task, "implementation")
                self.assertEqual(route["model_provider"], "deepseek")
                self.assertNotIn("reasoning_effort", route)
                self.assertNotIn("private", json.dumps(route))
            with patch.dict("os.environ", {"CODEX_HOME": directory}):
                with self.assertRaisesRegex(ValueError, "credential"): routing.resolve(task, "implementation")

    def test_missing_model_and_unsupported_effort_do_not_fallback(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict("os.environ", {"CODEX_HOME": directory}):
            task = {"model_routing": ROUTES}
            with patch("reqflow_scheduler.quota_adapter.rpc", return_value={"data": []}):
                with self.assertRaisesRegex(ValueError, "unavailable"): routing.resolve(task, "analysis")
            with patch("reqflow_scheduler.quota_adapter.rpc", return_value={"data": [{"id": "gpt-6-astra", "supportedReasoningEfforts": [{"reasoningEffort": "low"}]}]}):
                with self.assertRaisesRegex(ValueError, "effort unavailable"): routing.resolve(task, "analysis")

    def test_catalog_uses_explicit_provider_even_if_global_default_differs(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict("os.environ", {"CODEX_HOME": directory}):
            (Path(directory) / "config.toml").write_text('model_provider="deepseek"\n')
            routes = copy.deepcopy(ROUTES)
            routes["analysis"]["model_provider"] = "openai"
            catalog = {"data": [{"id": "gpt-6-astra", "supportedReasoningEfforts": [{"reasoningEffort": "high"}]}]}
            with patch("reqflow_scheduler.quota_adapter.rpc", return_value=catalog) as rpc:
                routing.resolve({"model_routing": routes}, "analysis")
            self.assertIn('model_provider="openai"', rpc.call_args.args[3])


if __name__ == "__main__":
    unittest.main()

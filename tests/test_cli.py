from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import tempfile
import subprocess
import unittest
from unittest.mock import patch

from reqflow_scheduler.cli import main
from reqflow_scheduler.config import load_config
from reqflow_scheduler.scheduler import sleeping
from reqflow_scheduler.scheduler import fingerprint, load_task
from reqflow_scheduler.state_store import write
from reqflow_scheduler.view import render_tree


class CliTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.task = self.root / "task.json"
        self.task.write_text(json.dumps(dict(version=1, id="demo", workspace=str(self.root),
            tree=dict(id="root", prompt="Root", children=[dict(id="child", prompt="Child")]))))
        self.config = self.root / "policy.toml"
        self.config.write_text('[scheduler]\nschedule_policy = "continuous"\ninterval_seconds = 2\n')

    def invoke(self, *args):
        output = io.StringIO()
        with redirect_stdout(output), redirect_stderr(output):
            code = main(list(args))
        return code, output.getvalue()

    def test_config_defaults_and_file(self):
        code, output = self.invoke("config", "--config", str(self.config))
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output)["schedule_policy"], "continuous")
        self.assertEqual(load_config()["reset_lead_minutes"], 30)

    def test_list_merges_definitions_and_checkpoints_without_dispatch(self):
        folder = self.root / "state" / "demo"
        folder.mkdir(parents=True)
        (folder / "state.json").write_text(json.dumps(dict(status="running", current_node="child",
            nodes={"root": {"status": "completed"}, "child": {"status": "running"}})))
        orphan = folder.parent / "old-task"
        orphan.mkdir()
        (orphan / "state.json").write_text('{"status": "stopped"}')
        with patch("reqflow_scheduler.cli.tick") as tick, patch("reqflow_scheduler.quota_adapter.query") as quota:
            code, output = self.invoke("list", "--task-dir", str(self.root), "--state-dir", str(folder.parent))
        self.assertEqual(code, 0)
        self.assertIn("demo | running | 1/2 completed | current=child", output)
        self.assertIn(str(self.task.resolve()), output)
        self.assertIn("old-task | stopped", output)
        tick.assert_not_called()
        quota.assert_not_called()

    def test_list_empty_is_read_only(self):
        missing = self.root / "missing"
        code, output = self.invoke("list", "--task-dir", str(missing), "--state-dir", str(missing))
        self.assertEqual(code, 0)
        self.assertIn("No tasks found", output)
        self.assertFalse(missing.exists())

    def test_list_archives_completed_trees_and_all_preserves_history(self):
        state_dir = self.root / "state"
        state_path = state_dir / "demo/state.json"
        write(state_path, dict(status="completed", nodes={
            "root": {"status": "completed"}, "child": {"status": "completed"}}))
        write(state_dir / "orphan/state.json", dict(status="completed", nodes={}))
        for status in ("waiting", "running", "interrupted", "stopped", "blocked", "queued"):
            write(state_dir / status / "state.json", dict(status=status, nodes={}))
        log_path = state_dir / "demo/runs/root/1/events.jsonl"
        log_path.parent.mkdir(parents=True)
        log_path.write_text("saved execution log\n")
        original = {path: path.read_bytes() for path in self.root.rglob("*") if path.is_file()}
        with patch("reqflow_scheduler.cli.tick") as tick, patch("reqflow_scheduler.quota_adapter.query") as quota:
            code, output = self.invoke("list", "--task-dir", str(self.root), "--state-dir", str(state_dir))
            self.assertEqual(code, 0)
            self.assertNotIn("demo |", output)
            self.assertNotIn("orphan |", output)
            for status in ("waiting", "running", "interrupted", "stopped", "blocked", "queued"):
                self.assertIn(f"{status} | {status}", output)
            code, output = self.invoke("list", "--all", "--task-dir", str(self.root), "--state-dir", str(state_dir))
        self.assertEqual(code, 0)
        self.assertIn("demo | completed | 2/2 completed", output)
        self.assertIn("orphan | completed", output)
        self.assertEqual(original, {path: path.read_bytes() for path in self.root.rglob("*") if path.is_file()})
        tick.assert_not_called()
        quota.assert_not_called()
        code, output = self.invoke("view", "demo", "--once", "--task-dir", str(self.root), "--state-dir", str(state_dir))
        self.assertEqual(code, 0)
        self.assertIn("root ✓", output)
        self.assertIn("child ✓", output)

    def test_list_reactivates_completed_tree_with_appended_nodes(self):
        definition = json.loads(self.task.read_text())
        definition["allow_append"] = True
        self.task.write_text(json.dumps(definition))
        state_dir = self.root / "state"
        write(state_dir / "demo/state.json", dict(status="completed", nodes={
            "root": {"status": "completed"}, "child": {"status": "completed"}}))
        args = ("list", "--task-dir", str(self.root), "--state-dir", str(state_dir))
        code, output = self.invoke(*args)
        self.assertEqual(code, 0)
        self.assertIn("No tasks found", output)
        definition["tree"]["children"][0]["children"] = [dict(id="next", prompt="Next")]
        self.task.write_text(json.dumps(definition))
        code, output = self.invoke(*args)
        self.assertEqual(code, 0)
        self.assertIn("demo | queued | 2/3 completed", output)

    def test_list_queued_and_invalid_files(self):
        (self.root / "broken.json").write_text("{")
        (self.root / "task.example.json").write_text(self.task.read_text())
        code, output = self.invoke("list", "--task-dir", str(self.root), "--state-dir", str(self.root / "state"))
        self.assertEqual(code, 0)
        self.assertIn("demo | queued | 0/2 completed", output)
        self.assertIn("Skipping", output)
        self.assertEqual(output.count("demo |"), 1)

    def test_rm_deletes_definitions_checkpoints_and_logs(self):
        state_dir = self.root / "state"
        state_path = state_dir / "demo/state.json"
        state = dict(status="interrupted", nodes={"root": dict(status="completed")})
        write(state_path, state)
        log = state_path.parent / "events.jsonl"
        log.write_text("saved log\n")
        duplicate = self.root / "duplicate.json"
        duplicate.write_bytes(self.task.read_bytes())
        options = ("--task-dir", str(self.root), "--state-dir", str(state_dir))
        with patch("reqflow_scheduler.cli.tick") as tick, patch("reqflow_scheduler.quota_adapter.query") as quota:
            code, output = self.invoke("rm", "demo", *options)
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(output), dict(ids=["demo"], status="removed"))
            code, output = self.invoke("list", *options)
            self.assertEqual(code, 0)
            self.assertNotIn("demo |", output)
            code, output = self.invoke("list", "--all", *options)
            self.assertEqual(code, 0)
            self.assertNotIn("demo |", output)
        self.assertFalse(self.task.exists())
        self.assertFalse(duplicate.exists())
        self.assertFalse(state_path.parent.exists())
        self.assertTrue(self.root.is_dir())
        self.assertTrue(self.config.is_file())
        tick.assert_not_called()
        quota.assert_not_called()

    def test_rm_supports_definition_only_and_checkpoint_only_tasks(self):
        state_dir = self.root / "state"
        definition = json.loads(self.task.read_text())
        definition["id"] = "demo.json"
        write(self.root / "other-definition.json", definition)
        write(state_dir / "orphan/state.json", dict(status="interrupted", nodes={}))
        options = ("--task-dir", str(self.root), "--state-dir", str(state_dir))
        for identifier in ("demo", "orphan", "demo.json"):
            code, output = self.invoke("rm", identifier, *options)
            self.assertEqual(code, 0, output)
        self.assertFalse((state_dir / "demo/state.json").exists())
        code, output = self.invoke("list", *options)
        self.assertEqual(code, 0)
        self.assertIn("No tasks found", output)
        code, output = self.invoke("list", "--task-dir", str(self.root), "--state-dir", str(self.root / "other-state"))
        self.assertEqual(code, 0)
        self.assertIn("No tasks found", output)

    def test_rm_matches_globs_against_ids_instead_of_filenames(self):
        for index, (pattern, expected, regex) in enumerate((
                ("batch-*", ["batch-a1", "batch-b2"], False),
                ("batch-?1", ["batch-a1"], False),
                ("batch-[ab]?", ["batch-a1", "batch-b2"], False),
                (r"batch-(a1|b2)", ["batch-a1", "batch-b2"], True))):
            with self.subTest(pattern=pattern):
                task_dir = self.root / f"definitions-{index}"
                state_dir = self.root / f"state-{index}"
                files = {}
                for number, identifier in enumerate(("batch-a1", "batch-b2", "keep")):
                    definition = json.loads(self.task.read_text())
                    definition["id"] = identifier
                    files[identifier] = task_dir / f"unrelated-{number}.json"
                    write(files[identifier], definition)
                    write(state_dir / identifier / "state.json", dict(status="completed", nodes={}))
                code, output = self.invoke("rm", pattern, "--task-dir", str(task_dir), "--state-dir", str(state_dir),
                                           *(["--regex"] if regex else []))
                self.assertEqual(code, 0, output)
                self.assertEqual(json.loads(output)["ids"], expected)
                for identifier, path in files.items():
                    self.assertEqual(path.exists(), identifier not in expected)
                    self.assertEqual((state_dir / identifier).exists(), identifier not in expected)

    def test_rm_deletes_external_definition_and_legacy_marker_only_state(self):
        state_dir = self.root / "state"
        write(state_dir / "demo/state.json", dict(status="stopped", nodes={}))
        write(state_dir / "demo/submit.json", dict(task=str(self.task.resolve())))
        (state_dir / "legacy").mkdir()
        (state_dir / "legacy/removed").touch()
        code, output = self.invoke("rm", "*", "--task-dir", str(self.root / "empty"), "--state-dir", str(state_dir))
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output)["ids"], ["demo", "legacy"])
        self.assertFalse(self.task.exists())
        self.assertEqual(list(state_dir.iterdir()), [])

    def test_rm_skips_live_tasks_and_continues_deleting_other_matches(self):
        state_dir = self.root / "state"
        definition = json.loads(self.task.read_text())
        options = ("--task-dir", str(self.root), "--state-dir", str(state_dir))
        for source in ("watcher", "child", "unrecorded-child"):
            with self.subTest(source=source):
                write(self.task, definition)
                write(state_dir / "demo/state.json", dict(status="stopped", nodes={}))
                write(state_dir / "zz-free/state.json", dict(status="stopped", nodes={}))
                write(state_dir / "z-live/state.json", dict(status="running", nodes={
                    "root": dict(status="running", pid=123 if source == "child" else None)}
                    if source != "watcher" else {}))
                write(state_dir / "z-live/submit.json", dict(pid=123 if source == "watcher" else None))
                with patch("reqflow_scheduler.task_list.process_running", return_value=True):
                    code, output = self.invoke("rm", "*", *options)
                self.assertEqual(code, 3, output)
                result = json.loads(output)
                self.assertEqual(result["status"], "partial")
                self.assertEqual(result["ids"], ["demo", "zz-free"])
                self.assertEqual([item["id"] for item in result["skipped"]], ["z-live"])
                self.assertFalse(self.task.exists())
                self.assertFalse((state_dir / "demo").exists())
                self.assertFalse((state_dir / "zz-free").exists())
                self.assertTrue((state_dir / "z-live/state.json").exists())

    def test_rm_skips_locked_task_and_removes_later_match(self):
        from reqflow_scheduler.state_store import lock
        state_dir = self.root / "state"
        write(state_dir / "a-locked/state.json", dict(status="stopped", nodes={}))
        with lock("task:" + str(state_dir.resolve() / "a-locked/state.json")):
            code, output = self.invoke("rm", "*", "--task-dir", str(self.root), "--state-dir", str(state_dir))
        self.assertEqual(code, 3, output)
        result = json.loads(output)
        self.assertEqual(result["ids"], ["demo"])
        self.assertEqual(result["skipped"][0]["id"], "a-locked")
        self.assertFalse(self.task.exists())
        self.assertTrue((state_dir / "a-locked/state.json").exists())

    def test_rm_unknown_or_invalid_id_does_not_create_state(self):
        state_dir = self.root / "missing-state"
        for identifier in ("missing", "../demo", "..", str(self.task)):
            with self.subTest(identifier=identifier):
                code, output = self.invoke("rm", identifier, "--task-dir", str(self.root), "--state-dir", str(state_dir))
                self.assertEqual(code, 1)
                self.assertFalse(state_dir.exists())
        code, output = self.invoke("rm", "[", "--regex", "--task-dir", str(self.root), "--state-dir", str(state_dir))
        self.assertEqual(code, 1)
        self.assertIn("Invalid ID regex", output)
        self.assertFalse(state_dir.exists())

    def test_rm_rejects_state_directory_link_outside_selected_root(self):
        state_dir = self.root / "state"
        state_dir.mkdir()
        outside = self.root / "outside"
        write(outside / "state.json", dict(status="stopped", nodes={}))
        link = state_dir / "demo"
        try:
            link.symlink_to(outside, target_is_directory=True)
        except OSError:
            self.skipTest("Directory symlinks unavailable")
        code, output = self.invoke("rm", "demo", "--task-dir", str(self.root), "--state-dir", str(state_dir))
        self.assertEqual(code, 3, output)
        self.assertIn("escapes state root", output)
        self.assertTrue((outside / "state.json").is_file())
        self.assertTrue(self.task.is_file())

    def test_stop_by_id_works_without_definition_and_preserves_quota_wait(self):
        state_dir = self.root / "state"
        state_path = state_dir / "orphan/state.json"
        state = dict(status="waiting", reason="quota exhausted", next_check_at="2100-01-01T00:00:00+00:00", nodes={})
        write(state_path, state)
        with patch("reqflow_scheduler.cli.tick") as tick, patch("reqflow_scheduler.quota_adapter.query") as quota:
            code, output = self.invoke("stop", "orphan", "--state-dir", str(state_dir))
        self.assertEqual(code, 0, output)
        self.assertEqual(json.loads(output), dict(id="orphan", stop_requested=True))
        self.assertTrue((state_path.parent / "stop").is_file())
        self.assertEqual(json.loads(state_path.read_text()), state)
        tick.assert_not_called()
        quota.assert_not_called()

    def test_stop_accepts_definition_id_and_file_path(self):
        for reference in ("demo", str(self.task)):
            state_dir = self.root / ("id-state" if reference == "demo" else "file-state")
            code, output = self.invoke("stop", reference, "--task-dir", str(self.root), "--state-dir", str(state_dir))
            self.assertEqual(code, 0, output)
            self.assertTrue((state_dir / "demo/stop").is_file())

    def test_stop_missing_id_does_not_create_checkpoint(self):
        state_dir = self.root / "missing-state"
        code, output = self.invoke("stop", "missing", "--task-dir", str(self.root), "--state-dir", str(state_dir))
        self.assertEqual(code, 1, output)
        self.assertIn("Task ID not found", output)
        self.assertFalse(state_dir.exists())

    def test_invalid_configs(self):
        for text in ('[scheduler]\ninterval_seconds = 0', '[scheduler]\nnight_start_hour = true',
                     '[scheduler]\nunknown = 1', '[other]\nx = 1', '[scheduler]\nschedule_policy = []',
                     '[scheduler]\nnight_start_hour = 9\nnight_end_hour = 9', 'broken = ['):
            self.config.write_text(text)
            self.assertEqual(self.invoke("config", "--config", str(self.config))[0], 1)

    def test_view_reads_checkpoints_without_dispatch_or_writes(self):
        folder = self.root / "state" / "demo"
        folder.mkdir(parents=True)
        state = dict(status="waiting", current_node="child", nodes={"root": dict(status="completed", attempts=1)})
        (folder / "state.json").write_text(json.dumps(state))
        with patch("reqflow_scheduler.cli.tick") as tick, patch("reqflow_scheduler.quota_adapter.query") as quota:
            code, output = self.invoke("view", str(self.task), "--once", "--state-dir", str(folder.parent))
        self.assertEqual(code, 0)
        self.assertIn("root ✓", output)
        self.assertIn("`-- child -", output)
        tick.assert_not_called()
        quota.assert_not_called()
        self.assertEqual(json.loads((folder / "state.json").read_text()), state)

    def test_config_applied_to_tick(self):
        with patch("reqflow_scheduler.cli.tick", return_value={"status": "completed"}) as tick:
            code, _ = self.invoke("tick", str(self.task), "--config", str(self.config))
        self.assertEqual(code, 0)
        self.assertEqual(tick.call_args.args[0]["schedule_policy"], "continuous")

    def test_continue_returns_after_background_running_acknowledgement(self):
        with patch('reqflow_scheduler.cli.continue_task', return_value={'status': 'queued'}) as resume, patch(
            'reqflow_scheduler.cli.subprocess.Popen'
        ) as launch, patch('reqflow_scheduler.cli.read', return_value={'status': 'running', 'running_nodes': ['root']}):
            (self.root / 'state' / 'demo').mkdir(parents=True)
            launch.return_value.pid = 123
            code, output = self.invoke('continue', 'demo', '--task-dir', str(self.root),
                                  '--state-dir', str(self.root / 'state'), '--config', str(self.config),
                                  '--interval-seconds', '3')
        self.assertEqual(code, 0)
        resume.assert_called_once()
        self.assertEqual(json.loads(output)['running_nodes'], ['root'])
        command = launch.call_args.args[0]
        self.assertIn('watch', command)
        self.assertIn(str(self.config.resolve()), command)
        self.assertEqual(command[command.index('--interval-seconds') + 1], '3')
        if os.name == 'nt':
            flags = launch.call_args.kwargs['creationflags']
            self.assertTrue(flags & subprocess.CREATE_NO_WINDOW)
            self.assertFalse(flags & subprocess.DETACHED_PROCESS)

    def test_background_watch_acknowledges_running_before_tick_returns(self):
        ready = self.root / 'ready.json'

        def run(task, root, on_running):
            on_running(['root'])
            self.assertEqual(json.loads(ready.read_text())['running_nodes'], ['root'])
            return {'status': 'completed'}

        with patch('reqflow_scheduler.cli.tick', side_effect=run):
            code, _ = self.invoke('watch', str(self.task), '--background-ready', str(ready))
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(ready.read_text())['status'], 'running')

    def test_background_watch_reports_waiting_without_claiming_running(self):
        ready = self.root / 'ready.json'
        with patch('reqflow_scheduler.cli.tick', return_value={'status': 'waiting', 'reason': 'quota exhausted'}), patch(
            'reqflow_scheduler.cli.time.sleep', side_effect=KeyboardInterrupt
        ):
            self.invoke('watch', str(self.task), '--background-ready', str(ready))
        self.assertEqual(json.loads(ready.read_text())['status'], 'waiting')

    def test_continue_missing_checkpoint_does_not_dispatch(self):
        with patch('reqflow_scheduler.cli.tick') as tick:
            code, output = self.invoke('continue', str(self.task), '--state-dir', str(self.root / 'state'))
        self.assertEqual(code, 1)
        self.assertIn('checkpoint', output)
        tick.assert_not_called()

    def test_continue_reuses_live_watcher_without_recovering_a_running_child(self):
        folder = self.root / 'state/demo'
        write(folder / 'submit.json', {'pid': 123})
        write(folder / 'state.json', {'status': 'running', 'nodes': {}, 'fingerprint': fingerprint(load_task(self.task))})
        with patch('reqflow_scheduler.cli.process_running', return_value=True), \
                patch('reqflow_scheduler.cli.continue_task') as resume, \
                patch('reqflow_scheduler.cli.subprocess.Popen') as spawn:
            code, output = self.invoke('continue', str(self.task), '--state-dir', str(folder.parent))
        self.assertEqual(code, 0)
        self.assertTrue(json.loads(output)['already_running'])
        resume.assert_not_called()
        spawn.assert_not_called()

    def test_continue_completed_task_does_not_restart(self):
        write(self.root / 'state/demo/state.json', {'status': 'completed', 'nodes': {}})
        with patch('reqflow_scheduler.cli.continue_task') as resume, patch('reqflow_scheduler.cli.subprocess.Popen') as spawn:
            code, output = self.invoke('continue', str(self.task), '--state-dir', str(self.root / 'state'))
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output)['status'], 'completed')
        resume.assert_not_called()
        spawn.assert_not_called()

    def test_submit_new_external_task_acknowledges_waiting_and_preserves_policy(self):
        def launch(command, **options):
            ready = Path(command[command.index('--background-ready') + 1])
            write(ready, {'id': 'demo', 'status': 'waiting', 'reason': 'quota exhausted', 'running_nodes': []})
            return type('Process', (), {'pid': 123})()

        with patch('reqflow_scheduler.cli.subprocess.Popen', side_effect=launch) as spawn, \
                patch('reqflow_scheduler.cli.continue_task') as resume, \
                patch('reqflow_scheduler.quota_adapter.query') as quota:
            code, output = self.invoke('submit', str(self.task), '--state-dir', str(self.root / 'state'),
                '--config', str(self.config), '--interval-seconds', '3')
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output)['status'], 'waiting')
        self.assertEqual(json.loads(output)['scheduler_pid'], 123)
        self.assertIn(str(self.config.resolve()), spawn.call_args.args[0])
        self.assertIn('watch', spawn.call_args.args[0])
        self.assertEqual(spawn.call_args.args[0][spawn.call_args.args[0].index('--interval-seconds') + 1], '3')
        self.assertEqual(json.loads((self.root / 'state/demo/submit.json').read_text())['task'], str(self.task.resolve()))
        resume.assert_not_called()
        quota.assert_not_called()

    def test_submit_reuses_live_background_and_rejects_changed_dispatched_content(self):
        folder = self.root / 'state/demo'
        write(folder / 'submit.json', {'pid': 123})
        write(folder / 'state.json', {'status': 'waiting', 'nodes': {}, 'fingerprint': fingerprint(load_task(self.task))})
        with patch('reqflow_scheduler.cli.process_running', return_value=True), \
                patch('reqflow_scheduler.cli.subprocess.Popen') as spawn:
            code, output = self.invoke('submit', str(self.task), '--state-dir', str(folder.parent))
            self.assertEqual(code, 0)
            self.assertTrue(json.loads(output)['already_running'])
            changed = json.loads(self.task.read_text())
            changed['tree']['prompt'] = 'changed after dispatch'
            self.task.write_text(json.dumps(changed))
            code, output = self.invoke('submit', str(self.task), '--state-dir', str(folder.parent))
        self.assertEqual(code, 1)
        self.assertIn('task tree changed', output)
        spawn.assert_not_called()

    def test_submit_does_not_implicitly_recover_or_ignore_stop(self):
        for status in ('interrupted', 'stopped', 'running'):
            with self.subTest(status=status):
                write(self.root / 'state/demo/state.json', {'status': status, 'nodes': {}})
                with patch('reqflow_scheduler.cli.subprocess.Popen') as spawn, \
                        patch('reqflow_scheduler.cli.continue_task') as resume:
                    code, output = self.invoke('submit', str(self.task), '--state-dir', str(self.root / 'state'))
                self.assertEqual(code, 1)
                self.assertIn('continue', output)
                spawn.assert_not_called()
                resume.assert_not_called()

    def test_submit_completed_task_does_not_restart_or_query_quota(self):
        write(self.root / 'state/demo/state.json', {'status': 'completed', 'nodes': {}})
        with patch('reqflow_scheduler.cli.subprocess.Popen') as spawn, \
                patch('reqflow_scheduler.quota_adapter.query') as quota:
            code, output = self.invoke('submit', str(self.task), '--state-dir', str(self.root / 'state'))
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output)['status'], 'completed')
        spawn.assert_not_called()
        quota.assert_not_called()

    def test_submit_background_startup_failure_is_reported(self):
        with patch('reqflow_scheduler.cli.subprocess.Popen') as spawn:
            spawn.return_value.pid = 123
            spawn.return_value.poll.return_value = 1
            spawn.return_value.returncode = 1
            code, output = self.invoke('submit', str(self.task), '--state-dir', str(self.root / 'state'))
        self.assertEqual(code, 1)
        self.assertIn('background scheduler exited (1)', output)
        self.assertIn('submit.log', output)

    def test_submit_real_background_survives_return_and_repeated_submission(self):
        definition = json.loads(self.task.read_text())
        definition['not_before'] = '2100-01-01T00:00:00+00:00'
        self.task.write_text(json.dumps(definition))
        workers = []
        real_popen = subprocess.Popen

        def launch(*args, **kwargs):
            worker = real_popen(*args, **kwargs)
            workers.append(worker)
            return worker

        try:
            with patch('reqflow_scheduler.cli.subprocess.Popen', side_effect=launch):
                code, output = self.invoke('submit', str(self.task), '--state-dir', str(self.root / 'state'), '--interval-seconds', '1')
                self.assertEqual(code, 0, output)
                self.assertEqual(json.loads(output)['status'], 'waiting')
                self.assertIsNone(workers[0].poll())
                code, output = self.invoke('submit', str(self.task), '--state-dir', str(self.root / 'state'))
                self.assertEqual(code, 0, output)
                self.assertTrue(json.loads(output)['already_running'])
                self.assertEqual(len(workers), 1)
            state = json.loads((self.root / 'state/demo/state.json').read_text())
            self.assertEqual(state['reason'], 'not due')
            self.assertNotIn('quota', state)
        finally:
            (self.root / 'state/demo/stop').touch()
            for worker in workers:
                worker.wait(timeout=10)

    def test_view_refresh_and_interval_override(self):
        with patch("reqflow_scheduler.cli.time.sleep", side_effect=KeyboardInterrupt) as sleep:
            code, _ = self.invoke("view", str(self.task), "--watch", "--config", str(self.config),
                                  "--interval-seconds", "3", "--state-dir", str(self.root / "state"))
        self.assertEqual(code, 130)
        sleep.assert_called_once_with(3)
        self.assertFalse((self.root / "state").exists())

    def test_view_refreshes_checkpoint_by_default(self):
        with patch("reqflow_scheduler.cli.read", side_effect=[
            {"status": "running", "nodes": {"root": {"status": "running", "pid": 123}}},
            {"status": "completed", "nodes": {"root": {"status": "completed"}}},
        ]), patch("reqflow_scheduler.view.process_running", return_value=True), patch("reqflow_scheduler.cli.time.sleep", side_effect=[None, KeyboardInterrupt]) as sleep:
            code, output = self.invoke("view", str(self.task))
        self.assertEqual(code, 130)
        self.assertRegex(output, "root [⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏]")
        self.assertIn("root ✓", output)
        self.assertEqual(sleep.call_args_list[0].args, (2,))

    def test_view_resolves_task_id_from_definition_not_filename(self):
        with patch("reqflow_scheduler.cli.DEFAULT_TASK_DIR", self.root):
            code, output = self.invoke("view", "demo", "--once", "--state-dir", str(self.root / "state"))
        self.assertEqual(code, 0)
        self.assertIn("Task demo", output)
        self.assertIn("child -", output)

    def test_view_id_lookup_errors_and_custom_directory(self):
        code, output = self.invoke("view", "demo", "--task-dir", str(self.root), "--once")
        self.assertEqual(code, 0)
        self.assertIn("Task demo", output)
        code, output = self.invoke("view", "missing", "--task-dir", str(self.root), "--once")
        self.assertEqual(code, 1)
        self.assertIn("Task ID not found", output)
        code, output = self.invoke("view", str(self.root / "missing.json"), "--once")
        self.assertEqual(code, 1)
        self.assertIn("Task file not found", output)
        (self.root / "duplicate.json").write_text(self.task.read_text())
        code, output = self.invoke("view", "demo", "--task-dir", str(self.root), "--once")
        self.assertEqual(code, 1)
        self.assertIn("Multiple definitions", output)

    def test_view_and_list_find_external_definition_from_submission_record(self):
        state_dir = self.root / "state"
        folder = state_dir / "demo"
        write(folder / "submit.json", dict(task=str(self.task.resolve()), pid=123))
        write(folder / "state.json", dict(status="waiting", nodes={"root": dict(status="completed")}))
        original = (folder / "state.json").read_bytes()
        task_dir = str(self.root / "empty-config")
        with patch("reqflow_scheduler.cli.tick") as tick, patch("reqflow_scheduler.quota_adapter.query") as quota:
            code, output = self.invoke("view", "demo", "--once", "--task-dir", task_dir, "--state-dir", str(state_dir))
            self.assertEqual(code, 0)
            self.assertIn("Task demo | waiting", output)
            self.assertIn("root ✓", output)
            self.assertIn("child -", output)
            code, output = self.invoke("list", "--task-dir", task_dir, "--state-dir", str(state_dir))
        self.assertEqual(code, 0)
        self.assertIn("demo | waiting | 1/2 completed", output)
        self.assertIn(str(self.task.resolve()), output)
        self.assertEqual((folder / "state.json").read_bytes(), original)
        tick.assert_not_called()
        quota.assert_not_called()

    def test_id_lookup_ignores_missing_mismatched_and_invalid_submission_records(self):
        state_dir = self.root / "state"
        record = state_dir / "demo/submit.json"
        mismatch = self.root / "other.json"
        write(mismatch, dict(version=1, id="other", workspace=str(self.root), prompt="Other"))
        for value in (dict(task=str(self.root / "deleted.json")), dict(task=str(mismatch)), {}, []):
            with self.subTest(record=value):
                write(record, value)
                code, output = self.invoke("view", "demo", "--once", "--task-dir", str(self.root / "empty-config"),
                                           "--state-dir", str(state_dir))
                self.assertEqual(code, 1)
                self.assertIn("Task ID not found", output)

    def test_cross_midnight(self):
        policy = dict(night_start_hour=23, night_end_hour=6)
        for hour, expected in ((22, False), (23, True), (0, True), (5, True), (6, False)):
            now = datetime(2026, 10, 31, hour, tzinfo=timezone(timedelta(hours=8)))
            self.assertEqual(sleeping(now, policy), expected)

    def test_view_only_spins_for_a_verified_live_process(self):
        task = dict(id='demo', workspace=str(self.root), tree=dict(id='root'))
        state = dict(status='running', nodes={'root': dict(status='running', pid=123)})
        with patch('reqflow_scheduler.view.process_running', return_value=False):
            output = render_tree(task, state)
        self.assertIn('Task demo | interrupted', output)
        self.assertIn('root -', output)
        self.assertEqual(state['nodes']['root']['status'], 'running')
        with patch('reqflow_scheduler.view.process_running', return_value=True):
            self.assertRegex(render_tree(task, state), 'root [⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏]')
        with patch('reqflow_scheduler.view.process_running', side_effect=OSError):
            self.assertIn('Task demo | unknown', render_tree(task, state))
        state['nodes']['root'].pop('pid')
        self.assertIn('root -', render_tree(task, state))
        state['nodes']['root']['status'] = 'interrupted'
        self.assertIn('Task demo | interrupted', render_tree(task, state))

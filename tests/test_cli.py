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

    def test_list_queued_and_invalid_files(self):
        (self.root / "broken.json").write_text("{")
        (self.root / "task.example.json").write_text(self.task.read_text())
        code, output = self.invoke("list", "--task-dir", str(self.root), "--state-dir", str(self.root / "state"))
        self.assertEqual(code, 0)
        self.assertIn("demo | queued | 0/2 completed", output)
        self.assertIn("Skipping", output)
        self.assertEqual(output.count("demo |"), 1)

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

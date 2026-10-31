from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from reqflow_scheduler.cli import main
from reqflow_scheduler.config import load_config
from reqflow_scheduler.scheduler import sleeping


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
            code, output = self.invoke("view", str(self.task), "--state-dir", str(folder.parent))
        self.assertEqual(code, 0)
        self.assertIn("[completed] root", output)
        self.assertIn("`-- > [queued] child", output)
        tick.assert_not_called()
        quota.assert_not_called()
        self.assertEqual(json.loads((folder / "state.json").read_text()), state)

    def test_config_applied_to_tick(self):
        with patch("reqflow_scheduler.cli.tick", return_value={"status": "completed"}) as tick:
            code, _ = self.invoke("tick", str(self.task), "--config", str(self.config))
        self.assertEqual(code, 0)
        self.assertEqual(tick.call_args.args[0]["schedule_policy"], "continuous")

    def test_view_refresh_and_interval_override(self):
        with patch("reqflow_scheduler.cli.time.sleep", side_effect=KeyboardInterrupt) as sleep:
            code, _ = self.invoke("view", str(self.task), "--watch", "--config", str(self.config),
                                  "--interval-seconds", "3", "--state-dir", str(self.root / "state"))
        self.assertEqual(code, 130)
        sleep.assert_called_once_with(3)
        self.assertFalse((self.root / "state").exists())

    def test_cross_midnight(self):
        policy = dict(night_start_hour=23, night_end_hour=6)
        for hour, expected in ((22, False), (23, True), (0, True), (5, True), (6, False)):
            now = datetime(2026, 10, 31, hour, tzinfo=timezone(timedelta(hours=8)))
            self.assertEqual(sleeping(now, policy), expected)

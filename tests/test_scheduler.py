from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from reqflow_scheduler import quota_adapter
from reqflow_scheduler.scheduler import load_task, paths, recover, sleeping, tick
from reqflow_scheduler.state_store import Busy, lock, read, write


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.workspace = self.root / 'project'
        self.workspace.mkdir()
        self.state_root = self.root / 'state'
        self.now = datetime(2026, 10, 27, 18, tzinfo=timezone.utc)  # 02:00 Beijing
        self.fake = self.root / 'fake.py'
        self.fake.write_text('''import json, sys, time
from pathlib import Path
prompt = sys.stdin.buffer.read().decode("utf-8")
with Path("calls.jsonl").open("a", encoding="utf8") as f:
    f.write(json.dumps(dict(args=sys.argv[1:], prompt=prompt)) + "\\n")
print(json.dumps({"type":"thread.started", "thread_id":"session-123"}), flush=True)
if prompt == "slow": time.sleep(5)
if prompt == "fail": sys.exit(7)
print(json.dumps({"type":"turn.completed"}), flush=True)
''', encoding='utf8')
        self.raw = dict(version=1, id='test', workspace=str(self.workspace),
                        tree=dict(id='root', prompt='root', children=[dict(id='a', prompt='a', children=[dict(id='b', prompt='b')]), dict(id='c', prompt='c')]),
                        codex=[sys.executable, str(self.fake)])
        self.task_file = self.root / 'task.json'
        self.task = self.load()
        self.quota = dict(source='codex-app-server', remaining_percent=79,
                          observed_at=self.now.isoformat(), resets_at=(self.now + timedelta(hours=2)).isoformat(), blocked_until=None)
        self.query = patch('reqflow_scheduler.quota_adapter.query', return_value=self.quota).start()
        self.addCleanup(patch.stopall)

    def load(self):
        write(self.task_file, self.raw)
        return load_task(self.task_file)

    def calls(self):
        p = self.workspace / 'calls.jsonl'
        return [json.loads(s) for s in p.read_text(encoding='utf8').splitlines()] if p.exists() else []

    def test_sleep_depth_first_and_completed_not_redispatched(self):
        result = tick(self.task, self.state_root, self.now)
        self.assertEqual(result['status'], 'completed')
        self.assertEqual([c['prompt'] for c in self.calls()], ['root', 'a', 'b', 'c'])
        tick(self.task, self.state_root, self.now)
        self.assertEqual(len(self.calls()), 4)

    def test_no_requirement_or_git_checks_even_legacy_fields(self):
        self.raw.update(requirements='missing', readiness='missing', require_clean=True, quota_file='missing')
        self.assertEqual(tick(self.load(), self.state_root, self.now)['status'], 'completed')

    def test_daytime_window_one_node_and_no_repeat(self):
        day = self.now - timedelta(hours=10)  # 16:00 Beijing
        self.quota['resets_at'] = (day + timedelta(minutes=30)).isoformat()
        result = tick(self.task, self.state_root, day)
        self.assertEqual(result['status'], 'waiting')
        self.assertEqual([c['prompt'] for c in self.calls()], ['root'])
        self.assertEqual(result['current_node'], 'a')
        tick(self.task, self.state_root, day + timedelta(minutes=1))
        self.assertEqual(len(self.calls()), 1)
        self.quota['resets_at'] = (day + timedelta(hours=5, minutes=30)).isoformat()
        tick(self.task, self.state_root, day + timedelta(hours=5))
        self.assertEqual([c['prompt'] for c in self.calls()], ['root', 'a'])

    def test_daytime_outside_window_no_dispatch(self):
        day = self.now - timedelta(hours=10)
        self.quota['resets_at'] = (day + timedelta(minutes=31)).isoformat()
        result = tick(self.task, self.state_root, day)
        self.assertEqual(result['reason'], 'outside daytime reset window')
        self.assertEqual(self.calls(), [])
        before = self.query.call_count
        tick(self.task, self.state_root, day + timedelta(seconds=30))
        self.assertEqual(self.query.call_count, before)

    def test_sleep_overrides_daytime_delay(self):
        day = self.now - timedelta(hours=2)  # midnight
        self.quota['resets_at'] = (day + timedelta(hours=5)).isoformat()
        tick(self.task, self.state_root, day)
        result = tick(self.task, self.state_root, self.now)
        self.assertEqual(result['status'], 'completed')

    def test_sleep_boundaries(self):
        for hour, minute, expected in [(0,59,False), (1,0,True), (8,59,True), (9,0,False), (23,0,False)]:
            local = datetime(2026,10,28,hour,minute,tzinfo=timezone(timedelta(hours=8)))
            self.assertEqual(sleeping(local), expected)

    def test_exhausted_waits_and_requeries_after_reset(self):
        reset = self.now + timedelta(hours=1)
        self.quota['blocked_until'] = reset.isoformat()
        self.assertEqual(tick(self.task, self.state_root, self.now)['reason'], 'quota exhausted')
        self.assertEqual(self.calls(), [])
        before = self.query.call_count
        tick(self.task, self.state_root, self.now + timedelta(minutes=5))
        self.assertEqual(self.query.call_count, before)
        self.quota.update(blocked_until=None, resets_at=(reset + timedelta(hours=5)).isoformat())
        self.assertEqual(tick(self.task, self.state_root, reset)['status'], 'completed')

    def test_limit_mid_node_resumes_exact_session_with_continue(self):
        self.raw['tree']['prompt'] = 'fail'
        task = self.load()
        exhausted = {**self.quota, 'blocked_until': (self.now + timedelta(hours=1)).isoformat()}
        self.query.side_effect = [self.quota, exhausted]
        result = tick(task, self.state_root, self.now)
        self.assertEqual(result['reason'], 'quota exhausted')
        self.assertEqual(result['current_node'], 'root')
        self.assertEqual(result['nodes']['root']['session_id'], 'session-123')
        self.query.side_effect = None
        self.query.return_value = self.quota
        result = tick(task, self.state_root, self.now + timedelta(hours=1))
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(self.calls()[1]['prompt'], '\u7ee7\u7eed')
        self.assertIn('resume', self.calls()[1]['args'])
        self.assertIn('session-123', self.calls()[1]['args'])

    def test_quota_checked_before_child(self):
        exhausted = {**self.quota, 'blocked_until': self.quota['resets_at']}
        self.query.side_effect = [self.quota, exhausted]
        result = tick(self.task, self.state_root, self.now)
        self.assertEqual(result['current_node'], 'a')
        self.assertEqual([c['prompt'] for c in self.calls()], ['root'])

    def test_unknown_quota_no_dispatch(self):
        self.query.side_effect = ValueError('quota unavailable')
        self.assertEqual(tick(self.task, self.state_root, self.now)['status'], 'blocked')
        self.assertEqual(self.calls(), [])

    def test_stop_before_first_dispatch_can_recover(self):
        folder, state_path, stop_path = paths(self.task, self.state_root)
        folder.mkdir(parents=True)
        stop_path.touch()
        self.assertEqual(tick(self.task, self.state_root, self.now)['status'], 'stopped')
        recover(self.task, self.state_root, True)
        self.assertEqual(tick(self.task, self.state_root, self.now)['status'], 'completed')

    def test_stop_running_child_preserves_session(self):
        self.raw['tree']['prompt'] = 'slow'
        task = self.load()
        folder, state_path, stop_path = paths(task, self.state_root)
        worker = threading.Thread(target=tick, args=(task, self.state_root, self.now))
        worker.start()
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if read(state_path, {}).get('nodes', {}).get('root', {}).get('session_id'):
                    break
                time.sleep(0.03)
            stop_path.touch()
            worker.join(timeout=10)
            self.assertFalse(worker.is_alive())
            result = read(state_path)
            self.assertEqual(result['reason'], 'stopped')
            self.assertEqual(result['nodes']['root']['session_id'], 'session-123')
        finally:
            stop_path.touch()
            worker.join(timeout=10)

    def test_failure_requires_recovery_and_keeps_session(self):
        self.raw['tree']['prompt'] = 'fail'
        task = self.load()
        self.assertEqual(tick(task,self.state_root,self.now)['status'], 'interrupted')
        with self.assertRaises(ValueError):
            recover(task,self.state_root)
        recover(task,self.state_root,True)
        self.assertEqual(tick(task,self.state_root,self.now)['status'], 'completed')
        self.assertEqual(self.calls()[1]['prompt'], '\u7ee7\u7eed')

    def test_timeout_and_stop(self):
        self.raw['tree']['prompt'] = 'slow'
        self.raw['timeout_seconds'] = 1
        self.task = self.load()
        result = tick(self.task, self.state_root, self.now)
        self.assertEqual(result['reason'], 'timeout')
        self.assertEqual(result['nodes']['root']['session_id'], 'session-123')
        recover(self.task,self.state_root,True)
        folder, state_path, stop_path = paths(self.task,self.state_root)
        stop_path.touch()
        self.assertEqual(tick(self.task,self.state_root,self.now)['status'], 'stopped')

    def test_workspace_lock_and_changed_tree(self):
        with lock('workspace:' + __import__('os').path.normcase(self.task['workspace'])):
            with self.assertRaises(Busy):
                tick(self.task,self.state_root,self.now)
        tick(self.task,self.state_root,self.now)
        self.raw['tree']['prompt'] = 'changed'
        with self.assertRaises(ValueError):
            tick(self.load(),self.state_root,self.now)

    def test_invalid_nodes(self):
        for child in [dict(id='root',prompt='duplicate'), dict(id='../escape',prompt='x'), dict(id='empty',prompt='')]:
            self.raw['tree']['children'] = [child]
            with self.assertRaises(ValueError):
                self.load()


class QuotaTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026,10,27,tzinfo=timezone.utc)
        self.reset = int((self.now+timedelta(hours=2)).timestamp())
        self.bucket = dict(limitId='codex',primary=dict(usedPercent=50,windowDurationMins=300,resetsAt=self.reset),secondary=None)

    def normalize(self):
        return quota_adapter.normalize(dict(rateLimits=self.bucket),self.now)

    def test_five_hour_and_weekly_limit(self):
        self.assertEqual(self.normalize()['remaining_percent'],50)
        self.bucket['secondary'] = dict(usedPercent=100,windowDurationMins=10080,resetsAt=self.reset+10000)
        self.assertEqual(quota_adapter.timestamp(self.normalize()['blocked_until']).timestamp(),self.reset+10000)

    def test_missing_stale_and_wrong_bucket(self):
        for mutation in [dict(limitId='other'),dict(primary=None),dict(primary=dict(usedPercent=50,windowDurationMins=300,resetsAt=0))]:
            original=self.bucket.copy()
            self.bucket.update(mutation)
            with self.assertRaises(ValueError): self.normalize()
            self.bucket=original

    def test_rpc_handshake_no_model_turn(self):
        with tempfile.TemporaryDirectory() as folder:
            fake=Path(folder)/'rpc.py'
            fake.write_text('''import sys,json
first=json.loads(sys.stdin.readline())
assert first['method']=='initialize'
print(json.dumps(dict(id=first['id'],result={})),flush=True)
assert json.loads(sys.stdin.readline())['method']=='initialized'
request=json.loads(sys.stdin.readline())
assert request['method']=='account/rateLimits/read'
print(json.dumps(dict(id=request['id'],result=dict(rateLimits=dict(limitId='codex')))),flush=True)
sys.stdin.read()
''')
            result=quota_adapter.fetch(dict(codex=[sys.executable,str(fake)]))
            self.assertEqual(result['rateLimits']['limitId'],'codex')


if __name__ == '__main__':
    unittest.main()


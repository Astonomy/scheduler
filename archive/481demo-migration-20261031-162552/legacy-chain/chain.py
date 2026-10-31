"""Run project-authored successor tasks serially through the quota-aware scheduler."""

import json
from pathlib import Path
import subprocess
import sys


ROOT = Path('D:/code/scheduler')
FOLDER = Path(__file__).resolve().parent
POINTER = FOLDER / 'current.json'
HANDOFF = FOLDER / 'next.json'
WORKSPACE = Path('D:/code/481/requirement-compilation-structure-main').resolve()


def run(initial):
    current = Path(initial).resolve()
    seen = set()
    while True:
        task = json.loads(current.read_text(encoding='utf-8-sig'))
        if Path(task['workspace']).resolve() != WORKSPACE:
            raise ValueError('Successor workspace does not match this project')
        if task['id'] in seen:
            raise ValueError('Repeated successor task; refusing to rerun')
        seen.add(task['id'])
        POINTER.write_text(json.dumps({'task': str(current), 'id': task['id']}), encoding='utf-8')
        print('Watching', task['id'], flush=True)
        code = subprocess.call([sys.executable, '-m', 'reqflow_scheduler', 'watch', str(current),
            '--config', str(ROOT / 'config/reqcomp-continuous.toml')], cwd=ROOT)
        if code:
            raise RuntimeError(f'Scheduler exited {code}; inspect and recover the original node')
        state_path = Path.home() / '.reqflow-scheduler' / task['id'] / 'state.json'
        state = json.loads(state_path.read_text(encoding='utf-8'))
        if state.get('status') != 'completed':
            print('Stopped without completion:', state.get('status'), flush=True)
            return
        if not HANDOFF.exists():
            print('No successor submitted; chain finished.', flush=True)
            return
        handoff = json.loads(HANDOFF.read_text(encoding='utf-8'))
        if handoff.get('previous_id') != task['id']:
            raise ValueError('Successor handoff does not belong to the completed task')
        candidate = Path(handoff['task']).resolve()
        if candidate.parent != (ROOT / 'config').resolve():
            raise ValueError('Successor task must be stored in scheduler/config')
        # Validate task format before consuming the handoff. Scheduler owns checkpoints.
        from reqflow_scheduler.scheduler import load_task
        successor = load_task(candidate)
        if successor['id'] in seen or successor['id'] == task['id']:
            raise ValueError('Successor needs a unique task id')
        HANDOFF.replace(FOLDER / f'handoff-{task["id"]}.json')
        current = candidate


if __name__ == '__main__':
    try:
        run(sys.argv[1])
    except Exception as error:
        print(f'Chain stopped: {error}', file=sys.stderr, flush=True)
        sys.exit(1)

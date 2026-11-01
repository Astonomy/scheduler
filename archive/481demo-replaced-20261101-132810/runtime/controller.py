"""Watch one quota-aware task tree and adopt validated round extensions."""

import json
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone

from reqflow_scheduler.scheduler import load_task, paths, validate_extension
from reqflow_scheduler.state_store import lock, read, write


ROOT = Path('D:/code/scheduler')
RUNTIME = ROOT / 'runtime/481demo'
TASK = ROOT / 'config/481demo.json'
PROPOSAL = RUNTIME / 'extension.json'
POLICY = ROOT / 'config/reqcomp-continuous.toml'
STATES = Path.home() / '.reqflow-scheduler'


def adopt_extension():
    """Consume exactly one proposal after a completed tree, without touching old nodes."""
    proposal = read(PROPOSAL)
    if not proposal:
        return False
    candidate_path = Path(proposal['candidate_file']).resolve()
    if candidate_path.parent != RUNTIME.resolve() or candidate_path == PROPOSAL.resolve():
        raise ValueError('Candidate must be a separate file in this runtime directory')
    candidate = load_task(candidate_path)
    current = load_task(TASK)
    if candidate['id'] != '481demo' or candidate['workspace'] != current['workspace']:
        raise ValueError('Extensions must stay in the same project and 481demo tree')
    _, checkpoint, _ = paths(current, STATES)
    with lock('task:' + str(checkpoint)):
        state = read(checkpoint)
        if proposal.get('base_fingerprint') != state.get('fingerprint'):
            raise ValueError('Proposal is stale; base fingerprint differs')
        validate_extension(candidate, state)
        write(TASK, candidate)
        history = RUNTIME / 'history'
        history.mkdir(exist_ok=True)
        destination = history / ('extension-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '.json')
        PROPOSAL.replace(destination)
    print('Adopted deeper levels in 481demo', flush=True)
    return True


def main():
    while True:
        print('Watching 481demo', flush=True)
        code = subprocess.call([sys.executable, '-m', 'reqflow_scheduler', 'watch', str(TASK),
            '--config', str(POLICY)], cwd=ROOT)
        if code:
            raise RuntimeError(f'Scheduler exited {code}; inspect and recover existing node sessions')
        current = load_task(TASK)
        _, checkpoint, _ = paths(current, STATES)
        state = read(checkpoint)
        if state.get('status') != 'completed':
            print('Scheduler stopped:', state.get('status'), flush=True)
            return
        if not adopt_extension():
            print('No next-round proposal; 481demo finished.', flush=True)
            return


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('481demo controller stopped:', error, file=sys.stderr, flush=True)
        sys.exit(1)

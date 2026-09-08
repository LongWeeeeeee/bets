#!/usr/bin/env python3
"""Non-LLM event monitor for offline laning training; one terminal notification."""
import argparse
import json
from pathlib import Path
import subprocess
import time

from watch_draft_phase_training import atomic_json, process_activity


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', type=Path, required=True)
    parser.add_argument('--thread', required=True)
    args = parser.parse_args()
    last, previous, sent = time.time(), None, set()
    receipt = args.run_dir / 'monitor.json'
    if receipt.exists():
        sent = set(json.loads(receipt.read_text()).get('sent', []))
    while True:
        event = None
        try:
            state = json.loads((args.run_dir / 'status.json').read_text())
            if state['state'] in ('DONE', 'FAIL'):
                event = state['state']
            else:
                alive, cpu = process_activity(int(state['pid']))
                current = (cpu, (args.run_dir / 'run.log').stat().st_mtime, state['stage'])
                if current != previous:
                    last, previous = time.time(), current
                if not alive:
                    event = 'FAIL'
                elif time.time() - last > 1800:
                    event = 'STALL'
            detail = state['stage']
        except Exception as exc:
            event, detail = 'UNREACHABLE', str(exc)
        print(f'{time.time():.0f} state={event or "RUNNING"} stage={detail}', flush=True)
        if event and event not in sent:
            # Persist uncertain delivery before enqueue: never retry an ambiguous send.
            sent.add(event)
            atomic_json(receipt, {'sent': sorted(sent), 'delivery': 'pending_or_uncertain'})
            message = (f'LANING_TRAINING {event}: {args.run_dir}; {detail}. '
                       'Проверь status.json/run.log и модели, заверши исходную задачу ingame-14y8. Прод не переключать.')
            try:
                result = subprocess.run(['/Applications/ChatGPT.app/Contents/Resources/codex', 'queue',
                    '--thread', args.thread, '--message', message], capture_output=True, text=True, timeout=30)
                atomic_json(receipt, {'sent': sorted(sent), 'queue_exit': result.returncode,
                                      'output': result.stdout, 'error': result.stderr})
            except subprocess.TimeoutExpired:
                pass
        if event in ('DONE', 'FAIL'):
            return
        time.sleep(60)


if __name__ == '__main__':
    main()

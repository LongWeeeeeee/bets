#!/usr/bin/env python3
"""Non-LLM completion monitor; queues only terminal or actionable events."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time


def atomic_json(path, value):
    tmp = path.with_name(path.name+'.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False))
    os.replace(tmp, path)


def cpu_seconds(value):
    days = 0
    if '-' in value:
        d, value = value.split('-', 1)
        days = int(d)
    parts = [float(p) for p in value.split(':')]
    result = days*86400
    for factor, part in zip((1, 60, 3600), reversed(parts)):
        result += factor*part
    return result


def process_activity(pid):
    proc = subprocess.run(['ps', '-axo', 'pid=,ppid=,time='], capture_output=True, text=True)
    if proc.returncode:
        raise RuntimeError(proc.stderr.strip())
    rows = {}
    for line in proc.stdout.splitlines():
        fields = line.split()
        if len(fields) == 3:
            rows[int(fields[0])] = (int(fields[1]), cpu_seconds(fields[2]))
    if pid not in rows:
        return False, 0
    descendants = {pid}
    while True:
        new = {p for p, (parent, _) in rows.items() if parent in descendants}
        if new <= descendants:
            break
        descendants |= new
    return True, sum(rows[p][1] for p in descendants)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', type=Path, required=True)
    parser.add_argument('--thread', required=True)
    parser.add_argument('--codex', default='/Applications/ChatGPT.app/Contents/Resources/codex')
    parser.add_argument('--interval', type=int, default=60)
    parser.add_argument('--stall-seconds', type=int, default=2400)
    args = parser.parse_args()
    state_file = args.run_dir/'monitor.json'
    sent = json.loads(state_file.read_text()).get('sent', {}) if state_file.exists() else {}
    last_activity = time.time()
    previous_activity = None
    while True:
        event = None
        detail = ''
        run_pid = 'unknown'
        try:
            state = json.loads((args.run_dir/'status.json').read_text())
            run_pid = str(state.get('pid', 'unknown'))
            if state['state'] in ('DONE', 'FAIL'):
                event = state['state']
                detail = state['stage']
            else:
                alive, cpu = process_activity(int(state['pid']))
                mtime = (args.run_dir/'run.log').stat().st_mtime
                activity = (cpu, mtime, state['stage'])
                if activity != previous_activity:
                    last_activity, previous_activity = time.time(), activity
                if not alive:
                    event, detail = 'FAIL', 'runner disappeared without a terminal state'
                elif time.time()-last_activity > args.stall_seconds:
                    event, detail = 'STALL', state['stage']
        except Exception as exc:
            event, detail = 'UNREACHABLE', str(exc)
        event_key = f'{run_pid}:{event}'
        if event and event_key not in sent:
            message = (f'DRAFT_PHASE_TRAINING {event}: {args.run_dir.name}; {detail}. '
                       f'Проверь {args.run_dir}/status.json и run.log, продолжи исходную задачу '
                       'до проверки четырёх моделей и итогового отчёта. Прод не переключать.')
            try:
                queued = subprocess.run([args.codex, 'queue', '--thread', args.thread, '--message', message],
                                        capture_output=True, text=True, timeout=30)
                if queued.returncode == 0:
                    sent[event_key] = time.time()
                print(f'{time.time():.0f} {event} queue_exit={queued.returncode} {queued.stdout} {queued.stderr}', flush=True)
            except subprocess.TimeoutExpired:
                # Delivery is uncertain: do not blindly enqueue the same event twice.
                sent[event_key] = 'delivery_uncertain'
                print(f'{time.time():.0f} {event} queue timeout; delivery uncertain', flush=True)
        atomic_json(state_file, dict(pid=os.getpid(), updated_at=time.time(), last_activity=last_activity, sent=sent))
        if event in ('DONE', 'FAIL') and event_key in sent:
            return
        time.sleep(args.interval)


if __name__ == '__main__':
    main()

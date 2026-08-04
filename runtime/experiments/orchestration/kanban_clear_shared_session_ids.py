#!/usr/bin/env python3
"""Clear shared origin session_id on open kanban tasks (collision hygiene)."""
from __future__ import annotations
import argparse, json, sqlite3, time
from pathlib import Path
DB = Path('/root/.hermes/kanban.db')

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--write', action='store_true')
    args = ap.parse_args()
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = list(conn.execute(
        """SELECT session_id, count(*) c, group_concat(id) ids
           FROM tasks
           WHERE ifnull(session_id,'')!='' AND status NOT IN ('done','archived')
           GROUP BY session_id HAVING c>1"""
    ))
    report = {'ts': int(time.time()), 'groups': [dict(r) for r in rows], 'cleared': []}
    if not rows:
        print(json.dumps({'ok': True, 'collisions': 0}))
        return
    for r in rows:
        sid = r['session_id']
        members = list(conn.execute(
            """SELECT id,status,assignee,substr(title,1,80) t FROM tasks
               WHERE session_id=? AND status NOT IN ('done','archived')""",
            (sid,),
        ))
        for m in members:
            report['cleared'].append({'session_id': sid, **dict(m)})
            if args.write:
                conn.execute('UPDATE tasks SET session_id=NULL WHERE id=?', (m['id'],))
    if args.write:
        conn.commit()
        out = Path('/root/main/runtime') / f"session_id_collision_fix_{time.strftime('%Y%m%d_%H%M%S')}_hygiene.json"
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(json.dumps({'ok': True, 'wrote': True, 'cleared_n': len(report['cleared']), 'report': str(out)}))
    else:
        print(json.dumps({'ok': True, 'wrote': False, 'collisions': len(rows), 'would_clear': len(report['cleared']), 'groups': report['groups']}, ensure_ascii=False))
    conn.close()

if __name__ == '__main__':
    main()

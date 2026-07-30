from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pwr_controller as pc


def _make_kanban(path: Path) -> None:
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE tasks(
          id TEXT PRIMARY KEY,title TEXT NOT NULL,body TEXT,assignee TEXT,status TEXT NOT NULL,
          priority INTEGER DEFAULT 0,created_by TEXT,created_at INTEGER NOT NULL,started_at INTEGER,
          completed_at INTEGER,workspace_kind TEXT DEFAULT 'dir',workspace_path TEXT,branch_name TEXT,
          claim_lock TEXT,claim_expires INTEGER,tenant TEXT,result TEXT,idempotency_key TEXT,
          consecutive_failures INTEGER DEFAULT 0,worker_pid INTEGER,last_failure_error TEXT,
          max_runtime_seconds INTEGER,last_heartbeat_at INTEGER,current_run_id INTEGER,
          workflow_template_id TEXT,current_step_key TEXT,skills TEXT,model_override TEXT,max_retries INTEGER,
          goal_mode INTEGER DEFAULT 0,goal_max_turns INTEGER,session_id TEXT,block_kind TEXT,
          block_recurrences INTEGER DEFAULT 0);
        CREATE TABLE task_links(parent_id TEXT,child_id TEXT,PRIMARY KEY(parent_id,child_id));
        CREATE TABLE task_comments(id INTEGER PRIMARY KEY AUTOINCREMENT,task_id TEXT,author TEXT,body TEXT,created_at INTEGER);
        CREATE TABLE task_runs(id INTEGER PRIMARY KEY AUTOINCREMENT,task_id TEXT,profile TEXT,step_key TEXT,status TEXT,
          claim_lock TEXT,claim_expires INTEGER,worker_pid INTEGER,max_runtime_seconds INTEGER,last_heartbeat_at INTEGER,
          started_at INTEGER,ended_at INTEGER,outcome TEXT,summary TEXT,metadata TEXT,error TEXT);
        """
    )
    con.commit(); con.close()


def _lane(tmp_path: Path) -> pc.Lane:
    home = tmp_path / "home"; home.mkdir()
    db = home / "kanban.db"; _make_kanban(db)
    root = tmp_path / "ruflo"; root.mkdir()
    return pc.Lane("orchestration1", "orchestration1", home, db, tmp_path / "runtime", root,
                   "test-ns", {"planner": "p", "worker": "w", "reviewer": "r"}, 3)


def _insert(lane: pc.Lane, tid: str, wid: str, phase: str, status: str, result: str = "") -> None:
    con = sqlite3.connect(lane.db)
    con.execute("INSERT INTO tasks(id,title,body,assignee,status,created_at,result) VALUES(?,?,?,?,?,?,?)",
                (tid, tid, f'PWR_WORKFLOW_ID: {wid}\nPWR_PHASE: {phase}\nPWR_ITERATION: 0',
                 {"PLAN":"p","WORK":"w","REVIEW":"r","REPLAN":"p"}[phase], status, 1, result))
    con.commit(); con.close()


def _workflow(lane: pc.Lane, wid: str, task: str, state: str = "PLANNING", iteration: int = 0, sigs=None) -> None:
    j = pc.journal(lane)
    j.execute("INSERT INTO workflows(workflow_id,title,goal,workspace,state,root_task_id,current_task_id,iteration,open_signatures,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
              (wid,"smoke","goal","dir:/tmp",state,task,task,iteration,sigs,1,1))
    j.commit(); j.close()


def test_planner_completion_materializes_worker(monkeypatch, tmp_path):
    lane=_lane(tmp_path); wid="pwr_a"; _insert(lane,"plan",wid,"PLAN","done","plan ok"); _workflow(lane,wid,"plan")
    made=[]
    monkeypatch.setattr(pc,"load_lane",lambda _: lane)
    monkeypatch.setattr(pc,"ruflo_store",lambda *a,**k:{"rc":0})
    monkeypatch.setattr(pc,"create_card",lambda *a,**kw: made.append(kw) or "work")
    out=pc.tick_lane(lane)
    assert out["changed"][0]["state"]=="WORKING"
    assert made[0]["assignee"]=="w" and "PWR_PHASE: WORK" in made[0]["body"]


def test_worker_completion_materializes_review(monkeypatch, tmp_path):
    lane=_lane(tmp_path); wid="pwr_b"; _insert(lane,"work",wid,"WORK","done","SUCCESS"); _workflow(lane,wid,"work","WORKING")
    made=[]
    monkeypatch.setattr(pc,"ruflo_store",lambda *a,**k:{"rc":0})
    monkeypatch.setattr(pc,"create_card",lambda *a,**kw: made.append(kw) or "review")
    out=pc.tick_lane(lane)
    assert out["changed"][0]["state"]=="REVIEWING"
    assert made[0]["assignee"]=="r" and made[0]["parent"]=="work"


def test_reviewer_issues_materializes_replan(monkeypatch, tmp_path):
    lane=_lane(tmp_path); wid="pwr_c"; _insert(lane,"review",wid,"REVIEW","done","ISSUES\nscope:type:broken"); _workflow(lane,wid,"review","REVIEWING")
    made=[]
    monkeypatch.setattr(pc,"ruflo_store",lambda *a,**k:{"rc":0})
    monkeypatch.setattr(pc,"create_card",lambda *a,**kw: made.append(kw) or "replan")
    out=pc.tick_lane(lane)
    assert out["changed"][0]["state"]=="REPLANNING"
    assert made[0]["assignee"]=="p" and "OPEN_REVIEWER_ISSUES" in made[0]["body"]


def test_reviewer_approve_is_terminal_and_durable(monkeypatch, tmp_path):
    lane=_lane(tmp_path); wid="pwr_d"; _insert(lane,"review",wid,"REVIEW","done","APPROVE"); _workflow(lane,wid,"review","REVIEWING")
    stored=[]
    monkeypatch.setattr(pc,"ruflo_store",lambda lane,key,value: stored.append((key,value)) or {"rc":0})
    out=pc.tick_lane(lane)
    assert out["changed"][0]["state"]=="APPROVED"
    assert any("workflow_approved" in k for k,_ in stored)


def test_failed_card_is_persisted_and_gives_up(monkeypatch, tmp_path):
    lane=_lane(tmp_path); wid="pwr_e"; _insert(lane,"work",wid,"WORK","blocked","")
    con=sqlite3.connect(lane.db); con.execute("UPDATE tasks SET last_failure_error='boom',consecutive_failures=2 WHERE id='work'"); con.commit(); con.close()
    _workflow(lane,wid,"work","WORKING")
    stored=[]
    monkeypatch.setattr(pc,"ruflo_store",lambda lane,key,value: stored.append((key,value)) or {"rc":0})
    out=pc.tick_lane(lane)
    assert out["changed"][0]["state"]=="GAVE_UP"
    assert any("observed:work:blocked" in k for k,_ in stored)
    assert any("workflow_terminal_failure" in k for k,_ in stored)

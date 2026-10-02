"""Delivery-boundary tests for the E-347 minute-model line and shadow journal.

Drives the REAL call-site wrapper `_ml_dispatch_tick_once_per_cycle` (which runs
the real `_ml_dispatch_tick` and then the minute-model hook) with a live-like
payload at 31:05, and asserts what leaves the system: the text handed to
`send_winline_odds_message` and the row appended to the shadow journal. Mocked:
only the Telegram sender, the Winline poller state seed (captured key/prices from
fixtures/winline_yangon_yache_map3_20260915.json) and dispatch persistence
(ledger / decision log / `_deliver_and_persist_signal`) so that "bets unchanged"
can be compared byte for byte. The minute model, the three draft verdict models,
the All draft model and the ELO helpers run for real.

Live path covered: PREMATCH_ML_ENABLED=0 (prod) -> verdicts arrive in the card's
DETAILS_KEY dict (refusal block + laning_serving.fallback_verdicts shape), All from
`win_model_veto.win_index_draft`.
"""
from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
import cyberscore_try as C  # noqa: E402
import minute_model_serving as M  # noqa: E402
from base import laning_serving as _laning_serving_module  # noqa: E402

WINLINE_FIXTURE = Path(__file__).parent / "fixtures/winline_yangon_yache_map3_20260915.json"
RADIANT, DIRE = "YANGON GALACTICOS", "YACHE123"
HEROES_R = {f"pos{i + 1}": {"hero_id": h, "account_id": 0} for i, h in enumerate([1, 2, 3, 4, 5])}
HEROES_D = {f"pos{i + 1}": {"hero_id": h, "account_id": 0} for i, h in enumerate([6, 7, 8, 9, 10])}
ELO_META = {"radiant_base_rating": 1500.0, "dire_base_rating": 1400.0,
            "source": "elo_composition_a"}          # the served variant-A source


def _wait():
    """Join the background worker (no-op against a build without one)."""
    drain = getattr(C, "_minute_model_drain", None)
    if drain is not None:
        assert drain(15.0), "minute-model worker did not go idle"


def _reset_state():
    for name in ("_minute_model_done", "_minute_model_inflight", "_minute_model_missing_detail",
                 "_minute_model_error_logged", "_minute_model_journal_done",
                 "_ml_dispatch_tick_last_cycle_key"):
        state = getattr(C, name, None)
        if state is not None:
            state.clear()
    if hasattr(C, "_minute_model_journal_path"):
        C._minute_model_journal_path = None
    if hasattr(C, "_minute_model_drop_logged"):
        C._minute_model_drop_logged = False


def _details(late=0.58, early_win=0.64, early_nw=0.66):
    """Fallback-shape verdicts exactly as laning_serving.fallback_verdicts builds them
    (probability = P(Radiant) for either side, confidence = max(p, 1-p))."""
    def verdict(p, note):
        return {"side": "Radiant" if p >= 0.5 else "Dire", "probability": p,
                "confidence": p if p >= 0.5 else 1.0 - p, "freshness_note": note}
    return {"refusal_reason": "prematch model disabled",
            "late": verdict(late, "данные до 29.08"),
            "early_win": verdict(early_win, "данные до 05.09"),
            "early_nw": verdict(early_nw, "данные до 01.09")}


def _blocks(details):
    return {"all_output": {C.win_model_veto.DETAILS_KEY: details}}


@pytest.fixture
def env(monkeypatch, tmp_path):
    monkeypatch.setenv("MINUTE_MODEL_SHADOW_PATH", str(tmp_path / "ml_minute_shadow.jsonl"))
    monkeypatch.setenv("WINLINE_ODDS_TELEGRAM_SENT_PATH", "0")
    monkeypatch.setenv("DISPATCH_MODE", "shadow")
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    for name in ("MINUTE_MODEL_ENABLED", "MINUTE_MODEL_TG", "MINUTE_MODEL_PATH"):
        monkeypatch.delenv(name, raising=False)
    M.reset()
    _wait()
    _reset_state()
    monkeypatch.setattr(C, "_lookup_match_map_num", lambda url: None)
    sent, delivered, logged = [], [], []
    monkeypatch.setattr(C, "send_winline_odds_message",
                        lambda message, **kw: sent.append(message) or True)
    key_fixture = json.loads(WINLINE_FIXTURE.read_text())["messages"][0]
    p1, p2 = key_fixture["observation"]["output_pair"]          # 1.45 / 2.5, team1 = YANGON
    monkeypatch.setattr(C, "_winline_odds_orientation_state", {
        key_fixture["canonical_key"]: {"p1": p1, "p2": p2, "status": "open",
                                       "last_quote_mono": time.monotonic()}})

    class Ledger:
        def __init__(self):
            self.keys = set()

        def as_set(self):
            return set(self.keys)

        def add(self, key):
            self.keys.add(tuple(key))

        def save(self):
            pass

    ledger = Ledger()
    monkeypatch.setattr(C, "_ml_dispatch_sent_ledger", lambda: ledger)
    monkeypatch.setattr(C, "_ml_dispatch_record_decisions",
                        lambda record, *, dedup_view: logged.append(
                            {k: v for k, v in record.items() if k != "ts"}))
    monkeypatch.setattr(C, "_deliver_and_persist_signal",
                        lambda *a, **k: delivered.append((a, k)) or True)
    monkeypatch.setattr(C.win_model_veto, "last_kills30",
                        lambda index: {"radiant": 0.395, "dire": 0.407, "total": 0.396})
    monkeypatch.setattr(_laning_serving_module, "verdicts",
                        lambda *a, **k: {"all": None, "lane": None})
    return {"sent": sent, "delivered": delivered, "logged": logged,
            "shadow": tmp_path / "ml_minute_shadow.jsonl", "ledger": ledger,
            "mp": monkeypatch}


def _drive(game_time, *, suffix=None, url="dltv.org/matches/2390001", kills=(18, 11), lead=3200,
           details=None, blocks=None, elo=ELO_META, map_num=3, live_league=None, wait=True):
    C._ml_dispatch_tick_once_per_cycle(
        match_key=f"{url}.{suffix if suffix is not None else int(game_time)}",
        radiant_team_name=RADIANT, dire_team_name=DIRE,
        live_league=(live_league if live_league is not None
                     else {"game_map_number": map_num, "match_id": 8200000001}),
        top="", mid="", bot="", protracker_payload=None, team_elo_block="",
        team_elo_meta=elo, game_time_seconds=game_time, radiant_lead=lead,
        live_kills=kills,
        radiant_heroes_and_pos=HEROES_R, dire_heroes_and_pos=HEROES_D,
        full_message_text="СТАВКА НА YANGON GALACTICOS x1\nYANGON GALACTICOS VS YACHE123",
        **(blocks if blocks is not None else _blocks(details or _details())),
    )
    if wait:
        _wait()


def _rows(env):
    _wait()
    path = env["shadow"]
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def _expected(minute, *, details, lead=3200, kills=(18, 11), elo=0.6400649998):
    index = C.win_model_veto.win_index_draft(HEROES_R, HEROES_D)
    assert index is not None, "All draft model must load for this test"
    p_all = 0.5 + index / 100.0
    return M.predict(minute, nw_lead=lead, radiant_kills=kills[0], dire_kills=kills[1],
                     p_all=p_all, p_late=details["late"]["probability"],
                     p_early_win=details["early_win"]["probability"],
                     p_enw_rad=details["early_nw"]["probability"], p_elo=elo), p_all


def test_fires_once_at_31_with_exact_text_and_shadow_row(env):
    details = _details()
    _drive(1865, details=details)
    expected, p_all = _expected(31, details=details)
    assert expected is not None and expected["p_stack"] is not None

    assert len(env["sent"]) == 1
    text = env["sent"][0]
    lines = text.splitlines()
    assert lines[0] == "🧮 Минутная модель 31:00 · YANGON GALACTICOS vs YACHE123 · карта 3"
    fav = RADIANT if expected["p_head"] >= 0.5 else DIRE
    fav_p = max(expected["p_head"], 1 - expected["p_head"])
    stack_fav = RADIANT if expected["p_stack"] >= 0.5 else DIRE
    stack_p = max(expected["p_stack"], 1 - expected["p_stack"])
    with_elo = (f"с ELO {stack_p * 100:.1f}%" if stack_fav == fav
                else f"с ELO {stack_fav} {stack_p * 100:.1f}%")
    assert lines[1] == f"{fav} {fav_p * 100:.1f}% (состояние+драфт) · {with_elo}"
    assert lines[2] == "NW +3.2k · киллы 18:11 · Winline 1.45 / 2.50"

    rows = _rows(env)
    assert len(rows) == 1
    row = rows[0]
    assert row["skip"] is None and row["tg"] == "queued" and row["tg_enabled"] is True and "tg_sent" not in row
    assert (row["base_url"], row["map_num"], row["match_id"], row["minute"]) == (
        "dltv.org/matches/2390001", 3, 8200000001, 31)
    assert (row["radiant"], row["dire"]) == (RADIANT, DIRE)
    assert row["game_time"] == 1865.0
    assert row["p_head"] == pytest.approx(expected["p_head"], abs=1e-12)
    # The expected stack uses the ELO constant 0.6400649998 (10 digits) while the
    # hook computes p_elo exactly from the ratings; that input gap (<=1e-9, asserted
    # below) propagates into p_stack, so 1e-12 only held for one set of draft models.
    assert row["p_stack"] == pytest.approx(expected["p_stack"], abs=1e-9)
    assert row["p_elo"] == pytest.approx(0.6400649998, abs=1e-9)
    assert row["model_sha256"] == expected["model_sha256"]
    assert row["inputs"]["nw_lead"] == 3200 and (row["inputs"]["radiant_kills"],
                                                 row["inputs"]["dire_kills"]) == (18, 11)
    # live verdict transforms: probability is P(Radiant) as is; All = 0.5 + index/100 unshifted.
    assert row["inputs"]["p_late"] == 0.58 and row["inputs"]["p_early_win"] == 0.64
    assert row["inputs"]["p_enw_rad"] == 0.66
    assert row["inputs"]["p_all"] == pytest.approx(p_all, abs=1e-12)
    assert row["verdicts"]["late"] == {"side": "Radiant", "probability": 0.58, "confidence": 0.58}
    assert row["winline_radiant"] == 1.45 and row["winline_dire"] == 2.5
    assert set(row["draft_models"]) == {"all", "late", "early_win", "early_nw"}
    assert all(row["draft_models"].values())
    assert env["delivered"] == []                       # shadow mode: still no bet either way


def test_same_minute_again_sends_nothing_new(env):
    _drive(1865)
    _drive(1866, suffix=19)
    _drive(1870, suffix=25)
    _drive(1950, suffix=40)
    assert len(env["sent"]) == 1 and len(_rows(env)) == 1


def test_minute_10_and_31_each_fire_once_on_one_map(env):
    _drive(605, suffix=3)
    _drive(640, suffix=5)
    _drive(1862, suffix=22)
    assert len(env["sent"]) == 2
    assert [r["minute"] for r in _rows(env)] == [10, 31]
    assert env["sent"][0].startswith("🧮 Минутная модель 10:00")


def test_first_seen_past_window_records_skip_without_telegram(env):
    _drive(2100)
    _drive(2110, suffix=33)
    assert env["sent"] == []
    rows = _rows(env)
    assert len(rows) == 1
    assert rows[0]["skip"] == "window_missed" and rows[0]["minute"] == 31
    assert rows[0]["tg"] == "off" and rows[0]["p_head"] is None


def test_missing_inputs_are_retried_inside_the_window_then_missed(env):
    _drive(1865, kills=None)
    assert env["sent"] == [] and _rows(env) == []        # nothing consumed yet
    _drive(1870, suffix=30, kills=None)                  # kills still missing
    _drive(2110, suffix=45, kills=(20, 13))              # past the window [1860, 2100)
    rows = _rows(env)
    assert env["sent"] == []
    assert len(rows) == 1 and rows[0]["skip"] == "window_missed"
    assert rows[0]["detail"] == "kills"

    _reset_state()
    env["shadow"].unlink()
    _drive(1865, kills=None)
    _drive(1900, suffix=31, kills=(19, 12))              # inputs arrive inside the window
    assert len(env["sent"]) == 1
    assert "киллы 19:12" in env["sent"][0] and _rows(env)[0]["game_time"] == 1900.0


def test_window_default_240s_late_first_poll_fires_and_past_it_is_missed(env):
    """W240: live polls reach the first poll 9..181 s after the minute; default window 240 s."""
    _drive(60 * 31 + 200, suffix=77)                     # 2060: inside [1860, 2100)
    rows = _rows(env)
    assert len(env["sent"]) == 1 and env["sent"][0].startswith("🧮 Минутная модель 31:00")
    assert len(rows) == 1 and rows[0]["skip"] is None and rows[0]["game_time"] == 2060.0
    assert rows[0]["minute"] == 31 and rows[0]["base_url"] == "dltv.org/matches/2390001"

    _reset_state()                                       # another map: first poll 250 s late
    env["shadow"].unlink()
    env["sent"].clear()
    _drive(60 * 31 + 250, suffix=78, url="dltv.org/matches/2390009")
    rows = _rows(env)
    assert env["sent"] == []
    assert len(rows) == 1 and rows[0]["skip"] == "window_missed" and rows[0]["minute"] == 31


@pytest.mark.parametrize("raw, fires_at_2060", [
    ("120", False),       # override honoured: 2060 is past [1860, 1980)
    ("300", True),
    ("600", True),
    ("59", True), ("601", True), ("abc", True), ("", True), ("nan", True),   # -> default 240
])
def test_window_env_override_and_invalid_values_fall_back_to_default(env, raw, fires_at_2060):
    env["mp"].setenv("MINUTE_MODEL_WINDOW_SECONDS", raw)
    _drive(60 * 31 + 200, suffix=79)
    rows = _rows(env)
    assert len(rows) == 1 and rows[0]["minute"] == 31
    if fires_at_2060:
        assert rows[0]["skip"] is None and len(env["sent"]) == 1
    else:
        assert rows[0]["skip"] == "window_missed" and env["sent"] == []


def test_dire_favoured_keeps_probability_as_p_radiant(env):
    details = _details(late=0.38, early_win=0.35, early_nw=0.30)
    # lead -15k: with heroes 1-5 vs 6-10 the real All draft model puts Radiant at 0.70, so a
    # -4.5k lead is not enough to flip the head (measured p_head 0.68); -15k makes Dire favoured
    _drive(1865, details=details, lead=-15000, kills=(9, 17))
    row = _rows(env)[0]
    assert row["verdicts"]["late"]["side"] == "Dire"
    assert row["inputs"]["p_late"] == 0.38              # NOT 0.62
    assert row["inputs"]["p_early_win"] == 0.35 and row["inputs"]["p_enw_rad"] == 0.30
    assert row["p_head"] < 0.5
    text = env["sent"][0]
    assert f"\n{DIRE} " in text and "NW -15.0k · киллы 9:17" in text


def test_prematch_disabled_empty_blocks_fall_back_to_real_models(env):
    _drive(1865, blocks={})                               # no card blocks at all
    rows = _rows(env)
    assert len(env["sent"]) == 1 and len(rows) == 1
    expected = _laning_serving_module.fallback_verdicts(HEROES_R, HEROES_D, draft_model=C.win_model_veto)
    for name in ("late", "early_win", "early_nw"):
        assert rows[0]["inputs"][{"late": "p_late", "early_win": "p_early_win",
                                  "early_nw": "p_enw_rad"}[name]] == expected[name]["probability"]
    assert 0.0 < rows[0]["p_head"] < 1.0


def test_no_elo_says_so_and_has_no_stack(env):
    _drive(1865, elo=None)
    row = _rows(env)[0]
    assert row["p_stack"] is None and row["p_elo"] is None and row["p_head"] is not None
    assert env["sent"][0].splitlines()[1].endswith(" · без ELO")


def test_no_winline_price_omits_the_part(env):
    env["mp"].setattr(C, "_winline_odds_orientation_state", {})
    _drive(1865)
    assert "Winline" not in env["sent"][0]
    row = _rows(env)[0]
    assert row["winline_radiant"] is None and row["winline_dire"] is None


def test_tg_switch_off_keeps_the_shadow_row(env):
    env["mp"].setenv("MINUTE_MODEL_TG", "0")
    _drive(1865)
    assert env["sent"] == []
    row = _rows(env)[0]
    assert row["tg_enabled"] is False and row["tg"] == "off" and row["p_head"] is not None


def test_disabled_sends_and_writes_nothing(env):
    env["mp"].setenv("MINUTE_MODEL_ENABLED", "0")
    _drive(1865)
    _drive(2100, suffix=9, url="dltv.org/matches/2390002")
    assert env["sent"] == [] and not env["shadow"].exists()


def _dispatch_view(env):
    return json.dumps({"logged": env["logged"], "delivered": repr(env["delivered"]),
                       "ledger": sorted(env["ledger"].keys, key=repr)}, sort_keys=True, default=str)


def _run_dispatch(env, mode):
    env["sent"].clear()
    env["logged"].clear()
    env["delivered"].clear()
    env["ledger"].keys.clear()
    _reset_state()
    with pytest.MonkeyPatch.context() as mp:
        # the dispatch payload stamps wall-clock times (observed_at, attempt_*, captured_at);
        # freeze the clock so the byte comparison sees only decisions, never timestamps
        mp.setattr(C.time, "time", lambda: 1790941000.0)
        _drive(1865, details=_details(late=0.72, early_win=0.70, early_nw=0.68), suffix=7)
    return _dispatch_view(env)


def test_dispatch_decisions_and_deliveries_are_identical_with_hook_failing_or_absent(env):
    env["mp"].setenv("DISPATCH_MODE", "ml")
    env["mp"].setattr(C, "_ml_dispatch_fresh_winline_price", lambda ctx, map_num: None)
    with pytest.MonkeyPatch.context() as mp:                # hook removed entirely
        mp.setattr(C, "_minute_model_tick_safe", lambda **kw: None)
        baseline = _run_dispatch(env, "ml")
    assert env["delivered"], "baseline must contain a real bet delivery or the comparison is empty"
    assert env["logged"] and env["logged"][0]["decisions"]

    with_hook = _run_dispatch(env, "ml")
    assert env["sent"], "hook must have produced its line in this run"
    assert with_hook == baseline

    def boom(*a, **k):
        raise RuntimeError("model exploded")
    env["mp"].setattr(M, "predict", boom)
    failing = _run_dispatch(env, "ml")
    assert env["sent"] == [] and failing == baseline


def test_sender_and_journal_failures_never_reach_dispatch(env, tmp_path):
    env["mp"].setenv("DISPATCH_MODE", "ml")
    env["mp"].setattr(C, "_ml_dispatch_fresh_winline_price", lambda ctx, map_num: None)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(C, "_minute_model_tick_safe", lambda **kw: None)
        baseline = _run_dispatch(env, "ml")

    def broken_send(message, **kw):
        raise OSError("telegram down")
    env["mp"].setattr(C, "send_winline_odds_message", broken_send)
    env["mp"].setenv("MINUTE_MODEL_SHADOW_PATH", str(tmp_path))     # a directory: open() fails
    assert _run_dispatch(env, "ml") == baseline


def test_exception_logs_one_line_per_key(env, caplog):
    def boom(*a, **k):
        raise RuntimeError("model exploded")
    env["mp"].setattr(M, "predict", boom)
    with caplog.at_level(logging.WARNING):
        for gt, sfx in ((1865, 1), (1866, 2), (1867, 3), (1868, 4)):
            _drive(gt, suffix=sfx)
    failures = [r for r in caplog.records if "minute model failed for" in r.getMessage()]
    assert len(failures) == 1
    assert env["sent"] == [] and not env["shadow"].exists()


# --- BC3: review findings (async worker, map identity, restart dedup, ELO source, order) ---

def _hook_kwargs(game_time=1865, **over):
    kw = dict(match_key=f"dltv.org/matches/2390001.{int(game_time)}",
              live_league={"game_map_number": 3, "match_id": 8200000001},
              game_time_seconds=game_time, radiant_lead=3200, live_kills=(18, 11),
              radiant_team_name=RADIANT, dire_team_name=DIRE,
              all_output={C.win_model_veto.DETAILS_KEY: _details()},
              radiant_heroes_and_pos=HEROES_R, dire_heroes_and_pos=HEROES_D,
              team_elo_meta=ELO_META)
    kw.update(over)
    return kw


def test_slow_telegram_does_not_block_the_poll_thread_or_dispatch(env):
    """Finding 1: a 2 s Telegram must not delay the hook, nor the dispatch call it follows."""
    def slow_send(message, **kw):
        time.sleep(2.0)
        env["sent"].append(message)
        return True
    env["mp"].setattr(C, "send_winline_odds_message", slow_send)
    started = time.monotonic()
    C._minute_model_tick_safe(**_hook_kwargs())
    assert time.monotonic() - started < 0.2, "the hook waited on Telegram / model work"
    # through the real call-site wrapper too: the whole dispatch call stays far below the 2 s send
    started = time.monotonic()
    _drive(1865, url="dltv.org/matches/2390002", wait=False)
    assert time.monotonic() - started < 1.5
    _wait()
    assert len(env["sent"]) == 2                         # both lines still arrive
    assert len(_rows(env)) == 2 and all(r["tg"] == "queued" for r in _rows(env))


def test_map_number_comes_from_series_game_and_unknown_never_fires(env):
    """Finding 2: series_game=2 with no series score is map 2, not map 1 and not slot 0."""
    seen = []
    env["mp"].setattr(C, "_ml_dispatch_fresh_winline_price",
                      lambda ctx, map_num: seen.append(map_num) or None)
    _drive(1865, live_league={"series_game": 2, "match_id": 8200000001})
    row = _rows(env)[0]
    assert row["map_num"] == 2 and seen == [2, 2]
    assert env["sent"][0].splitlines()[0].endswith("· карта 2")
    # SourceTV payload shape: explicit 0:0 score but a real series_game_number=2
    _drive(1865, url="dltv.org/matches/2390003",
           live_league={"radiant_series_wins": 0, "dire_series_wins": 0, "series_game": 2})
    assert [r["map_num"] for r in _rows(env)] == [2, 2]

    # unknown map: nothing fires inside the window, one explicit skip row after it
    before = len(env["sent"])
    _drive(1865, url="dltv.org/matches/2390004", live_league={"match_id": 1})
    _drive(2110, url="dltv.org/matches/2390004", live_league={"match_id": 1}, suffix=60)
    _drive(2115, url="dltv.org/matches/2390004", live_league={"match_id": 1}, suffix=61)
    assert len(env["sent"]) == before
    unknown = [r for r in _rows(env) if r["base_url"].endswith("2390004")]
    assert len(unknown) == 1 and unknown[0]["skip"] == "map_unknown" and unknown[0]["map_num"] is None


def test_restart_does_not_resend_a_line_already_in_the_journal(env):
    """Finding 3: a row written before the restart suppresses the second message."""
    _drive(1865)
    assert len(env["sent"]) == 1 and len(_rows(env)) == 1
    _reset_state()                                       # "restart": every in-memory set is gone
    _drive(1900, suffix=40)
    assert len(env["sent"]) == 1 and len(_rows(env)) == 1
    # an old row (older than the replay window) does not block a new map with the same key
    old = json.loads(env["shadow"].read_text().splitlines()[0])
    old["ts_utc"] = "2020-01-01T00:00:00+00:00"
    env["shadow"].write_text(json.dumps(old) + "\n")
    _reset_state()
    _drive(1900, suffix=41)
    assert len(env["sent"]) == 2


def test_stack_only_takes_the_served_variant_a_elo(env):
    """Finding 4: K24 (the A->K24 fallback) or an unknown source never reaches the stack."""
    for source in ("elo_composition_k24", None):
        meta = dict(ELO_META, source=source)
        if source is None:
            meta.pop("source")
        _reset_state()
        env["shadow"].unlink() if env["shadow"].exists() else None
        env["sent"].clear()
        _drive(1865, elo=meta)
        row = _rows(env)[0]
        assert row["p_stack"] is None and row["p_elo"] is None and row["p_head"] is not None
        assert row["elo_skip"] == f"elo_source_{source}"
        assert env["sent"][0].splitlines()[1].endswith(" · без ELO")
    _reset_state()
    env["shadow"].unlink()
    _drive(1865)                                         # variant A
    row = _rows(env)[0]
    assert row["p_stack"] is not None and row["p_elo"] is not None and row["elo_skip"] is None


def test_telegram_false_is_journaled_once_before_the_send_and_not_retried(env):
    """compute -> append (tg=queued) -> send; a false send is not retried, no duplicate row."""
    order = []
    env["mp"].setattr(C, "send_winline_odds_message",
                      lambda message, **kw: order.append("send") or False)
    real_append = C._minute_model_append_shadow
    env["mp"].setattr(C, "_minute_model_append_shadow",
                      lambda record: order.append("append") or real_append(record))
    _drive(1865)
    _drive(1870, suffix=30)
    _drive(1875, suffix=35)
    rows = _rows(env)
    assert order == ["append", "send"]
    assert len(rows) == 1 and rows[0]["tg"] == "queued" and rows[0]["p_head"] is not None


def test_journal_failure_is_logged_once_and_keeps_the_key_reserved(env, caplog):
    """Superseded in BC4: an append failure now also suppresses the send (see the BC4 test)."""
    def broken(record):
        raise OSError("disk full")
    env["mp"].setattr(C, "_minute_model_append_shadow", broken)
    with caplog.at_level(logging.WARNING):
        for gt, sfx in ((1865, 1), (1866, 2), (1867, 3)):
            _drive(gt, suffix=sfx)
    assert env["sent"] == []                              # no line without its evidence row
    assert len([r for r in caplog.records if "journal append failed" in r.getMessage()]) == 1


def test_two_threads_write_one_window_missed_row(env):
    """Finding 6: check-and-reserve under one lock."""
    import threading
    barrier = threading.Barrier(2)
    real_enabled = M.enabled

    def synced_enabled():
        barrier.wait(timeout=5)
        return real_enabled()
    env["mp"].setattr(M, "enabled", synced_enabled)
    errors = []

    def run():
        try:
            C._minute_model_tick(**_hook_kwargs(game_time=2100))
        except Exception as exc:                          # noqa: BLE001
            errors.append(exc)
    threads = [threading.Thread(target=run) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert not errors
    rows = _rows(env)
    assert len(rows) == 1 and rows[0]["skip"] == "window_missed"


def test_full_queue_drops_and_logs_once_without_blocking(env, caplog):
    import queue as _queue
    if not hasattr(C, "_minute_model_enqueue"):
        pytest.skip("no worker")
    gate = __import__("threading").Event()
    C._minute_model_enqueue(lambda: gate.wait(10))        # park the worker
    try:
        started = time.monotonic()
        with caplog.at_level(logging.WARNING):
            results = [C._minute_model_enqueue(lambda: None) for _ in range(C._MINUTE_MODEL_QUEUE_MAX + 5)]
        assert time.monotonic() - started < 1.0
        assert results.count(False) >= 5
        assert len([r for r in caplog.records if "queue full" in r.getMessage()]) == 1
    finally:
        gate.set()
        _wait()


# --- BC4: round-2 review (journal replay off the poll thread, evidence before send) ---

import builtins  # noqa: E402
import threading  # noqa: E402

WORKER_NAME = "minute-model-worker"


def _seed_journal(env, **over):
    """A recent row for an UNRELATED match so the journal file exists and is non-empty."""
    row = {"ts_utc": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
           "base_url": "dltv.org/matches/9999999", "map_num": 1, "minute": 31}
    row.update(over)
    with open(env["shadow"], "a", encoding="utf-8") as handle:
        handle.write(json.dumps(row) + "\n")


def _spy_journal_reads(env, *, sleep_off_worker=0.0, sleep_any=0.0):
    """Record every read-open of the journal; optionally make it slow (race / blocking)."""
    real_open = builtins.open
    journal = str(env["shadow"])
    seen = []

    def spy(file, mode="r", *args, **kwargs):
        if str(file) == journal and "r" in mode:
            name = threading.current_thread().name
            seen.append(name)
            if name != WORKER_NAME and sleep_off_worker:
                time.sleep(sleep_off_worker)
            if sleep_any:
                time.sleep(sleep_any)
        return real_open(file, mode, *args, **kwargs)
    env["mp"].setattr(builtins, "open", spy)
    return seen


def test_poll_thread_never_reads_the_journal(env):
    """Round-2 blocker: the 10:00 poll must not wait for a journal read (a 2 s read on the
    poll thread would delay every bet that follows the hook)."""
    _seed_journal(env)
    seen = _spy_journal_reads(env, sleep_off_worker=2.0)
    _drive(605, url="dltv.org/matches/2390009")           # warm models / lazy imports
    _reset_state()                                        # next poll is a "first poll after restart"
    seen.clear()
    started = time.monotonic()
    _drive(605, url="dltv.org/matches/2390010", wait=False)
    elapsed = time.monotonic() - started
    _wait()
    assert elapsed < 0.2, f"poll thread blocked {elapsed:.2f}s"
    assert [name for name in seen if name != WORKER_NAME] == []
    assert len(env["sent"]) == 2                          # both lines still arrive via the worker


def test_replay_race_with_the_row_already_journaled_sends_nothing(env):
    """Round-2 major: the row for K exists; two threads driving K inside the window -> 0 sends."""
    _drive(1865)
    assert len(env["sent"]) == 1 and len(_rows(env)) == 1
    env["sent"].clear()
    _reset_state()                                        # restart: in-memory dedup gone
    _spy_journal_reads(env, sleep_any=0.3)                # widen the replay window
    barrier = threading.Barrier(2)
    real_enabled = M.enabled

    def synced_enabled():
        if threading.current_thread().name != WORKER_NAME:
            barrier.wait(timeout=5)                       # the two poll threads start together
        return real_enabled()
    env["mp"].setattr(M, "enabled", synced_enabled)
    errors = []

    def run():
        try:
            C._minute_model_tick(**_hook_kwargs(game_time=1870))
        except Exception as exc:                          # noqa: BLE001
            errors.append(exc)
    threads = [threading.Thread(target=run) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    _wait()
    assert not errors
    assert env["sent"] == [] and len(_rows(env)) == 1


def test_append_failure_sends_nothing_and_logs_once_then_restart_stays_silent(env, caplog):
    """Round-2 major: no Telegram line without its evidence row; no resend after a restart."""
    def broken(record):
        raise OSError("disk full")
    real_append = C._minute_model_append_shadow
    env["mp"].setattr(C, "_minute_model_append_shadow", broken)
    with caplog.at_level(logging.WARNING):
        for gt, sfx in ((1865, 1), (1866, 2), (1867, 3)):
            _drive(gt, suffix=sfx)
    assert env["sent"] == []
    assert len([r for r in caplog.records if "journal append failed" in r.getMessage()]) == 1
    # row written but the send dies (crash window): the row says "queued"; a restart never resends
    env["mp"].setattr(C, "_minute_model_append_shadow", real_append)
    _reset_state()

    def boom(message, **kw):
        raise OSError("telegram down")
    env["mp"].setattr(C, "send_winline_odds_message", boom)
    _drive(1865, suffix=11)
    rows = _rows(env)
    assert len(rows) == 1 and rows[0]["tg"] == "queued" and "tg_sent" not in rows[0]
    env["mp"].setattr(C, "send_winline_odds_message",
                      lambda message, **kw: env["sent"].append(message) or True)
    _reset_state()                                        # restart with the row present
    _drive(1870, suffix=12)
    assert env["sent"] == [] and len(_rows(env)) == 1


def test_full_queue_releases_the_reservation_so_a_later_poll_enqueues(env):
    """Round-2 minor: a dropped job must not retire the key for the whole window."""
    gate = threading.Event()
    C._minute_model_enqueue(lambda: gate.wait(10))        # park the worker
    try:
        while C._minute_model_enqueue(lambda: None):       # fill the queue
            pass
        C._minute_model_tick(**_hook_kwargs(game_time=1865))
        assert env["sent"] == []
    finally:
        gate.set()
        _wait()
    C._minute_model_tick(**_hook_kwargs(game_time=1870))  # next poll, same window
    _wait()
    assert len(env["sent"]) == 1 and len(_rows(env)) == 1

"""Delivery-boundary acceptance for same-team bundles (card ingame-0u31).

Captured decisions are replayed, not re-evaluated. Telegram and the fresh
Winline quote are the only mocked delivery dependencies; both ledgers are real.
See fixtures/ml_dispatch_multi_ticks_20261010.README.md for provenance/goldens.
"""
from __future__ import annotations

import dataclasses
import json
from pathlib import Path
import sys
import types

import pytest

from base import ml_dispatch as md

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
try:
    import keys  # noqa: F401,E402
except ImportError:
    _test_keys = types.ModuleType("keys")
    _test_keys.api_to_proxy = {}
    _test_keys.BOOKMAKER_PROXY_URL = None
    _test_keys.BOOKMAKER_PROXY_POOL = []
    _test_keys.DLTV_PROXY_POOL = []
    # The isolated delivery tree has no git-ignored keys.py; this stub is then the
    # one every later test module in the same pytest process sees, so it carries
    # the attributes functions.send_message reads (test_telegram_nonbet_silent).
    _test_keys.Token = "0:test-token"
    _test_keys.Chat_id = None
    _test_keys.Chat_ids = []
    sys.modules["keys"] = _test_keys

import cyberscore_try as C  # noqa: E402
import win_model_veto  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"
ROWS = json.loads((FIXTURES / "ml_dispatch_multi_ticks_20261010.json").read_text())
GOLDEN_PATH = FIXTURES / "ml_dispatch_bundle_golden_20261010.json"
# Select by team pair, never silently relabel the captured Dire target Blasterbl.
LEGION = next(r for r in ROWS if r["teams"]["radiant"] == "LEGION")
SHADOW = next(r for r in ROWS if r["teams"]["radiant"] == "Shadow Dance")
OPPOSITE = next(r for r in ROWS if r["teams"]["radiant"] == "Aurora Gaming")


# Measured on the captured LEGION map (bundle 257 chars, singles 91/86/159 at pad 0):
# bundle 4048 chars fits, +123-char position footer does not; every single + footer fits.
PAD_FITS_ONLY_WITHOUT_FOOTER = 3790


def _decisions(row):
    return [md.Decision(
        **{name: d.get(name, []) if name == "models_against" else d[name]
           for name in ("market", "target_side", "target_team", "rule", "models_for",
                        "models_against", "timing", "expected_wr", "min_odds", "reasons")},
        floor_informational=d["rule"] == "kills_panel_window",
    ) for d in row["decisions"]]


def _panel_body(row, pad=0):
    """Adapt the existing panel-kills tick's body to this captured map's verdict."""
    panel = row["verdicts"]["panel_w_5_15"]
    body = (f"СТАВКА НА x\n{row['teams']['radiant']} VS {row['teams']['dire']}\n"
            f"🤖 ML:\n  окно 5-15: {panel['side']} {panel['confidence'] * 100:.0f}%")
    return body + ("\n" + "п" * pad if pad else "")


def _drive_tick(monkeypatch, tmp_path, row, *, bundle=None, price=3.0, decisions=None,
                fingerprint=False, premark=(), pad=0, on_send=None, pos_warning=None,
                expect_journal=True):
    """``fingerprint``: register the map's dedup fingerprint (prod registers it
    before delivery; the default leaves it fail-open like the other tests).
    ``premark``: first lines already "sent". ``price``: float or callable(ctx, map).
    ``on_send``: called with the text at every confirmed send. ``pos_warning``: the
    sourcetv position footer `_deliver_and_persist_signal` appends to the final
    payload. ``expect_journal=False``: the tick may abort (proven send failure)
    before its decision journal and then returns ``None`` for it."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("DISPATCH_MODE", "ml")
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    monkeypatch.delenv("ML_DISPATCH_PANEL_KILLS_REQUIRE_TIER1", raising=False)
    monkeypatch.delenv("ML_DISPATCH_KILLS_FLOOR", raising=False)
    assert md.Config.from_env().kills_floor_enabled is True  # E-359 production default
    if bundle is None:
        monkeypatch.delenv("ML_DISPATCH_BUNDLE_SAME_TEAM", raising=False)
    else:
        monkeypatch.setenv("ML_DISPATCH_BUNDLE_SAME_TEAM", bundle)
    monkeypatch.setenv("ML_DISPATCH_SENT_PATH", str(tmp_path / "ml_sent.json"))
    monkeypatch.setenv("ML_DISPATCH_LOG_PATH", str(tmp_path / "decisions.jsonl"))
    monkeypatch.setenv("MAP_VERDICTS_PATH", str(tmp_path / "map_verdicts.json"))
    monkeypatch.setenv("SENT_SIGNAL_FINGERPRINT_PATH", str(tmp_path / "fingerprints.json"))
    for name in ("MAP_ID_CHECK_PATH", "BET_DISPATCH_LEDGER_PATH",
                 "SENT_SIGNAL_JOURNAL_PATH", "SENT_SIGNAL_JOURNAL_FALLBACK_PATH",
                 "UNCERTAIN_SIGNAL_DELIVERY_PATH", "UNCERTAIN_SIGNAL_DELIVERY_FALLBACK_PATH"):
        monkeypatch.setattr(C, name, str(tmp_path / (name.lower() + ".jsonl")))
    # Match production --no-odds configuration, without replacing its gate.
    monkeypatch.setattr(C, "BOOKMAKER_PREFETCH_ENABLED", False)
    monkeypatch.setattr(C, "BOOKMAKER_PREFETCH_GATE_MODE", "presence")
    monkeypatch.setattr(C, "TEST_DISABLE_ADD_URL", False)
    monkeypatch.setattr(C, "_ml_dispatch_sent_ledger_instance", None)
    for name in ("_SIGNAL_DEDUP_FINGERPRINTS", "_ml_dispatch_decisions_log_last_fingerprint",
                 "_PLAYER_DENYLIST_BY_MAP", "_PLAYER_DENYLIST_TEAM_NAMES",
                 "_SOURCETV_POS_WARNING_BY_KEY"):
        monkeypatch.setattr(C, name, {})
    monkeypatch.setattr(C, "_SENT_SIGNAL_DEDUP_KEYS", set())
    monkeypatch.setattr(C, "processed_urls_cache", set())

    if pos_warning:
        C._SOURCETV_POS_WARNING_BY_KEY[C._signal_fingerprint_registry_key(row["match_key"])] = (
            pos_warning)
    if fingerprint or premark:
        C._SIGNAL_DEDUP_FINGERPRINTS[C._signal_fingerprint_registry_key(row["match_key"])] = (
            "a|b|map1")
    for header in premark:
        C._signal_fingerprint_mark_sent(row["match_key"], header)
    selected = _decisions(row) if decisions is None else decisions
    result = md.EvalResult(selected, [], row["underdog_side"], row["elo_diff"], "ml")
    monkeypatch.setattr(md, "evaluate", lambda ctx, cfg: result)
    sent = []
    monkeypatch.setattr(C, "send_message", lambda text, **kwargs: (
        on_send and on_send(text), sent.append(text), True)[-1])
    monkeypatch.setattr(C, "_ml_dispatch_fresh_winline_price",
                        price if callable(price) else (lambda ctx, map_num: price))
    details = {k: row["verdicts"][k]
               for k in ("early_nw", "early_win", "late", "panel_w_5_15", "kills30")}
    C._ml_dispatch_tick(
        match_key=row["match_key"], radiant_team_name=row["teams"]["radiant"],
        dire_team_name=row["teams"]["dire"], live_league={"map_num": row["map_num"]},
        top="", mid="", bot="", protracker_payload=None, team_elo_block="", team_elo_meta={},
        game_time_seconds=row["game_time"], radiant_lead=row["radiant_networth_lead"],
        early_output={win_model_veto.DETAILS_KEY: details}, mid_output={}, all_output={},
        full_message_text=_panel_body(row, pad),
    )
    bet_path = Path(C.BET_DISPATCH_LEDGER_PATH)
    bets = [json.loads(line) for line in bet_path.read_text().splitlines()] if bet_path.exists() else []
    ml_path = tmp_path / "ml_sent.json"
    ml_keys = json.loads(ml_path.read_text()) if ml_path.exists() else []
    journal_path = tmp_path / "decisions.jsonl"
    if not expect_journal:
        journal = (json.loads(journal_path.read_text().splitlines()[-1])
                   if journal_path.exists() else None)
        return sent, bets, ml_keys, journal
    assert journal_path.exists(), "tick swallowed an exception before its decision journal"
    journal = json.loads(journal_path.read_text().splitlines()[-1])
    return sent, bets, ml_keys, journal


def _assert_ledgers(row, bets, ml_keys, decisions):
    assert len(bets) == len(decisions)
    assert {b["market"] for b in bets} == {d.market for d in decisions}
    by_market = {b["market"]: b for b in bets}
    for d in decisions:
        b = by_market[d.market]
        assert b["min_odds"] == pytest.approx(d.min_odds)
        assert b["expected_wr"] == pytest.approx(d.expected_wr)
        assert b["rule"] == d.rule
        assert b["side"] == d.target_side.lower()
        assert b["target_team_name"] == d.target_team
        assert b["map_num"] == row["map_num"]
    assert {tuple(k) for k in ml_keys} == {
        (row["base_url"], row["map_num"], d.market, d.target_side) for d in decisions}


def _assert_bundle(text, row, decisions):
    by_market = {d.market: d for d in decisions}
    team = C.normalize_team_name_display(decisions[0].target_team)
    primary = "win" if "win" in by_market else "kills_window"
    header = f"СТАВКА НА {team} x1" if primary == "win" else f"СТАВКА НА Ранние килы 5-15 {team}"
    lines = text.splitlines()
    assert lines[0] == header
    assert lines[1] == f"Ставки на {team} ({len(decisions)}):"
    labels = (("win", "Победа x1"), ("kills_window", "Ранние килы 5-15"),
              ("kills_total", "Тотал килов БОЛЬШЕ (ИТБ 29,5)"))
    bullets = [f"• {label} — от кэфа {by_market[market].min_odds:.2f}"
               for market, label in labels if market in by_market]
    assert lines[2:2 + len(bullets)] == bullets
    assert [line for line in lines if line.startswith("• ")] == bullets
    assert "Ставить от кэфа" not in text
    panel_decision = by_market["kills_window"]
    panel_line = (f"🤖 Ранние килы 5-15 по панели ML: {panel_decision.target_team} "
                  f"({panel_decision.target_side}) {panel_decision.expected_wr * 100:.0f}%")
    assert lines.count(panel_line) == 1
    for body_line in _panel_body(row).splitlines()[1:]:
        assert lines.count(body_line) == 1


@pytest.mark.parametrize("row", [SHADOW, LEGION], ids=["shadow_radiant_00", "legion_blasterbl_dire"])
def test_same_team_three_bets_one_send(monkeypatch, tmp_path, row):
    sent, bets, ml_keys, journal = _drive_tick(monkeypatch, tmp_path, row)
    decisions = _decisions(row)
    _assert_ledgers(row, bets, ml_keys, decisions)
    assert len(journal["delivered"]) == 3
    assert all(d["status"] == "delivered" for d in journal["delivered"])
    assert len(sent) == 1, "same-team tick must send one bundle, not one message per bet"
    _assert_bundle(sent[0], row, decisions)


def test_win_floor_block_bundles_only_two_kills_bets(monkeypatch, tmp_path):
    sent, bets, ml_keys, journal = _drive_tick(monkeypatch, tmp_path, LEGION, price=1.80)
    remaining = [d for d in _decisions(LEGION) if d.market != "win"]
    # Verify the real floor before the expected-red send-count assertion.
    _assert_ledgers(LEGION, bets, ml_keys, remaining)
    statuses = {d["market"]: d["status"] for d in journal["delivered"]}
    assert statuses == {"win": "blocked", "kills_total": "delivered", "kills_window": "delivered"}
    assert len(sent) == 1, "blocked win must leave one bundle of the two kills bets"
    _assert_bundle(sent[0], LEGION, remaining)


def _win_header(row):
    win = next(d for d in _decisions(row) if d.market == "win")
    return f"СТАВКА НА {C.normalize_team_name_display(win.target_team)} x1"


def test_sent_win_fingerprint_does_not_swallow_the_kills_bundle(monkeypatch, tmp_path):
    """Win single header already sent -> win via the single path (not resent), the
    two kills bets are ONE bundle under their own header, both in the bet ledger."""
    sent, bets, ml_keys, journal = _drive_tick(
        monkeypatch, tmp_path, LEGION, premark=[_win_header(LEGION)])
    remaining = [d for d in _decisions(LEGION) if d.market != "win"]
    assert len(sent) == 1, sent
    assert not any(text.startswith(_win_header(LEGION)) for text in sent)
    _assert_bundle(sent[0], LEGION, remaining)
    assert sent[0].splitlines()[0].startswith("СТАВКА НА Ранние килы 5-15 ")
    assert {b["market"] for b in bets} == {"kills_window", "kills_total"}
    assert all(d["status"] == "delivered" for d in journal["delivered"])
    assert {tuple(k) for k in ml_keys} == {
        (LEGION["base_url"], LEGION["map_num"], d.market, d.target_side)
        for d in _decisions(LEGION)}


def test_bundle_marks_every_member_single_fingerprint(monkeypatch, tmp_path):
    with pytest.MonkeyPatch.context() as patch:
        singles, *_ = _drive_tick(patch, tmp_path / "singles", LEGION, bundle="0",
                                  fingerprint=True)
    assert len(singles) == 3
    sent, *_ = _drive_tick(monkeypatch, tmp_path / "bundle", LEGION, fingerprint=True)
    assert len(sent) == 1
    for text in singles:
        assert C._signal_fingerprint_already_sent(LEGION["match_key"], text), text.splitlines()[0]


def test_uncertain_bundle_send_marks_every_member_fingerprint(monkeypatch, tmp_path):
    """Uncertain bundle send = "maybe sent" for EVERY bet: a later tick must not
    re-bundle and resend the kills bets (the old per-message code never did)."""
    attempts = []

    def uncertain(text):
        attempts.append(text)
        raise C.TelegramSendError("timeout after request", delivery_uncertain=True)

    sent, *_ = _drive_tick(monkeypatch, tmp_path, LEGION, fingerprint=True, on_send=uncertain)
    assert sent == [] and len(attempts) == 1, "tick 1 must attempt exactly one bundle send"
    sent2, *_ = _drive_tick(monkeypatch, tmp_path, LEGION, fingerprint=True)
    assert sent2 == [], [text.splitlines()[0] for text in sent2]


def _reads_counter(prices):
    """Price getter returning ``prices[i]`` on the i-th read (last one repeats)."""
    reads = []

    def price(ctx, map_num):
        reads.append(1)
        return prices[min(len(reads), len(prices)) - 1]

    return reads, price


def _count_floor_reads(monkeypatch, reads):
    """Price reads made INSIDE the delivery win-floor check (other tick code
    reads the quote too); returns the per-call list."""
    original = C._ml_dispatch_min_odds_reject_for_delivery
    per_call = []

    def counting(*args, **kwargs):
        before = len(reads)
        try:
            return original(*args, **kwargs)
        finally:
            per_call.append(len(reads) - before)

    monkeypatch.setattr(C, "_ml_dispatch_min_odds_reject_for_delivery", counting)
    return per_call


def test_win_floor_is_read_once_at_delivery_and_passes(monkeypatch, tmp_path):
    reads, price = _reads_counter([3.0, 1.8])  # floor 2.05: only the first read passes
    per_call = _count_floor_reads(monkeypatch, reads)
    sent, bets, ml_keys, journal = _drive_tick(monkeypatch, tmp_path, LEGION, price=price)
    assert len(sent) == 1
    _assert_bundle(sent[0], LEGION, _decisions(LEGION))
    assert per_call == [1], "one win floor evaluation, one price read, per bundle attempt"
    _assert_ledgers(LEGION, bets, ml_keys, _decisions(LEGION))


def test_win_floor_below_at_delivery_blocks_win_and_bundles_the_kills(monkeypatch, tmp_path):
    """The floor is read where HEAD reads it (inside delivery): a price that is
    below the floor THERE blocks the win, even if another read would pass."""
    reads, price = _reads_counter([1.8, 3.0])
    per_call = _count_floor_reads(monkeypatch, reads)
    sent, bets, ml_keys, journal = _drive_tick(monkeypatch, tmp_path, LEGION, price=price)
    remaining = [d for d in _decisions(LEGION) if d.market != "win"]
    assert len(sent) == 1, sent
    assert not any(text.startswith(_win_header(LEGION)) for text in sent)
    _assert_bundle(sent[0], LEGION, remaining)
    assert sum(per_call) == 1, f"win floor read once; the kills bundle has no price: {per_call}"
    _assert_ledgers(LEGION, bets, ml_keys, remaining)
    statuses = {d["market"]: d["status"] for d in journal["delivered"]}
    assert statuses == {"win": "blocked", "kills_total": "delivered", "kills_window": "delivered"}


def test_price_falling_below_floor_during_presence_gate_blocks_the_win(monkeypatch, tmp_path):
    """Verifier round 2 (P1): the quote drops 3.0 -> 1.8 while the Winline presence
    gate runs; HEAD reads the floor AFTER that gate and blocks the win."""
    state = {"price": 3.0}
    original = C._winline_presence_reject_for_delivery

    def presence_then_price_drop(*args, **kwargs):
        verdict = original(*args, **kwargs)
        state["price"] = 1.8
        return verdict

    monkeypatch.setattr(C, "_winline_presence_reject_for_delivery", presence_then_price_drop)
    sent, bets, ml_keys, journal = _drive_tick(
        monkeypatch, tmp_path, LEGION, price=lambda ctx, map_num: state["price"])
    remaining = [d for d in _decisions(LEGION) if d.market != "win"]
    assert not any(text.startswith(_win_header(LEGION)) for text in sent), sent
    assert len(sent) == 1
    _assert_bundle(sent[0], LEGION, remaining)
    _assert_ledgers(LEGION, bets, ml_keys, remaining)


def _race_on_header(monkeypatch, header_prefix):
    """Another sender owns the dedup key of the message whose first line starts
    with ``header_prefix`` (it reserves it between our pre-check and delivery)."""
    original = C._signal_fingerprint_try_reserve

    def reserve(match_key, text):
        if str(text).startswith(header_prefix):
            return False, "owned-by-another-sender"
        return original(match_key, text)

    monkeypatch.setattr(C, "_signal_fingerprint_try_reserve", reserve)


def test_dedup_race_on_the_win_header_still_sends_both_kills_bets(monkeypatch, tmp_path):
    """Verifier round 2 (P1): the win header is reserved by someone else after the
    group pre-check. HEAD dedups ONLY the win and still sends the kills bets."""
    _race_on_header(monkeypatch, _win_header(LEGION))
    ml_path = tmp_path / "ml_sent.json"
    at_send = []

    def on_send(text):
        at_send.append({tuple(k) for k in json.loads(ml_path.read_text())}
                       if ml_path.exists() else set())

    sent, bets, ml_keys, journal = _drive_tick(monkeypatch, tmp_path, LEGION, on_send=on_send)
    decisions = _decisions(LEGION)
    remaining = [d for d in decisions if d.market != "win"]
    assert len(sent) == 1, sent
    _assert_bundle(sent[0], LEGION, remaining)
    assert {b["market"] for b in bets} == {"kills_window", "kills_total"}, "no row for the unsent win"
    assert {tuple(k) for k in ml_keys} == {
        (LEGION["base_url"], LEGION["map_num"], d.market, d.target_side) for d in decisions}
    # win key right after its dedup (HEAD single semantics), kills keys only after their send
    win = next(d for d in decisions if d.market == "win")
    assert at_send == [{(LEGION["base_url"], LEGION["map_num"], "win", win.target_side)}]
    statuses = {d["market"]: d["status"] for d in journal["delivered"]}
    assert statuses == {"win": "delivered", "kills_total": "delivered", "kills_window": "delivered"}


def _registry_keys(tmp_path):
    path = tmp_path / "fingerprints.json"
    return set(json.loads(path.read_text())) if path.exists() else set()


def test_member_reservation_lost_to_another_sender_falls_back_to_singles(monkeypatch, tmp_path):
    """Round 5: a bundle MEMBER's own header owned by another sender at send time
    must not ride in the bundle (that would duplicate the contested bet). Nothing
    the bundle attempt reserved stays held; every member goes through the single
    path and the contested one dedups exactly like HEAD."""
    _race_on_header(monkeypatch, "СТАВКА НА Тотал")
    sent, bets, ml_keys, journal = _drive_tick(monkeypatch, tmp_path, LEGION, fingerprint=True)
    decisions = _decisions(LEGION)
    assert not any("Ставки на" in text for text in sent), "no bundle with a contested member"
    heads = sorted(text.splitlines()[0] for text in sent)
    assert len(sent) == 2, heads
    assert sum(t.startswith(_win_header(LEGION)) for t in sent) == 1
    assert sum(t.startswith("СТАВКА НА Ранние килы 5-15 ") for t in sent) == 1
    assert not any(t.startswith("СТАВКА НА Тотал") for t in sent)
    assert {b["market"] for b in bets} == {"win", "kills_window"}, "rows only for the sent bets"
    assert {tuple(k) for k in ml_keys} == {
        (LEGION["base_url"], LEGION["map_num"], d.market, d.target_side) for d in decisions}
    statuses = {d["market"]: d["status"] for d in journal["delivered"]}
    assert statuses == {"win": "delivered", "kills_total": "delivered", "kills_window": "delivered"}
    assert _registry_keys(tmp_path) == {C._signal_dedup_key(LEGION["match_key"], t) for t in sent}


def test_member_reservation_raising_falls_back_to_singles(monkeypatch, tmp_path):
    """The member reservation RAISES during the bundle attempt: same single fallback,
    every key the attempt reserved (primary + earlier members) is released first."""
    original = C._signal_fingerprint_try_reserve
    calls = []

    def reserve(match_key, text):
        if str(text).startswith("СТАВКА НА Тотал") and not calls:
            calls.append(text)
            raise RuntimeError("fingerprint store unavailable")
        return original(match_key, text)

    monkeypatch.setattr(C, "_signal_fingerprint_try_reserve", reserve)
    sent, bets, ml_keys, journal = _drive_tick(monkeypatch, tmp_path, LEGION, fingerprint=True)
    decisions = _decisions(LEGION)
    assert len(calls) == 1, "the bundle attempt hit the raising member"
    assert not any("Ставки на" in text for text in sent), "no bundle after a failed member reserve"
    assert len(sent) == 3, sorted(text.splitlines()[0] for text in sent)
    _assert_ledgers(LEGION, bets, ml_keys, decisions)
    assert all(d["status"] == "delivered" for d in journal["delivered"])
    assert _registry_keys(tmp_path) == {C._signal_dedup_key(LEGION["match_key"], t) for t in sent}


def test_footer_pushes_the_bundle_over_4096_and_falls_back_to_singles(monkeypatch, tmp_path):
    """Verifier round 2 (P2): the position warning is appended to the FINAL payload;
    the bundle that fitted before it (<=4096) must not be sent over the limit."""
    footer = "⚠️ " + "позиции не подтверждены " * 5
    sent, bets, ml_keys, _ = _drive_tick(
        monkeypatch, tmp_path / "footer", LEGION, pad=PAD_FITS_ONLY_WITHOUT_FOOTER,
        pos_warning=footer)
    assert len(sent) == 3 and all(len(t) <= 4096 for t in sent), [len(t) for t in sent]
    assert all("Ставки на" not in t for t in sent)
    assert all(t.rstrip().endswith(footer.strip()) for t in sent)
    _assert_ledgers(LEGION, bets, ml_keys, _decisions(LEGION))
    # control: the same composed bundle without a footer fits and goes as ONE message
    plain, *_ = _drive_tick(monkeypatch, tmp_path / "plain", LEGION,
                            pad=PAD_FITS_ONLY_WITHOUT_FOOTER)
    assert len(plain) == 1 and len(plain[0]) <= 4096


def test_proven_bundle_failure_releases_every_member_reservation(monkeypatch, tmp_path):
    with pytest.MonkeyPatch.context() as patch:
        singles, *_ = _drive_tick(patch, tmp_path / "singles", LEGION, bundle="0",
                                  fingerprint=True)
    assert len(singles) == 3
    held_at_send = []

    def proven_failure(text):
        held_at_send.append([bool(C._signal_fingerprint_already_sent(LEGION["match_key"], t))
                             for t in singles])
        raise C.TelegramSendError("rejected by Telegram", delivery_uncertain=False)

    work = tmp_path / "work"
    sent, bets, ml_keys, journal = _drive_tick(
        monkeypatch, work, LEGION, fingerprint=True, on_send=proven_failure,
        expect_journal=False)
    assert held_at_send == [[True, True, True]], "primary AND members are reserved while sending"
    assert sent == [] and bets == [] and ml_keys == [] and journal is None
    assert [C._signal_fingerprint_already_sent(LEGION["match_key"], t) for t in singles] == [None] * 3
    assert not json.loads((work / "fingerprints.json").read_text()), "durable registry released too"
    sent2, bets2, ml_keys2, _ = _drive_tick(monkeypatch, work, LEGION, fingerprint=True)
    assert len(sent2) == 1, "a later tick sends the whole bundle again"
    _assert_bundle(sent2[0], LEGION, _decisions(LEGION))
    _assert_ledgers(LEGION, bets2, ml_keys2, _decisions(LEGION))


def test_odds_gate_active_never_bundles_the_win(monkeypatch):
    """With the odds gate active the win goes alone through the single path."""
    calls = []

    def fake_deliver(decision, *, bundle_decisions=(), bundle_outcome=None, **kwargs):
        calls.append((decision.market, [d.market for d in bundle_decisions]))
        return {"market": decision.market, "target_side": decision.target_side,
                "rule": decision.rule, "status": "delivered"}

    def no_precheck(*args, **kwargs):
        raise AssertionError("the group code must never read the win floor itself")

    monkeypatch.setattr(C, "_ml_dispatch_deliver_decision", fake_deliver)
    monkeypatch.setattr(C, "_ml_dispatch_min_odds_reject_for_delivery", no_precheck)
    monkeypatch.setattr(C, "_signal_fingerprint_already_sent", lambda *a: None)
    monkeypatch.setattr(C, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(C, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    kwargs = {k: None for k in C._ML_DISPATCH_BUNDLE_BUILD_KEYS}
    kwargs.update(radiant_team_name=LEGION["teams"]["radiant"],
                  dire_team_name=LEGION["teams"]["dire"], team_elo_block="",
                  ml_laning_line="", all_model_line="", full_message_text=_panel_body(LEGION),
                  base_url=LEGION["base_url"], ctx_map_num=LEGION["map_num"], ledger=None)
    (group,) = C._ml_dispatch_bundle_groups(_decisions(LEGION))
    views = C._ml_dispatch_deliver_group(
        group, match_key=LEGION["match_key"],
        resolved_map_num=LEGION["map_num"], **kwargs)
    assert calls == [("win", []), ("kills_window", ["kills_total"])], calls
    assert len(views) == 3


def test_oversized_bundle_falls_back_to_singles(monkeypatch, tmp_path):
    sent, bets, ml_keys, _ = _drive_tick(monkeypatch, tmp_path, LEGION, pad=4500)
    assert len(sent) == 3, [len(t) for t in sent]
    assert all("Ставки на" not in t for t in sent)
    _assert_ledgers(LEGION, bets, ml_keys, _decisions(LEGION))


def test_decision_without_a_valid_side_is_never_bundled(monkeypatch, tmp_path):
    decisions = [dataclasses.replace(d, target_side=None) if d.market != "win" else d
                 for d in _decisions(LEGION)]
    groups = C._ml_dispatch_bundle_groups(decisions)
    assert sorted(tuple(d.market for d in g) for g in groups) == [
        ("kills_total",), ("kills_window",), ("win",)]
    sent, *_ = _drive_tick(monkeypatch, tmp_path, LEGION, decisions=decisions)
    assert all("Ставки на" not in t for t in sent), sent


def test_same_side_different_teams_never_share_a_group():
    decisions = _decisions(LEGION)
    decisions[2] = dataclasses.replace(decisions[2], target_team="Some Other Team")
    groups = C._ml_dispatch_bundle_groups(decisions)
    assert sorted(len(g) for g in groups) == [1, 2]


def _golden(case):
    texts = json.loads(GOLDEN_PATH.read_text())[case]
    assert texts and all(texts), "golden must contain confirmed, non-empty sends"
    return texts


def test_opposite_sides_keep_two_single_messages(monkeypatch, tmp_path):
    sent, bets, ml_keys, _ = _drive_tick(monkeypatch, tmp_path, OPPOSITE)
    assert len(sent) == 2
    assert sent == _golden("opposite_sides")
    _assert_ledgers(OPPOSITE, bets, ml_keys, _decisions(OPPOSITE))


def test_bundle_disabled_keeps_three_single_messages(monkeypatch, tmp_path):
    sent, bets, ml_keys, _ = _drive_tick(monkeypatch, tmp_path, SHADOW, bundle="0")
    assert len(sent) == 3
    assert sent == _golden("bundle_disabled")
    _assert_ledgers(SHADOW, bets, ml_keys, _decisions(SHADOW))


def test_single_decision_keeps_single_message(monkeypatch, tmp_path):
    decisions = _decisions(SHADOW)[:1]
    sent, bets, ml_keys, _ = _drive_tick(monkeypatch, tmp_path, SHADOW, decisions=decisions)
    assert len(sent) == 1
    assert sent == _golden("single_decision")
    _assert_ledgers(SHADOW, bets, ml_keys, decisions)


def _write_golden(tmp_path):
    """Explicit baseline capture command only; pytest never regenerates goldens."""
    captures = {}
    for case, row, options, count in (
        ("opposite_sides", OPPOSITE, {}, 2),
        ("bundle_disabled", SHADOW, {"bundle": "0"}, 3),
        ("single_decision", SHADOW, {"decisions": _decisions(SHADOW)[:1]}, 1),
    ):
        with pytest.MonkeyPatch.context() as patch:
            sent, bets, keys, _ = _drive_tick(patch, tmp_path / case, row, **options)
            assert len(sent) == count and all(sent), (case, sent)
            _assert_ledgers(row, bets, keys, options.get("decisions", _decisions(row)))
            captures[case] = sent
    GOLDEN_PATH.write_text(json.dumps(captures, ensure_ascii=False, indent=2) + "\n")

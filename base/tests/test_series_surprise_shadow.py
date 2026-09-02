"""Проверки теневого учёта сюрприза серии и разбора истории у Stratz.

Стережётся здесь прежде всего ОРИЕНТАЦИЯ. Признак описывает радианта ТЕКУЩЕЙ
карты, а стороны между картами серии меняются. В первой версии признак считался
для команды, за которую голосует модель, и знак относительно цели переворачивался
на картах с выбором в пользу дайра: веса выходили разного знака на двух
источниках вероятности. Ловит это `test_orientation_flips_with_sides`.

Второе стерегомое место — связка «наш вердикт ↔ карта Stratz». Она идёт ПО
ВРЕМЕНИ, и окно нельзя расширять: между картами серии по корпусу медиана 25
минут, так что широкое окно склеит соседние карты.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import series_surprise_shadow as sss  # noqa: E402
from series_surprise_shadow import observe, surprise_from_maps  # noqa: E402
import stratz_map_result as smr  # noqa: E402

A, B = 111, 222          # команды
T0 = 1_700_000_000       # старт первой карты
FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _store(tmp_path: Path) -> Path:
    return tmp_path / "series_surprise_shadow.json"


def _map(mid, start, rad, dire, rad_won, series=7):
    return {"match_id": mid, "series_id": series, "start": start,
            "end": start + 2100, "radiant_team_id": rad,
            "dire_team_id": dire, "radiant_won": rad_won}


def test_first_map_has_no_history(tmp_path: Path) -> None:
    out = observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
                  p_radiant=0.9, store_path=_store(tmp_path), now=T0,
                  history_lookup=lambda r, d, n: [])
    assert out == {"s_sum": 0.0, "s_last": 0.0, "n_prev": 0.0}


def test_favourite_lost_gives_negative_surprise(tmp_path: Path) -> None:
    st = _store(tmp_path)
    observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
            p_radiant=0.9, store_path=st, now=T0, history_lookup=lambda *a: [])
    hist = [_map(1, T0, A, B, False)]          # радиант A с 0.9 проиграл
    out = observe(series_key="s", map_key="m2", radiant_team_id=A, dire_team_id=B,
                  p_radiant=0.85, store_path=st, now=T0 + 3000,
                  history_lookup=lambda *a: hist)
    assert out["n_prev"] == 1.0
    assert out["s_sum"] == pytest.approx(-0.9)


def test_orientation_flips_with_sides(tmp_path: Path) -> None:
    """На второй карте команды поменялись сторонами — знак обязан зеркалиться."""
    st = _store(tmp_path)
    observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
            p_radiant=0.9, store_path=st, now=T0, history_lookup=lambda *a: [])
    hist = [_map(1, T0, A, B, False)]
    out = observe(series_key="s", map_key="m2", radiant_team_id=B, dire_team_id=A,
                  p_radiant=0.5, store_path=st, now=T0 + 3000,
                  history_lookup=lambda *a: hist)
    assert out["s_sum"] == pytest.approx(+0.9)


def test_three_maps_accumulate_real_series(tmp_path: Path) -> None:
    """Серия 1132426 Nigma — Team Yandex, три карты, боевые числа 22.08.2026.

    Карта 1: радиант Yandex, вердикт «радиант 70.4%», победил дайр (Nigma).
    Карта 2: радиант Nigma, вердикт «дайр 76.2%», победил дайр.
    Карта 3: радиант Nigma — для неё и считаем сюрприз: +0.704 − 0.238 = +0.466.
    """
    NIGMA, YAN = 10136357, 9823272
    st = _store(tmp_path)
    observe(series_key="s", map_key="m1", radiant_team_id=YAN, dire_team_id=NIGMA,
            p_radiant=0.704, store_path=st, now=T0, history_lookup=lambda *a: [])
    observe(series_key="s", map_key="m2", radiant_team_id=NIGMA, dire_team_id=YAN,
            p_radiant=0.238, store_path=st, now=T0 + 5000, history_lookup=lambda *a: [])
    hist = [_map(1, T0, YAN, NIGMA, False), _map(2, T0 + 5000, NIGMA, YAN, False)]
    out = observe(series_key="s", map_key="m3", radiant_team_id=NIGMA, dire_team_id=YAN,
                  p_radiant=0.340, store_path=st, now=T0 + 10000,
                  history_lookup=lambda *a: hist)
    assert out["n_prev"] == 2.0
    assert out["s_sum"] == pytest.approx(0.466, abs=1e-6)
    assert out["s_last"] == pytest.approx(-0.238)


def test_map_without_our_verdict_is_skipped(tmp_path: Path) -> None:
    """Без своей вероятности сюрприз не считается — 0.5 не подставляется."""
    st = _store(tmp_path)
    hist = [_map(1, T0, A, B, False)]
    out = observe(series_key="s", map_key="m2", radiant_team_id=A, dire_team_id=B,
                  p_radiant=0.85, store_path=st, now=T0 + 3000,
                  history_lookup=lambda *a: hist)
    assert out["n_prev"] == 0.0


def test_verdict_too_far_from_start_does_not_link(tmp_path: Path) -> None:
    """Окно связки узкое: иначе к карте прицепится вердикт соседней."""
    st = _store(tmp_path)
    observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
            p_radiant=0.9, store_path=st, now=T0 - 3600, history_lookup=lambda *a: [])
    hist = [_map(1, T0, A, B, False)]
    out = observe(series_key="s", map_key="m2", radiant_team_id=A, dire_team_id=B,
                  p_radiant=0.85, store_path=st, now=T0 + 3000,
                  history_lookup=lambda *a: hist)
    assert out["n_prev"] == 0.0


def test_repeated_verdicts_do_not_duplicate(tmp_path: Path) -> None:
    st = _store(tmp_path)
    for t in (T0, T0 + 30, T0 + 60):
        observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
                p_radiant=0.9, store_path=st, now=t, history_lookup=lambda *a: [])
    hist = [_map(1, T0, A, B, False)]
    out = observe(series_key="s", map_key="m2", radiant_team_id=A, dire_team_id=B,
                  p_radiant=0.85, store_path=st, now=T0 + 3000,
                  history_lookup=lambda *a: hist)
    assert out["n_prev"] == 1.0


def test_history_lookup_failure_is_silent(tmp_path: Path) -> None:
    def boom(*a):
        raise RuntimeError("сеть")
    out = observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
                  p_radiant=0.9, store_path=_store(tmp_path), now=T0,
                  history_lookup=boom)
    assert out["n_prev"] == 0.0


def test_store_survives_corrupted_file(tmp_path: Path) -> None:
    st = _store(tmp_path)
    st.write_text("не json", encoding="utf-8")
    out = observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
                  p_radiant=0.7, store_path=st, now=T0, history_lookup=lambda *a: [])
    assert out["n_prev"] == 0.0


# ---------- разбор истории у Stratz ----------

def _raw(mid, series, start, rad, dire, won):
    return {"id": mid, "seriesId": series, "startDateTime": start,
            "endDateTime": start + 2100, "durationSeconds": 2100,
            "radiantTeamId": rad, "direTeamId": dire, "didRadiantWin": won}


def test_series_history_takes_only_current_series(tmp_path: Path) -> None:
    """Берётся серия последней встречи, прошлая встреча тех же команд не липнет."""
    raw = [_raw(10, 500, T0 - 90000, A, B, True),      # вчерашняя серия
           _raw(11, 600, T0, A, B, False),
           _raw(12, 600, T0 + 5000, B, A, True)]
    got = smr.series_history(A, B, before=T0 + 9000, now=T0 + 9000,
                             cache_path=tmp_path / "c.json",
                             query=lambda tid, since: raw)
    assert [m["match_id"] for m in got] == [11, 12]


def test_series_history_drops_future_maps(tmp_path: Path) -> None:
    """Карту, которая ещё не кончилась к моменту решения, брать нельзя."""
    raw = [_raw(11, 600, T0, A, B, False), _raw(12, 600, T0 + 5000, B, A, True)]
    got = smr.series_history(A, B, before=T0 + 3000, now=T0 + 3000,
                             cache_path=tmp_path / "c.json",
                             query=lambda tid, since: raw)
    assert [m["match_id"] for m in got] == [11]


def test_team_matches_skips_unfinished(tmp_path: Path) -> None:
    live = {"id": 99, "seriesId": 600, "startDateTime": T0, "endDateTime": None,
            "durationSeconds": 0, "radiantTeamId": A, "direTeamId": B,
            "didRadiantWin": None}
    got = smr.team_matches(A, now=T0, cache_path=tmp_path / "c.json",
                           query=lambda tid, since: [live])
    assert got == []


def test_team_matches_network_failure_returns_empty(tmp_path: Path) -> None:
    got = smr.team_matches(A, now=T0, cache_path=tmp_path / "c.json",
                           query=lambda tid, since: None)
    assert got == []


# ---------- фоновый прогрев ----------

def test_recent_team_ids_from_store(tmp_path: Path) -> None:
    from series_surprise_shadow import recent_team_ids
    st = _store(tmp_path)
    observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
            p_radiant=0.7, store_path=st, now=T0, history_lookup=lambda *a: [])
    observe(series_key="s", map_key="m2", radiant_team_id=333, dire_team_id=444,
            p_radiant=0.7, store_path=st, now=T0 - 10 * 3600, history_lookup=lambda *a: [])
    got = recent_team_ids(store_path=st, now=T0)
    assert got == [A, B], "старый вердикт греть незачем"


def test_refresh_reports_newly_appeared_map(tmp_path: Path) -> None:
    """Прогрев обязан сообщать о появлении карты — это и есть замер задержки."""
    seen = []
    raw = [_raw(11, 600, T0 - 1800, A, B, False)]
    n = smr.refresh([A], cache_path=tmp_path / "c.json", now=T0,
                    query=lambda tid, since: raw,
                    on_new=lambda tid, m, delay: seen.append((tid, m["match_id"], delay)))
    assert n == 1 and seen and seen[0][1] == 11
    # повторный прогон той же карты новой не считает
    n2 = smr.refresh([A], cache_path=tmp_path / "c.json", now=T0 + 30,
                     query=lambda tid, since: raw, on_new=lambda *a: seen.append(a))
    assert n2 == 0


def test_refresh_survives_network_failure(tmp_path: Path) -> None:
    n = smr.refresh([A], cache_path=tmp_path / "c.json", now=T0,
                    query=lambda tid, since: None)
    assert n == 0


def test_teams_from_elo_progress(tmp_path: Path) -> None:
    """Живые карты берутся из прогресса ELO — он пишется на каждой карте."""
    import json as _json
    from series_surprise_shadow import teams_from_elo_progress, teams_to_warm
    prog = tmp_path / "live_elo_progress.json"
    prog.write_text(_json.dumps({"pending_series": {
        "s1": {"updated_at": T0, "pending_map": {"match_record": {
            "radiant_team_id": 777, "dire_team_id": 888}}},
        "old": {"updated_at": T0 - 10 * 3600, "pending_map": {"match_record": {
            "radiant_team_id": 999, "dire_team_id": 1000}}},
    }}), encoding="utf-8")
    assert teams_from_elo_progress(path=prog, now=T0) == [777, 888]
    st = _store(tmp_path)
    observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
            p_radiant=0.7, store_path=st, now=T0, history_lookup=lambda *a: [])
    assert teams_to_warm(store_path=st, elo_progress_path=prog, now=T0) == [A, B, 777, 888]


def test_teams_from_elo_progress_missing_file(tmp_path: Path) -> None:
    from series_surprise_shadow import teams_from_elo_progress
    assert teams_from_elo_progress(path=tmp_path / "нет.json", now=T0) == []


# ---------- подмена team_id проводом ----------

def test_prod_team_id_is_translated_to_feed_id(tmp_path: Path) -> None:
    """Прод подменяет id команды словарным, а Stratz знает только тот, что дал фид.

    Боевой случай 22.08.2026: фид отдал BoomBoys 8255888 и Nigma 10136357, прод
    заменил их на 10163435 и 7554697 (`TEAM_ID_NAME_MISMATCH`), и по подменённым
    Stratz возвращает НОЛЬ матчей — справка не могла сработать никогда.
    """
    cache = tmp_path / "c.json"
    seen = []

    def query(tid, since):
        seen.append(tid)
        return [_raw(11, 600, T0, 8255888, 10136357, True)] if tid == 8255888 else []

    smr.note_team_alias(10163435, 8255888, cache_path=cache)
    got = smr.team_matches(10163435, now=T0 + 10, cache_path=cache, query=query)
    assert seen == [8255888], "спрашивать надо id фида, а не подменённый"
    assert [m["match_id"] for m in got] == [11]


def test_alias_is_not_invented_when_absent(tmp_path: Path) -> None:
    """Без записанной связки id остаётся как есть — гадать нельзя."""
    seen = []
    smr.team_matches(555, now=T0, cache_path=tmp_path / "c.json",
                     query=lambda tid, since: seen.append(tid) or [])
    assert seen == [555]


def test_alias_survives_reload(tmp_path: Path) -> None:
    """Связка переживает перезапуск: она на диске, а не в памяти процесса."""
    cache = tmp_path / "c.json"
    smr.note_team_alias(10163435, 8255888, cache_path=cache)
    assert smr.resolve_team_id(10163435, cache_path=cache) == 8255888
    assert smr.resolve_team_id(999, cache_path=cache) == 999


# ---------- кэш «последний сюрприз пары» (для синхронного чтения без сети) ----
#
# `last_surprise` читает то, что `observe()` уже посчитал через `history_lookup`
# (в бою — сеть Stratz, недопустимая в пути ставки) на ПРЕДЫДУЩЕЙ оценке той же
# пары команд. Фикстура `fixtures/series_surprise_shadow_serv1_20260901.json` —
# 14 настоящих вердиктов, снятых `scp serv1:/root/main/runtime/series_surprise_shadow.json`
# 01.09.2026; формат ДО поля `pairs`, проверяет обратную совместимость загрузчика.

def test_captured_store_loads_without_pairs_key(tmp_path: Path) -> None:
    """Старый снимок прода (без `pairs`) читается, кэша сюрприза в нём просто нет."""
    import shutil
    st = tmp_path / "series_surprise_shadow.json"
    shutil.copy(FIXTURES / "series_surprise_shadow_serv1_20260901.json", st)
    assert sss.last_surprise(9600141, 10164236, store_path=st, now=1788168312) is None
    got = sss.recent_team_ids(store_path=st, now=1788280974)
    assert 10212329 in got and 9256405 in got


def test_observe_persists_pair_surprise_for_sync_read(tmp_path: Path) -> None:
    """`observe()` кладёт посчитанный сюрприз в стор — `last_surprise` берёт его."""
    st = tmp_path / "series_surprise_shadow.json"
    observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
            p_radiant=0.7, store_path=st, now=T0, history_lookup=lambda *a: [])
    hist = [_map(1, T0, A, B, True)]         # радиант A с 0.7 победил
    observe(series_key="s", map_key="m2", radiant_team_id=A, dire_team_id=B,
            p_radiant=0.6, store_path=st, now=T0 + 3000, history_lookup=lambda *a: hist)
    got = sss.last_surprise(A, B, store_path=st, now=T0 + 3000)
    assert got is not None
    assert got["n_prev"] == 1.0
    assert got["s_sum"] == pytest.approx(0.3)        # 1.0 − 0.7


def test_last_surprise_flips_sign_when_sides_swap(tmp_path: Path) -> None:
    """Ориентация читается ПОД ЗАПРОС: та же карта, команды поменялись сторонами."""
    st = tmp_path / "series_surprise_shadow.json"
    observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
            p_radiant=0.7, store_path=st, now=T0, history_lookup=lambda *a: [])
    hist = [_map(1, T0, A, B, True)]
    observe(series_key="s", map_key="m2", radiant_team_id=A, dire_team_id=B,
            p_radiant=0.6, store_path=st, now=T0 + 3000, history_lookup=lambda *a: hist)
    # запрос с ДРУГИМ радиантом той же пары -> знак должен перевернуться
    got = sss.last_surprise(B, A, store_path=st, now=T0 + 3000)
    assert got["s_sum"] == pytest.approx(-0.3)


def test_last_surprise_none_for_unknown_pair(tmp_path: Path) -> None:
    st = tmp_path / "series_surprise_shadow.json"
    observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
            p_radiant=0.7, store_path=st, now=T0, history_lookup=lambda *a: [])
    assert sss.last_surprise(333, 444, store_path=st, now=T0) is None


def test_last_surprise_expires_with_age(tmp_path: Path) -> None:
    """Устаревшая запись (>36ч) не отдаётся — серия столько не длится."""
    st = tmp_path / "series_surprise_shadow.json"
    observe(series_key="s", map_key="m1", radiant_team_id=A, dire_team_id=B,
            p_radiant=0.7, store_path=st, now=T0, history_lookup=lambda *a: [])
    hist = [_map(1, T0, A, B, True)]
    observe(series_key="s", map_key="m2", radiant_team_id=A, dire_team_id=B,
            p_radiant=0.6, store_path=st, now=T0 + 3000, history_lookup=lambda *a: hist)
    assert sss.last_surprise(A, B, store_path=st, now=T0 + 3000) is not None
    stale_now = T0 + 3000 + sss.MAX_AGE_SECONDS + 1
    assert sss.last_surprise(A, B, store_path=st, now=stale_now) is None


def test_series_history_translates_both_sides(tmp_path: Path) -> None:
    """Перевод нужен НЕ ТОЛЬКО в запросе, но и в сравнении пары команд.

    Первая версия правки переводила лишь id для запроса: матчи приходили с
    идентификаторами Stratz, а фильтр пары сравнивал их с продовыми, и история
    выходила пустой. Ловится только тестом, где ОБЕ стороны заданы по-продовому.
    """
    cache = tmp_path / "c.json"
    smr.note_team_alias(10163435, 8255888, cache_path=cache)
    smr.note_team_alias(7554697, 10136357, cache_path=cache)
    raw = [_raw(1, 900, T0, 8255888, 10136357, True),
           _raw(2, 900, T0 + 4000, 10136357, 8255888, True)]
    got = smr.series_history(10163435, 7554697, before=T0 + 9000, now=T0 + 9000,
                             cache_path=cache, query=lambda tid, since: raw)
    assert [m["match_id"] for m in got] == [1, 2]

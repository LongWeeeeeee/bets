import importlib
import sys
import time
from pathlib import Path

import pytest
BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime
import sourcetv_probe as probe

from sourcetv_bridge import resolve_sourcetv_matches_path


def test_resolve_sourcetv_matches_path_ignores_cwd_for_overrides(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    project_root = tmp_path / "project"
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SOURCETV_MATCHES_PATH", raising=False)
    assert resolve_sourcetv_matches_path(project_root) == (
        project_root / "runtime" / "sourcetv_matches.json"
    ).resolve()

    absolute_override = tmp_path / "outside" / "matches.json"
    monkeypatch.setenv("SOURCETV_MATCHES_PATH", str(absolute_override))
    assert resolve_sourcetv_matches_path(project_root) == absolute_override.resolve()

    monkeypatch.setenv("SOURCETV_MATCHES_PATH", "bridge/matches.json")
    assert resolve_sourcetv_matches_path(project_root) == (
        project_root / "bridge" / "matches.json"
    ).resolve()

def test_sourcetv_default_paths_share_repo_root_runtime() -> None:
    expected = Path(runtime.PROJECT_ROOT) / "runtime" / "sourcetv_matches.json"
    assert Path(runtime.SOURCETV_MATCHES_PATH) == expected
    assert Path(probe.SOURCETV_MATCHES_PATH) == expected


def test_sourcetv_bridge_timestamp_tracks_real_progress_not_rewrites() -> None:
    state = {"game": None, "last_seen": 0.0, "last_progress_at": 0.0}
    first = {
        "game_time": 1800,
        "radiant_score": 20,
        "dire_score": 18,
        "radiant_lead": 1500,
    }
    probe._note_sourcetv_snapshot(state, first, now=100.0)
    state["game"] = first
    assert probe._sourcetv_snapshot_timestamp(state, now=100.0) == 100.0

    # An identical finished row can be received forever, but its exported
    # freshness must stay at the last real game change.
    probe._note_sourcetv_snapshot(state, dict(first), now=500.0)
    assert state["last_seen"] == 500.0
    assert probe._sourcetv_snapshot_timestamp(state, now=500.0) == 100.0

    progressed = dict(first, game_time=1801)
    probe._note_sourcetv_snapshot(state, progressed, now=501.0)
    state["game"] = progressed
    assert probe._sourcetv_snapshot_timestamp(state, now=501.0) == 501.0


def test_live_list_keeps_bridge_record_fresh_while_gc_is_silent() -> None:
    """Замерший GC при живом матче не имеет права состарить запись.

    Регрессия 16.08.2026 (LGD Gaming — Team Yandex, карта 3): ретрансляция
    молчала с 51:47 до 57:23, запись протухла по last_progress_at, probe сам
    вычистил живую карту из моста, и потребитель прочитал пустой файл как
    доказанный конец карты.
    """
    state = {"game": None, "last_seen": 0.0, "last_progress_at": 0.0}
    row = {
        "game_time": 3107,
        "radiant_score": 24,
        "dire_score": 23,
        "radiant_lead": 18553,
    }
    probe._note_sourcetv_snapshot(state, row, now=100.0)
    state["game"] = row

    # GC молчит пять с половиной минут, но live-список Valve держит матч.
    state["last_api_seen"] = 420.0
    assert probe._sourcetv_snapshot_timestamp(state, now=430.0) == 420.0


def test_ghost_match_without_live_list_confirmation_still_ages() -> None:
    """Матч-призрак исчезает из live-списка — запись стареет ровно как раньше."""
    state = {"game": None, "last_seen": 0.0, "last_progress_at": 0.0}
    row = {
        "game_time": 4092,
        "radiant_score": 26,
        "dire_score": 40,
        "radiant_lead": 80583,
    }
    probe._note_sourcetv_snapshot(state, row, now=100.0)
    state["game"] = row
    state["last_api_seen"] = 100.0

    # Valve продолжает отдавать ту же законченную строку по GC.
    probe._note_sourcetv_snapshot(state, dict(row), now=600.0)
    assert state["last_seen"] == 600.0
    assert probe._sourcetv_snapshot_timestamp(state, now=600.0) == 100.0


class _LogSink:
    def __init__(self) -> None:
        self.warnings: list = []
        self.infos: list = []

    def warning(self, *args) -> None:
        self.warnings.append(args)

    def info(self, *args) -> None:
        self.infos.append(args)


def test_gc_stall_is_reported_once_per_episode() -> None:
    """Молчание ретранслятора видно в логе, но не заливает его каждую секунду."""
    sink = _LogSink()
    state = {"last_progress_at": 100.0, "last_api_seen": 400.0, "gc_stall_logged": False}

    assert probe._note_gc_stall(1, state, now=150.0, logger=sink) is False
    assert probe._note_gc_stall(1, state, now=400.0, logger=sink) is True
    assert probe._note_gc_stall(1, state, now=402.0, logger=sink) is False
    assert len(sink.warnings) == 1

    state["last_progress_at"] = 410.0
    assert probe._note_gc_stall(1, state, now=411.0, logger=sink) is True
    assert len(sink.infos) == 1


def test_idle_probe_still_beats_so_watchdog_sees_a_live_process() -> None:
    """Пауза между матчами — не залипание: лог обязан обновляться.

    Watchdog судит о жизни probe по mtime лога (порог 900 с), а тихий probe в
    паузе не печатал ничего и перезапускался на ровном месте (16.08.2026,
    рестарты в 15:15 и 15:35 при живом процессе).
    """
    assert probe.HEARTBEAT_SECONDS < 900.0
    assert probe._heartbeat_due(0, 0.0, now=probe.HEARTBEAT_SECONDS) is True
    assert probe._heartbeat_due(0, 0.0, now=probe.HEARTBEAT_SECONDS - 1) is False
    # Пока матчи на экране, лог и так пишется каждой итерацией.
    assert probe._heartbeat_due(2, 0.0, now=probe.HEARTBEAT_SECONDS * 10) is False


def test_sourcetv_pregame_timestamp_uses_latest_receipt() -> None:
    state = {
        "game": {"game_time": -45},
        "last_seen": 700.0,
        "last_progress_at": 100.0,
    }
    assert probe._sourcetv_snapshot_timestamp(state, now=701.0) == 700.0


def test_sourcetv_module_paths_anchor_relative_override_to_repo_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    expected = (Path(runtime.PROJECT_ROOT) / "bridge" / "matches.json").resolve()
    monkeypatch.setenv("SOURCETV_MATCHES_PATH", "bridge/matches.json")
    for cwd in (tmp_path, tmp_path / "other"):
        cwd.mkdir(exist_ok=True)
        monkeypatch.chdir(cwd)
        assert Path(importlib.reload(runtime).SOURCETV_MATCHES_PATH) == expected
        assert Path(importlib.reload(probe).SOURCETV_MATCHES_PATH) == expected

    monkeypatch.delenv("SOURCETV_MATCHES_PATH")
    importlib.reload(runtime)
    importlib.reload(probe)


def test_stake_multiplier_requires_complete_late_core_coverage() -> None:
    common = dict(
        team_elo_meta=None,
        target_side="radiant",
        selected_early_sign=1,
        selected_late_sign=1,
        has_selected_early_star=True,
        has_selected_late_star=True,
        early_wr_pct=70.0,
        late_wr_pct=65.0,
        game_time_seconds=30 * 60,
        radiant_lead=2000.0,
        late_star_hit_count=3,
        early_star_hit_count=2,
    )
    assert runtime._stake_multiplier_for_signal(
        late_star_hit_metrics=["counterpick_1vs1", "solo"], **common
    ) == 0.5
    assert runtime._stake_multiplier_for_signal(
        late_star_hit_metrics=["counterpick_1vs1", "counterpick_1vs2", "solo"], **common
    ) != 0.5


def test_live_list_alone_keeps_match_under_watch_while_gc_is_dead() -> None:
    """Матч в live-списке не снимается, даже когда ретрансляция мертва.

    22.08.2026, карта 2 серии BoomBoys — Team Spirit (`8959362208`): GC отдал
    один пустой снимок и замолчал. Пока Valve подтверждал матч в live-списке,
    снимать его нельзя — иначе конец карты объявляется по молчанию одного из
    двух каналов.
    """
    state = {"created_at": 5.0, "last_seen": 10.0, "last_api_seen": 900.0}
    assert probe._match_drop_reason(state, in_live_list=True, now=1000.0) is None
    # Тот же матч, но live-список его больше не подтверждает: держим ещё grace.
    assert probe._match_drop_reason(state, in_live_list=False, now=1000.0) is None
    assert probe._match_drop_reason(
        state, in_live_list=False,
        now=900.0 + probe.DROP_GRACE_SECONDS + 1) is not None


def test_live_list_flicker_does_not_drop_a_match_with_silent_gc() -> None:
    """Мигание live-списка при замершем GC не снимает карту.

    Прежнее правило смотрело только на ответы GC: одного рефетча без матча
    хватало, чтобы удалить запись, если ретранслятор молчал больше пяти минут.
    """
    state = {"created_at": 5.0, "last_seen": 100.0, "last_api_seen": 640.0}
    assert probe._match_drop_reason(state, in_live_list=False, now=700.0) is None


def test_ghost_match_is_dropped_by_the_lifetime_cap() -> None:
    """Призрак, который GC отдаёт бесконечно, снимается по потолку.

    `last_seen` обновляется каждым ответом GC, поэтому условие устаревания у
    такой записи не выполняется никогда.
    """
    born = 1_000.0
    state = {"created_at": born, "last_seen": 1e9, "last_api_seen": 1e9}
    assert probe._match_drop_reason(
        state, in_live_list=True, now=born + probe.MAX_MATCH_LIFETIME - 1) is None
    assert probe._match_drop_reason(
        state, in_live_list=True, now=born + probe.MAX_MATCH_LIFETIME + 1) is not None


def test_drop_report_flags_a_map_we_never_got_data_for() -> None:
    """Снятие карты без данных обязано быть заметно в логе, а не выглядеть как конец."""
    lost = {"created_at": 1_000.0, "progress_count": 1, "max_game_time": 9}
    level, text = probe._drop_report(8959362208, lost, "исчез из live-списка", now=2_800.0)
    assert level == "warning"
    assert "ПОТЕРЯНА" in text

    played = {"created_at": 1_000.0, "progress_count": 340, "max_game_time": 2128}
    level, text = probe._drop_report(8959222564, played, "исчез из live-списка", now=4_600.0)
    assert level == "info"
    assert "ПОТЕРЯНА" not in text
    assert "35:28" in text


def test_snapshot_counts_only_real_progress() -> None:
    """Повтор одной и той же строки не считается обновлением."""
    state = {}
    row = {"game_time": 9, "radiant_score": 0, "dire_score": 0, "radiant_lead": 0}
    probe._note_sourcetv_snapshot(state, row, now=100.0)
    for tick in range(101, 400):
        probe._note_sourcetv_snapshot(state, dict(row), now=float(tick))
    assert state["progress_count"] == 1
    assert state["max_game_time"] == 9

    probe._note_sourcetv_snapshot(
        state, {"game_time": 600, "radiant_score": 5, "dire_score": 3,
                "radiant_lead": 2000}, now=500.0)
    assert state["progress_count"] == 2
    assert state["max_game_time"] == 600


def test_status_line_marks_an_echo_of_a_dead_snapshot() -> None:
    """Строка статуса печатается из последнего снимка — её возраст должен быть виден."""
    state = {"last_seen": 100.0}
    assert probe._snapshot_age_mark(state, now=100.0 + probe.GC_STALL_WARN_SECONDS - 1) == ""
    mark = probe._snapshot_age_mark(state, now=100.0 + 900.0)
    assert "GC молчит" in mark and "15" in mark


def test_gc_stall_reminder_repeats_while_silence_lasts() -> None:
    """Долгое молчание напоминает о себе, а не тонет в одной строке."""
    sink = _LogSink()
    state = {"last_progress_at": 100.0, "last_api_seen": 400.0, "gc_stall_logged": False}
    assert probe._note_gc_stall(1, state, now=300.0, logger=sink) is True
    assert probe._note_gc_stall(1, state, now=310.0, logger=sink) is False
    assert probe._note_gc_stall(
        1, state, now=300.0 + probe.GC_STALL_REPEAT_SECONDS + 1, logger=sink) is True
    assert len(sink.warnings) == 2


def test_probe_follows_a_recreated_lobby() -> None:
    """Смена lobby_id живого матча переключает запрос к GC, а не теряет карту."""
    target = {"lobby_id": 111, "rad": "A", "dire": "B"}
    assert probe._adopt_new_lobby(target, {"lobby_id": 111}) is None
    assert probe._adopt_new_lobby(target, {}) is None
    assert probe._adopt_new_lobby(target, {"lobby_id": 222}) == (111, 222)
    assert target["lobby_id"] == 222
    # Прочие поля цели правка не трогает.
    assert target["rad"] == "A" and target["dire"] == "B"


def test_dead_broadcast_is_reported_while_the_map_is_still_running() -> None:
    """Несостоявшаяся ретрансляция должна быть видна сразу, а не при снятии записи.

    22.08.2026, карта `8959362208`: GC отдал один снимок с `game_time` 9 c и
    нулём зрителей и замолчал. В логе это выглядело как живой матч ещё
    пятнадцать минут, а игра тем временем шла своим ходом ещё полчаса.
    """
    sink = _LogSink()
    born = 1_000.0
    state = {"created_at": born, "progress_count": 1, "max_game_time": 9}
    # Свежая цель: молчание в первые минуты — нормальная задержка старта.
    assert probe._note_dead_broadcast(1, state, now=born + 60.0, logger=sink) is False
    assert probe._note_dead_broadcast(
        1, state, now=born + probe.DEAD_BROADCAST_AFTER + 1, logger=sink) is True
    # Второй раз об одном и том же не сообщаем.
    assert probe._note_dead_broadcast(1, state, now=born + 5_000.0, logger=sink) is False
    assert len(sink.warnings) == 1

    # Карта, по которой поток идёт, тревоги не вызывает.
    alive = {"created_at": born, "progress_count": 84, "max_game_time": 1_200}
    assert probe._note_dead_broadcast(2, alive, now=born + 5_000.0, logger=sink) is False
    assert len(sink.warnings) == 1


def test_unknown_league_is_rejected_blind_not_by_name() -> None:
    """Отказ по пустому имени и отказ по чужому названию — разные события.

    26.08.2026, вопрос alex: «не сработал allowlist для BLAST Slam, Qualifier,
    хотя blast в allowlist точно есть». Сверяется не то название, которое видно
    на сайте, а имя из справочника OpenDota по `league_id`; у свежего тикета его
    нет вовсе, и allowlist отказывает вслепую — слово в списке есть, сравнивать
    не с чем.
    """
    assert probe._league_admission(
        20142, "RES Unchained - A Blast Dota Slam VIII Qualifier EU") == "ok"
    assert probe._league_admission(20142, "") == "no_name"
    assert probe._league_admission(20142, "   ") == "no_name"
    assert probe._league_admission(19850, "KUZYA X ISLAM X AYATO CUP 6.3") == "not_allowed"
    # Явный id-allowlist остаётся сильнее отсутствующего имени.
    assert probe._league_admission(19722, "") == "ok"


def test_rejected_league_is_written_to_log_once_per_hour() -> None:
    """Отброшенная лига обязана читаться из лога, но не заливать его.

    Возвращается число игр с прошлой записи: у платформенного тикета (10877
    Challengermode) на одном id живут и наш квал, и чужие ежедневки, и «сколько
    игр стоит этот отказ» — единственный способ решить, впускать ли его.
    """
    seen: dict = {}
    assert probe._note_rejected_league(seen, 10877, "Challengermode", now=1_000.0) == 1
    assert probe._note_rejected_league(seen, 10877, "Challengermode", now=1_100.0) == 0
    assert probe._note_rejected_league(seen, 10877, "Challengermode", now=1_200.0) == 0
    # Молчание не теряет игры: они доезжают в следующей записи.
    assert probe._note_rejected_league(
        seen, 10877, "Challengermode",
        now=1_000.0 + probe.KW_REJECT_LOG_REPEAT + 1) == 3
    assert seen[10877]["hits"] == 4
    # Другая лига — своя запись, а не хвост чужого окна.
    assert probe._note_rejected_league(seen, 19850, "KUZYA CUP", now=1_100.0) == 1
    assert seen[19850]["name"] == "KUZYA CUP"


def test_heartbeat_tells_silence_apart_from_a_filtered_out_league() -> None:
    """«Никто не играет» и «играют, но всё отброшено» снаружи выглядят одинаково."""
    seen: dict = {}
    probe._note_rejected_league(seen, 20142, "", now=1_000.0)
    probe._note_rejected_league(seen, 19850, "KUZYA CUP", now=1_010.0)
    summary = probe._rejected_leagues_summary(seen, now=1_020.0)
    assert "20142 <имени нет> x1" in summary
    assert "19850 KUZYA CUP x1" in summary
    # Протухшие отказы в пульс не попадают.
    assert probe._rejected_leagues_summary(
        seen, now=1_010.0 + probe.KW_REJECT_LOG_REPEAT + 1) == ""


def test_live_league_missing_from_directory_forces_one_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Живая лига без имени догоняется внеочередным обновлением справочника.

    Плановое обновление идёт раз в шесть часов, и всё это время свежий тикет
    невидим: у него пустое имя, а значит отказ allowlist'а.
    """
    calls = {"n": 0}

    def _fake_reload():
        calls["n"] += 1
        probe._LEAGUE_NAMES[20142] = "RES Unchained - A Blast Dota Slam VIII Qualifier EU"
        return True

    monkeypatch.setattr(probe, "_LEAGUE_NAMES", {19944: "EPL Masters 2026"})
    monkeypatch.setattr(probe, "_LEAGUE_NAMES_FETCHED_AT", time.time())
    monkeypatch.setattr(probe, "_LEAGUE_NAMES_LAST_MISS", 0.0)
    monkeypatch.setattr(probe, "_reload_league_names", _fake_reload)

    # Известная лига обновления не требует.
    assert probe.league_name(19944, refresh_if_missing=True) == "EPL Masters 2026"
    assert calls["n"] == 0
    # Неизвестная — требует, и имя доезжает сразу.
    assert probe.league_name(20142, refresh_if_missing=True).lower().startswith("res unchained")
    assert calls["n"] == 1
    # Повторный промах в окне тишины второго запроса не делает.
    monkeypatch.setattr(probe, "_LEAGUE_NAMES", {})
    assert probe.league_name(20143, refresh_if_missing=True) == ""
    assert calls["n"] == 1
    # Без флага плановый путь остаётся прежним.
    assert probe.league_name(20144) == ""
    assert calls["n"] == 1


def test_failed_directory_refresh_retries_in_minutes_not_hours(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Сбой OpenDota не должен оставлять старый справочник на шесть часов."""
    attempts = {"n": 0}

    def _boom(*_a, **_kw):
        attempts["n"] += 1
        raise TimeoutError("handshake operation timed out")

    monkeypatch.setattr(probe.urllib.request, "urlopen", _boom)
    monkeypatch.setattr(probe, "_LEAGUE_NAMES", {19944: "EPL Masters 2026"})
    monkeypatch.setattr(probe, "_LEAGUE_NAMES_FETCHED_AT", 0.0)

    assert probe.league_name(19944) == "EPL Masters 2026"
    assert attempts["n"] == 1
    age = time.time() - probe._LEAGUE_NAMES_FETCHED_AT
    expected = probe._LEAGUE_NAMES_TTL - probe._LEAGUE_NAMES_RETRY
    assert expected - 5 <= age <= expected + 5
    # В окне повтора второй попытки нет...
    assert probe.league_name(19944) == "EPL Masters 2026"
    assert attempts["n"] == 1
    # ...а после него — есть, и это минуты, а не часы.
    monkeypatch.setattr(
        probe, "_LEAGUE_NAMES_FETCHED_AT",
        time.time() - probe._LEAGUE_NAMES_TTL - 1,
    )
    assert probe.league_name(19944) == "EPL Masters 2026"
    assert attempts["n"] == 2


def test_scheduled_refresh_is_not_doubled_by_a_missing_league(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Плановое обновление и догон неизвестной лиги не должны идти подряд."""
    calls = {"n": 0}

    def _fake_reload():
        calls["n"] += 1
        return True

    monkeypatch.setattr(probe, "_LEAGUE_NAMES", {})
    # Справочник протух — сработает плановая перезагрузка...
    monkeypatch.setattr(probe, "_LEAGUE_NAMES_FETCHED_AT",
                        time.time() - probe._LEAGUE_NAMES_TTL - 1)
    monkeypatch.setattr(probe, "_LEAGUE_NAMES_LAST_MISS", 0.0)
    monkeypatch.setattr(probe, "_reload_league_names", _fake_reload)

    assert probe.league_name(20142, refresh_if_missing=True) == ""
    # ...и второй ходки за тем же файлом в ту же секунду не делаем.
    assert calls["n"] == 1


def test_explicit_league_ids_are_always_polled_directly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Явно разрешённый id обязан попадать в прямой опрос, даже вне окна id.

    10877 (тикет площадки Challengermode, под которым приезжают открытые квалы
    BLAST) заведён в 2019-м и ниже `KW_RECENT_FLOOR`, то есть в cold-sweep не
    попадал бы вовсе — оставалась бы только надежда на широкий снимок (0).
    """
    monkeypatch.setattr(probe, "_LEAGUE_NAMES", {
        19944: "EPL Masters 2026",
        19850: "KUZYA X ISLAM X AYATO CUP 6.3",
        10877: "Challengermode Daily Tournaments",
    })
    candidates = probe._keyword_candidate_league_ids()
    assert 19944 in candidates            # keyword-лига текущей эры
    assert 19850 not in candidates        # чужая лига
    assert 10877 in candidates            # явный id ниже KW_RECENT_FLOOR
    for lid in probe.TOURNAMENT_LEAGUE_ID_ALLOWLIST:
        assert int(lid) in candidates
    assert candidates == sorted(set(candidates))

import sys

import pytest


@pytest.fixture(autouse=True)
def _isolate_map_verdicts(tmp_path, monkeypatch):
    """Не даёт тестам писать в боевой журнал вердиктов (tail_log)."""
    monkeypatch.setenv("MAP_VERDICTS_PATH", str(tmp_path / "map_verdicts.json"))


@pytest.fixture(autouse=True)
def _isolate_sent_signal_fingerprints(tmp_path, monkeypatch):
    """Изолирует межинстансный дедуп сигналов от общего state-файла.

    Без изоляции отпечатки тестовых матчей (generic team names + замороженное
    time.time()) копятся в ~/.local/state/ingame/sent_signal_fingerprints.json
    и блокируют отправку в последующих тестах/прогонах.
    """
    monkeypatch.setenv(
        "SENT_SIGNAL_FINGERPRINT_PATH",
        str(tmp_path / "sent_signal_fingerprints.json"),
    )
    for module_name, module in list(sys.modules.items()):
        if module_name.rsplit(".", 1)[-1] == "cyberscore_try":
            registry = getattr(module, "_SIGNAL_DEDUP_FINGERPRINTS", None)
            if isinstance(registry, dict):
                registry.clear()
            sent_keys = getattr(module, "_SENT_SIGNAL_DEDUP_KEYS", None)
            if isinstance(sent_keys, set):
                sent_keys.clear()
    yield


@pytest.fixture(autouse=True)
def _isolate_winline_odds_history(tmp_path, monkeypatch):
    """Не даёт тестам писать в БОЕВОЙ архив котировок.

    `WINLINE_ODDS_HISTORY_PATH` в поллере вычисляется НА ИМПОРТЕ, поэтому одной
    переменной среды мало: если модуль уже загружен, константа в нём осталась
    боевой. Отсюда правим и атрибут модуля.

    Зачем. В архиве накопилось 463 записи с фиктивным временем (1700000000 и
    соседние круглые числа) — это тестовые прогоны, попавшие в файл, который
    мы собираемся джойнить с корпусом по `match_id`. Отличить их можно только
    по неправдоподобному `wall`, то есть постфактум и вручную.
    """
    target = str(tmp_path / "winline_odds_history.jsonl")
    monkeypatch.setenv("WINLINE_ODDS_HISTORY_PATH", target)
    for name, module in list(sys.modules.items()):
        if name.rsplit(".", 1)[-1] == "winline_current_map_odds_poller":
            if hasattr(module, "WINLINE_ODDS_HISTORY_PATH"):
                monkeypatch.setattr(module, "WINLINE_ODDS_HISTORY_PATH", target,
                                    raising=False)


@pytest.fixture(autouse=True)
def _isolate_winline_sent_journal(tmp_path, monkeypatch):
    """Не даёт тестам писать в боевой журнал отправленных карточек кэфов.

    Путь читается НА ВЫЗОВЕ (`WINLINE_ODDS_TELEGRAM_SENT_PATH`), поэтому, в
    отличие от архива котировок, достаточно переменной среды.
    """
    monkeypatch.setenv(
        "WINLINE_ODDS_TELEGRAM_SENT_PATH",
        str(tmp_path / "winline_telegram_sent.jsonl"))


@pytest.fixture(autouse=True)
def _isolate_live_elo_progress(tmp_path, monkeypatch):
    """Не даёт тестам писать в БОЕВОЕ состояние живого рейтинга.

    Зачем. 23.08.2026 прогон набора на serv1 затёр `pending_series` в боевом
    `runtime/live_elo_progress.json`: там осталось три записи с ключами
    `dltv.org/matches/test-match`, `test-integrity`, `test-shadow-every-map` и
    ни одной настоящей серии. Сам рейтинг (`live_elo_model_state.json`) и
    журнал `applied_maps` уцелели, но очередь отложенных карт была потеряна.

    Путь в `ELO.live_team_strength` вычисляется на импорте и переменной среды
    не читает, поэтому правится атрибут модуля; в `series_surprise_shadow` есть
    и env, и атрибут — закрываем оба.
    """
    target = tmp_path / "live_elo_progress.json"
    monkeypatch.setenv("LIVE_ELO_PROGRESS", str(target))
    monkeypatch.setenv("SERIES_SURPRISE_STORE",
                       str(tmp_path / "series_surprise_shadow.json"))
    for name, module in list(sys.modules.items()):
        leaf = name.rsplit(".", 1)[-1]
        if leaf == "live_team_strength":
            monkeypatch.setattr(module, "DEFAULT_RUNTIME_PROGRESS_PATH", target,
                                raising=False)
        elif leaf == "cyberscore_try":
            # Путь копируется в модуль ЗНАЧЕНИЕМ на импорте (`... as
            # _elo_live_default_progress_path`), поэтому правка исходного
            # модуля сюда не доезжает.
            monkeypatch.setattr(module, "_elo_live_default_progress_path", target,
                                raising=False)
        elif leaf == "series_surprise_shadow":
            monkeypatch.setattr(module, "DEFAULT_ELO_PROGRESS_PATH", target,
                                raising=False)
            monkeypatch.setattr(module, "DEFAULT_STORE_PATH",
                                tmp_path / "series_surprise_shadow.json",
                                raising=False)


@pytest.fixture(autouse=True)
def _isolate_prematch_eval_journal(tmp_path, monkeypatch):
    """Не даёт тестам писать в БОЕВОЙ журнал оценок предматчевой модели.

    23.08.2026 прогон набора добавил в `runtime/prematch_model_eval.jsonl`
    четыре записи с пустыми именами команд и причиной «не передан
    hybrid_strength». Журнал диагностический, но по нему судят о работе модели
    в бою, и отличить тестовые строки от настоящих можно только по этим
    пустым именам — то есть постфактум и на глаз.
    """
    target = str(tmp_path / "prematch_model_eval.jsonl")
    monkeypatch.setenv("PREMATCH_EVAL_JOURNAL", target)
    for name, module in list(sys.modules.items()):
        if name.rsplit(".", 1)[-1] == "win_model_veto":
            monkeypatch.setattr(module, "_EVAL_JOURNAL", target, raising=False)


@pytest.fixture(autouse=True)
def _isolate_prematch_live_delta(tmp_path, monkeypatch):
    """Не даёт score() читать или завести боевой runtime/prematch_live_delta.json."""
    monkeypatch.setenv("PREMATCH_LIVE_DELTA", str(tmp_path / "prematch_live_delta.json"))


@pytest.fixture(autouse=True)
def _isolate_tier2_dynamic_overlay(tmp_path, monkeypatch):
    """Изолирует overlay динамического tier2-onboarding от боевого файла.

    Путь читается на вызове (env `TIER2_DYNAMIC_ONBOARDING_PATH`), поэтому
    переменной среды достаточно; флаг однократной загрузки сбрасываем, чтобы
    каждый тест перечитывал изолированный overlay, а не кэш предыдущего.
    """
    monkeypatch.setenv(
        "TIER2_DYNAMIC_ONBOARDING_PATH",
        str(tmp_path / "id_to_names_dynamic_tier2.json"),
    )
    for name, module in list(sys.modules.items()):
        if name.rsplit(".", 1)[-1] == "cyberscore_try":
            monkeypatch.setattr(
                module, "_dynamic_tier2_overlay_loaded", False, raising=False
            )

#!/usr/bin/env python3
"""Живой провайдер блока `hybrid_*` — производственный гибридный рейтинг.

ДВЕ КОЛОНКИ И ИХ ОПРЕДЕЛЕНИЕ взяты из сборщика обучающих колонок
(`runtime/experiments/misc/map_winner_hybrid_quality_forward.py:371-372`):

    hybrid_0  hybrid_production_logit          logit(clip(p_radiant, 1e-9))
    hybrid_1  hybrid_raw_strength_diff_div400  (сила Rad − сила Dire) / 400

Порядок подтверждён трижды: `kills27_max_model.py:129` кладёт блок как
`hz["F"]`, разбор границ разбиений CatBoost независимо дал ту же привязку с
запасом 6.4x и 3.3x над альтернативой, и наконец живой блок сверен с колонками
обучения числами — `runtime/artifacts/misc/verify_hybrid_block.md`.

Та сверка идёт не по max|Δ|, а по ЗАВИСИМОСТИ согласия от возраста карты, и это
единственный корректный способ: обучение считало as-of состояние до исхода
карты, а снимок уже впитал эти исходы. Совпади формула — согласие обязано расти
к дате снимка, и оно растёт: 0.90 на картах моложе недели против 0.71 на
годовалых. Неверная формула дала бы ровную корреляцию.

ОТКУДА БЕРУТСЯ ЧИСЛА. Гибрид — не наш новый автомат: это боевой пакет `ELO/`,
уже вшитый в бота (`cyberscore_try.py` импортирует `ELO.live_team_strength`).
Снимок `ELO/output/live_team_elo_snapshot.json` содержит состояние модели, из
него поднимается `HybridPlayerRosterEloModel`, и дальше зовётся ровно тот же
`predict_match`, что звало обучение. Своей копии формулы здесь нет — она была бы
пятой копией и разъехалась бы первой.

МОДЕЛЬ ИГРАЕТ ОТ ИГРОКОВ, А НЕ ОТ НАЗВАНИЯ КОМАНДЫ. Состояние хранит рейтинги
316 828 игроков, ростеры и составы; при пустых `player_ids` обе стороны выходят
нулевыми, а `p_radiant` — ровно 0.5. Поэтому десять аккаунтов обязательны, а
идентификаторы команд — нет: замер `verify_hybrid_live_args.md` дал одинаковый
результат для вызова с id и без него (0.997213 в обоих случаях).

ЦЕНА. Снимок весит 366 МБ и читается ~3.6 с — один раз на процесс, как и
остальные тяжёлые таблицы панели. Сам вызов стоит 0.2 мс.
"""
from __future__ import annotations

import math
import os
import threading
from pathlib import Path
from typing import Any, Sequence
import time

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = Path(os.getenv("PANEL_HYBRID_SNAPSHOT",
                          str(PROJECT_ROOT / "ELO" / "output" /
                              "live_team_elo_snapshot.json")))

COLUMNS: tuple[str, ...] = ("hybrid_0", "hybrid_1")
CLIP = 1e-9

_lock = threading.Lock()
SNAPSHOT_WARN_DAYS = 14.0

_state: dict[str, Any] = {"loaded": False, "model": None, "names": {},
                          "error": None, "built_ts": 0}


def _load() -> dict[str, Any]:
    """Ленивая загрузка снимка и восстановление модели."""
    with _lock:
        if _state["loaded"]:
            return _state
        _state["loaded"] = True
        try:
            import sys

            if str(PROJECT_ROOT) not in sys.path:
                sys.path.insert(0, str(PROJECT_ROOT))
            from ELO.live_team_strength import (_restore_model_from_snapshot,
                                                load_live_snapshot)

            # Снимок берём ОБЩИМ загрузчиком пакета, а не своим `json.loads`.
            # Причина не в красоте: `live_team_strength` держит модульный кэш
            # разобранного словаря, а `_restore_model_from_snapshot` кэширует
            # восстановленную модель по `id(snapshot)` — по тождеству ОБЪЕКТА.
            # Свой разбор дал бы другой объект, значит промах кэша: 366 МБ JSON
            # разбирались бы второй раз, и в том же процессе появилась бы ВТОРАЯ
            # модель на 316 828 игроков. А процесс этот — боевой бот, который
            # ведёт ставки. Через общий загрузчик обе копии переиспользуются, и
            # если бот уже поднял снимок сам, блок не стоит вообще ничего.
            # Общий загрузчик кэширует разобранный словарь и ВОЗВРАЩАЕТ КЭШ,
            # игнорируя переданный путь. В бою путь один, и это ровно то, что
            # нужно. Но если просят другой файл (тесты, `PANEL_HYBRID_SNAPSHOT`),
            # кэш отдал бы чужой снимок молча — поэтому общим путём идём только
            # когда просят ТОТ ЖЕ файл, что у пакета по умолчанию.
            from ELO.live_team_strength import DEFAULT_SNAPSHOT_PATH

            if Path(SNAPSHOT).resolve() == Path(DEFAULT_SNAPSHOT_PATH).resolve():
                # `load_live_snapshot`, а не `load_snapshot`: базовый файл
                # пересобирается редко (на боевой машине он от 12.08), а
                # рантайм-состояние обновляется постоянно. Колонка `hybrid_*`
                # считалась по рейтингам восьмидневной давности, хотя свежие
                # лежали в том же процессе.
                snap = load_live_snapshot(SNAPSHOT)
            else:
                import json as _json

                snap = (_json.loads(SNAPSHOT.read_text(encoding="utf-8"))
                        if SNAPSHOT.exists() else None)
            if snap is None:
                raise ValueError(f"снимок не найден: {SNAPSHOT}")
            model = _restore_model_from_snapshot(snap)
            if model is None:
                raise ValueError("модель не восстановилась из снимка")
            # Имена команд по id — только для заполнения MatchRecord; на числа
            # они не влияют (проверено), но пустое имя мешает разбору org-ключа.
            names = {}
            for row in (snap.get("teams_by_org_key") or {}).values():
                tid = row.get("team_id")
                if tid:
                    names[int(tid)] = str(row.get("team_name") or "")
            _built = int((snap.get("meta") or {}).get("reference_timestamp") or 0)
            _state.update(model=model, names=names, built_ts=_built)
            # `built_ts` клали сюда с самого начала и нигде не сравнивали —
            # аудит 19.08.2026. Отказ мягкий: блок продолжает считать.
            if _built:
                _age = (time.time() - float(_built)) / 86400.0
                if _age > SNAPSHOT_WARN_DAYS:
                    print(f"ВНИМАНИЕ: ELO/Hybrid-снимок старше "
                          f"{SNAPSHOT_WARN_DAYS:.0f} суток (возраст {_age:.1f}) — "
                          "сила команд считается по устаревшему корпусу.",
                          flush=True)
        except Exception as exc:                     # noqa: BLE001
            _state["error"] = f"{type(exc).__name__}: {exc}"
        return _state


def block(now_ts: int, accounts10: Sequence[int],
          team_ids: Sequence[int] | None = None,
          tier: str | None = None) -> dict[str, float] | None:
    """Две колонки по времени, десяти аккаунтам и (необязательно) id команд.

    `None`, если снимок не поднялся: непоставленный блок честнее нулей, которых
    модель не видела.
    """
    st = _load()
    model = st.get("model")
    if model is None:
        return None
    try:
        from ELO.domain import LeagueTier, MatchRecord

        acc = [int(a) for a in accounts10]
        if len(acc) != 10:
            raise ValueError(f"ожидались 10 аккаунтов, получено {len(acc)}")
        # `or` здесь недопустим: на numpy-массиве он падает на неоднозначной
        # истинности, и блок молча становился None.
        tids = [0, 0] if team_ids is None else [int(t) for t in team_ids]
        while len(tids) < 2:
            tids.append(0)
        nm = st["names"]
        lt = LeagueTier.TIER3
        if tier:
            try:
                lt = LeagueTier(str(tier).upper())
            except ValueError:
                lt = LeagueTier.TIER3
        rec = MatchRecord(
            match_id=0, timestamp=int(now_ts), radiant_win=True,
            radiant_team_id=(tids[0] or None),
            radiant_team_name=nm.get(tids[0], ""),
            dire_team_id=(tids[1] or None),
            dire_team_name=nm.get(tids[1], ""),
            radiant_player_ids=tuple(acc[:5]), dire_player_ids=tuple(acc[5:]),
            league_id=None, league_name="", source_league_tier=None,
            series_id=None, series_type=None, derived_league_tier=lt)
        step = model.predict_match(rec)
        p = min(max(float(step.p_radiant), CLIP), 1.0 - CLIP)
        return {
            "hybrid_0": math.log(p / (1.0 - p)),
            "hybrid_1": (float(step.radiant_strength)
                         - float(step.dire_strength)) / 400.0,
        }
    except Exception as exc:                         # noqa: BLE001
        _state["error"] = f"{type(exc).__name__}: {exc}"
        return None


def status() -> dict[str, Any]:
    st = _load()
    return {"ready": st.get("model") is not None, "error": st.get("error"),
            "path": str(SNAPSHOT), "built_ts": st.get("built_ts", 0)}

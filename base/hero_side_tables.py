#!/usr/bin/env python3
"""Симметричный блок карточки героя: таблицы и суммы по стороне.

Эти две функции строят 742 колонки из 822 в предматчевой панели — девять
десятых её входа. До сих пор они жили в `runtime/experiments/misc/eval_pregame_features.py`,
то есть в gitignored-эксперименте, и живой путь построил бы их СВОИМ кодом.
Ровно так уже разъехались обучение и раздача у драфт-логита (E-201, цена
−0.0046), поэтому здесь один общий модуль на оба пути: офлайн-сборка и прод
импортируют его, а не переписывают.

  `hero_tables(pf)`  разворачивает карточку в две матрицы: константы героя
                     `C[hero, признак]` и baseline `B[hero, позиция, метрика]`;
  `side_matrix(her)` складывает пятёрку в вектор стороны.

Среднее по команде намеренно не выдаётся: оно равно сумме, делённой на пять,
то есть коллинеарно ей.

Версия карточки. Модели обучены под конкретный `hero_features_v7.json`, и если
на проде окажется другой, 742 колонки поедут молча. `card_fingerprint()` даёт
короткий отпечаток, который кладётся в артефакт при обучении и сверяется при
загрузке.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import numpy as np

N_POSITIONS = 6            # 0 — «все», 1-5 — позиции
TEAM_SLOTS = 5


def columns_sha(cols) -> str:
    """Отпечаток состава и ПОРЯДКА колонок панели.

    Живёт здесь, а не у сборщика, ровно по той же причине, что и таблицы героев:
    обе стороны обязаны считать его ОДНОЙ функцией. Сборщик кладёт значение в
    `manifest.json`, живой путь сверяет при загрузке.

    Зачем вообще. Модели панели сохраняются без имён колонок — у `.cbm`
    `feature_names_` это `['0'..'927']`, скоринг чисто позиционный. Проверка при
    загрузке сравнивала только ДЛИНУ, поэтому перестановка 928 колонок при той
    же длине прошла бы молча.
    """
    import hashlib
    return hashlib.sha256(
        "\n".join(str(c) for c in cols).encode("utf-8")).hexdigest()[:16]


def card_fingerprint(path: str | Path) -> str:
    """Короткий sha1 файла карточки — тот же способ, что у сборщика каталога."""
    h = hashlib.sha1()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:12]


def hero_tables(pf: Any) -> tuple[np.ndarray, np.ndarray, list[str], list[str]]:
    """Константы героя и baseline герой×позиция в виде матриц для векторизации."""
    ids = sorted(pf.heroes)
    if not ids:
        raise ValueError("в карточке нет героев")
    hmax = max(ids) + 1
    cnames = list(pf.hero_numeric_fields) + [f"{f}_count" for f in pf.hero_list_fields]
    C = np.zeros((hmax, len(cnames)))
    for h in ids:
        row = pf.heroes[h]
        C[h] = [float(row.get(f) or 0.0) for f in pf.hero_numeric_fields] + \
               [float(len(row.get(f) or [])) for f in pf.hero_list_fields]
    bnames: list[str] = []
    for corp in sorted(pf.baseline_metrics):
        bnames += [f"bl_{corp}_{n}" for n in pf.baseline_metrics[corp]]
    B = np.zeros((hmax, N_POSITIONS, len(bnames)))
    for h in ids:
        base = pf.heroes[h].get("baselines") or {}
        for pos in range(N_POSITIONS):
            col = 0
            for corp in sorted(pf.baseline_metrics):
                b = base.get(corp) or {}
                # позиционная ячейка берётся только если она надёжна, иначе
                # откат к «все позиции» — иначе редкая позиция даёт шум
                cell = ((b.get("pos") or {}).get(str(pos)) or {}) if pos else (b.get("all") or {})
                if not cell.get("reliable_mean"):
                    cell = b.get("all") or {}
                m = cell.get("m") or {}
                for n in pf.baseline_metrics[corp]:
                    v = m.get(n)
                    B[h, pos, col] = float(v[0]) if v and v[0] is not None else 0.0
                    col += 1
    return C, B, cnames, bnames


def side_matrix(her: np.ndarray, C: np.ndarray, B: np.ndarray,
                chunk: int = 100_000) -> np.ndarray:
    """(карт, признаков стороны): суммы констант и baseline по пятёрке.

    Кусками: `C[her]` на пяти миллионах карт — матрица на 26 ГБ, первый заход
    упёрся именно в это. На одной карте (живой путь) кусок ровно один.
    """
    her = np.asarray(her)
    if her.ndim != 2 or her.shape[1] != TEAM_SLOTS:
        raise ValueError(f"ожидались (n, {TEAM_SLOTS}) hero_id, получено {her.shape}")
    n = len(her)
    out = np.zeros((n, C.shape[1] + B.shape[2]), dtype=np.float32)
    for i in range(0, n, chunk):
        h = her[i:i + chunk]
        pos = np.tile(np.arange(1, TEAM_SLOTS + 1), (len(h), 1))
        out[i:i + chunk, :C.shape[1]] = C[h].sum(1)
        out[i:i + chunk, C.shape[1]:] = B[h, pos].sum(1)
    return out


def sym_block(heroes10: np.ndarray, C: np.ndarray, B: np.ndarray) -> np.ndarray:
    """Симметричный блок: [разность сторон, сумма сторон].

    Разность отвечает на «кто сильнее», сумма — на «какая карта вообще»;
    вместе они покрывают и знаковые, и беззнаковые цели одним блоком.
    """
    heroes10 = np.asarray(heroes10)
    if heroes10.ndim != 2 or heroes10.shape[1] != 2 * TEAM_SLOTS:
        raise ValueError(f"ожидались (n, {2*TEAM_SLOTS}) hero_id, "
                         f"получено {heroes10.shape}")
    R = side_matrix(heroes10[:, :TEAM_SLOTS].astype(np.int64), C, B)
    D = side_matrix(heroes10[:, TEAM_SLOTS:].astype(np.int64), C, B)
    return np.hstack([R - D, R + D])

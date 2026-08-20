#!/usr/bin/env python3
"""Выравнивание замороженных кэшей признаков с растущим корпусом — по `mid`.

ЗАЧЕМ. Кэши обучающих признаков адресуются ПОЗИЦИЕЙ в массиве и заморожены на
482 486 строках, а про-корпус вырос до 1 260 420 карт. Любая маска, построенная
по длине корпуса, не налезает на кэш, и numpy падает:

    ValueError: could not broadcast input array from shape (1260420,)
                into shape (482486,)

Из-за этого 19.08.2026 не запустился сквозной аудит E-177 — тот самый, что
однажды нашёл пять разошедшихся колонок ценой −0.0015 AUC. То есть сломан не
только путь переобучения, но и ПРИБОР, которым проверяют качество.

ПОЧЕМУ НЕ ОБРЕЗКА. Напрашивается взять первые 482 486 строк корпуса, но это
неверно: замороженный набор — ПОДПОСЛЕДОВАТЕЛЬНОСТЬ корпуса, а не его начало.
Проверено: его карты лежат на позициях от 30 до 1 260 254, монотонно, но с
разрывами. Обрезка даёт ноль тестовых карт после TEST_FROM — именно так и
выглядел отказ.

ЧТО ЗДЕСЬ. Канонический порядок замороженных строк восстановим: десять кэшей
несут массив `mids` на 482 486 записей, и порядок у всех ДЕСЯТИ идентичен
(сверено поэлементно). Значит кэшу без `mids` можно сопоставить карту по
позиции, а дальше join с корпусом идёт по `mid`, а не по индексу.

Снимаются ОБЕ поломки прибора, а не одна. Второй отказ был
`KeyError` на отсутствующем обучающем `hybrid_strength` — он возникал потому,
что обход шёл по всем 1 260 420 картам, где у 248 946 значений нет. На
замороженном наборе покрытие полное: `hybrid_features.npz` даёт 100%,
`hybrid_strength_tier3.npz` — 97.1%.

Проверка, что выравнивание восстанавливает именно тот тест, на котором стоят
эксперименты: после него остаётся 26 016 тестовых карт после `TEST_FROM` —
ровно то число, что фигурирует в E-173, E-175 и E-201. Наивная обрезка первых
482 486 строк даёт ноль.

Это не заменяет пересборку кэшей на полном корпусе — обучение по-прежнему
видит только 482 486 карт из 1 260 420. Но замерять снова можно.

ИСПОЛЬЗОВАНИЕ:

    from frozen_cache_index import frozen_mids, align_to_corpus

    rows, mask = align_to_corpus(corpus_mids)
    # rows — индексы строк в замороженном кэше для карт, где он есть
    # mask — какие карты корпуса вообще покрыты кэшем
    F_aligned = np.load(CACHE)["F"][rows]
    y = wins[mask]
"""
from __future__ import annotations

import os
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
ART = ROOT / "runtime/artifacts/misc"

# Кэши, несущие канонический порядок. Порядок сверен поэлементно между всеми
# десятью 19.08.2026; берём первый доступный.
_SOURCES = (
    "kills_dict_features.npz",
    "win_blocks_player.npz",
    "undercount_glicko_features.npz",
    "public_kills_pro_features.npz",
    "lan_model_P.npz",
)

FROZEN_ROWS = 482486
_cache: np.ndarray | None = None


def frozen_mids() -> np.ndarray:
    """Канонический порядок mid для замороженного поколения кэшей."""
    global _cache
    if _cache is not None:
        return _cache
    for name in _SOURCES:
        path = ART / name
        if not path.exists():
            continue
        try:
            mids = np.asarray(np.load(path, allow_pickle=True)["mids"]).astype(np.int64)
        except (OSError, ValueError, KeyError):
            continue
        if len(mids) != FROZEN_ROWS:
            continue
        _cache = mids
        return _cache
    raise SystemExit(
        "не найден ни один кэш с каноническим порядком mids на "
        f"{FROZEN_ROWS} строк — проверьте {ART}")


def align_to_corpus(corpus_mids) -> tuple[np.ndarray, np.ndarray]:
    """Индексы строк замороженного кэша под карты корпуса.

    Возвращает `(rows, mask)`:
      * `mask` — булев массив длины корпуса: у каких карт кэш вообще есть;
      * `rows` — индексы в кэше для карт, где `mask` истинна, в том же порядке.

    Обе величины нужны вместе: `CACHE["F"][rows]` встаёт строка в строку с
    `corpus_array[mask]`, и позиционного выравнивания больше нигде не остаётся.
    """
    corpus = np.asarray(corpus_mids).astype(np.int64)
    pos = {int(m): i for i, m in enumerate(frozen_mids().tolist())}
    mask = np.zeros(len(corpus), dtype=bool)
    rows_list: list[int] = []
    for i, m in enumerate(corpus.tolist()):
        r = pos.get(int(m))
        if r is not None:
            mask[i] = True
            rows_list.append(r)
    return np.asarray(rows_list, dtype=np.int64), mask


def coverage(corpus_mids) -> dict:
    """Насколько кэш покрывает корпус — для диагностики и предупреждений."""
    _rows, mask = align_to_corpus(corpus_mids)
    total = int(len(mask))
    covered = int(mask.sum())
    return {"corpus": total, "covered": covered,
            "uncovered": total - covered,
            "share": (covered / total) if total else 0.0}


if __name__ == "__main__":
    z = np.load(ART / "pro_corpus_compact.npz", allow_pickle=True)
    info = coverage(z["mids"])
    print(f"корпус: {info['corpus']:,} карт")
    print(f"покрыто замороженным кэшем: {info['covered']:,} ({info['share']:.1%})")
    print(f"вне кэша: {info['uncovered']:,}")

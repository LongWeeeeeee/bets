#!/usr/bin/env python3
"""Предматчевая модель в бою: строгий режим без дефолтов.

Модель обучена на 482 486 про-картах (E-88…E-101). Артефакт v2: 29 признаков
(20 базовых + lvl_rel_pos/kda_player/farm_dep + 6 взаимодействий с контекстом
матча), AUC 0.7142 на всём тесте и 0.7483 на НАСТОЯЩИХ офлайн-турнирах.
Подтверждена на шести непересекающихся forward-окнах.

Взаимодействие с ЭПОХОЙ намеренно исключено, хотя давало +0.0009: в обучении
она лежит в [0,1], а в бою всегда >1 — модель экстраполировала бы по времени.

ПРИНЦИП: дефолтов нет. Если для матча не хватает данных — анализ не отдаётся,
возвращается `MissingData` с перечислением того, чего именно нет. Это сделано
намеренно: у каждого признака есть «безопасное» значение (рейтинг 1500, ноль
игр, винрейт 0.5), и модель на них не падает — она посчитает вероятность для
десяти неизвестных игроков и вернёт уверенное число из воздуха.

УРОВНИ СТРОГОСТИ и их измеренная цена (симуляция «снимок собран за сутки до
карты», 26 016 про-карт теста; в скобках доля от офлайн-турниров):

    full     всё, включая ячейки (аккаунт, герой) и историю встреч — 13.0% (27.0%)
    cells    аккаунты + команды + ячейки (аккаунт, герой)          — 18.4% (38.6%)
    teams    аккаунты + id обеих команд                            — 65.0% (63.0%)
    accounts только все 10 аккаунтов известны                       — 77.1% (65.1%)

Требование ячеек (аккаунт, герой) режет выборку в четыре раза: игроки постоянно
берут новых героев, и отсутствие ячейки — не «данных нет», а «он на этом герое
ещё не играл». Уровень выбирается вызывающим осознанно.

ПРОВЕРКИ, КОТОРЫЕ ЛОВЯТ МОЛЧАЛИВУЮ ПОЛОМКУ:
  * протухший снимок — `wr30` это окно 30 дней, `vs_wr` распад 45 дней; при
    возрасте больше `max_age_days` анализ не отдаётся. ВНИМАНИЕ: дефолт
    сигнатуры (3.0) вдесятеро строже боевого значения — прод
    (`win_model_veto._prematch_index`) передаёт `_SNAPSHOT_MAX_AGE_DAYS`,
    по умолчанию 30 и настраивается `WIN_MODEL_SNAPSHOT_MAX_AGE_DAYS`.
    За качество на 3 сутках отвечает отдельное предупреждение
    `_SNAPSHOT_WARN_DAYS`, а не этот жёсткий отказ;
  * сломанная разметка позиций — модель ждёт позиции 1..5 по порядку, и ошибка
    резолвера молча сдвинет `pos_games` и ячейки. Калибровка на 1 500 свежих
    картах: жёсткий порог 0.1% ложных отказов при 42.7% пойманных перестановок,
    мягкий (замечание) 2.5% при 71.2%.

КАЛИБРОВКА НА LAN. На офлайн-турнирах модель систематически НЕДООЦЕНИВАЕТ себя:
при заявленных 50-60% фактический винрейт 60.3%, при 80-90% — 89.4%. Изотоника,
обученная на исторических LAN-картах, эту разницу не чинит (в бакете 80-90%
даёт 83.3% против факта 89.4%), поэтому используется эмпирическая таблица
`LAN_CALIBRATION`: `lan_winrate` возвращает фактический винрейт для уровня
уверенности. Таблица меряна на 1 391 офлайн-карте.
"""
from __future__ import annotations

import itertools
import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence

import numpy as np

try:                                   # так же, как в `win_model_veto`
    import prematch_components as _C
except ImportError:                    # pragma: no cover
    from base import prematch_components as _C

try:
    import prematch_live_delta as _DELTA
except ImportError:                    # pragma: no cover
    try:
        from base import prematch_live_delta as _DELTA
    except ImportError:                # pragma: no cover
        _DELTA = None

ARTIFACT_PATH = os.getenv(
    "PREMATCH_ARTIFACT",
    str(Path(__file__).resolve().parents[1] / "data" / "prematch_model_artifact_v3.npz"),
)
BASE20 = ["draft_logit", "elo", "games", "hero_games", "pos_games", "opp_elo",
          "hero_pool", "form", "hero_gpm_rel", "imp_recent", "wr30", "h2h_resid",
          "gpm_rel_pos", "vs_wr", "imp50", "imp_rel_pos", "lh_rel_hero",
          "gpm_ewma", "lh30", "imp30"]
EXTRA3 = ["lvl_rel_pos", "kda_player", "farm_dep"]
INTER_KEYS = ["draft_logit", "elo", "form"]
# E-168: шесть колонок кандидата C. Порядок обязан совпадать со сборщиком
# (`build_prematch_artifact_v2.py`), но опорой служит `feature_names` из самого
# артефакта — константа ниже нужна только для артефактов без этого поля.
NEW6 = ["hybrid_strength", "cp_lane", "syn_pos_mean",
        "a_hdmg_rel_pos", "a_hdmg_rel_hero", "a_nw_rel_pos"]
FEATURES = BASE20 + EXTRA3 + [f"{k}_x_{c}" for c in ("elo_gap", "games_exp") for k in INTER_KEYS]
STRICTNESS = ("accounts", "teams", "cells", "full")
# Позиционные про-ячейки контрпика и синергии: распад 45 дней и шринкедж к
# позиционно-агностичной паре — один в один с `ideas_batch8.build`, которым
# считались обучающие колонки. Линиями считаются те же пять противостояний,
# что и в продовом cp1vs1.
DRAFT_HL_DAYS = 45.0
LANES = ((1, 3), (2, 2), (3, 1), (4, 5), (5, 4))

# Пороги на НАСТОЯЩИХ офлайн-турнирах, без открытых квалификаций (E-142).
# 2 456 карт, честные предсказания из шести forward-окон. Прежняя таблица
# считалась на выборке, где 74% — квалификации (профи против любителей), и была
# завышена: там ≥90% даёт 95.9%, а на настоящих LAN 83.8%.
# (порог уверенности, доля отбора, винрейт, карт, безубыточный кэф)
LAN_THRESHOLDS = ((0.50, 1.00, 0.651, 2456, 1.54), (0.55, 0.82, 0.677, 2017, 1.48),
                  (0.60, 0.66, 0.710, 1621, 1.41), (0.65, 0.52, 0.743, 1282, 1.35),
                  (0.70, 0.38, 0.777, 942, 1.29), (0.75, 0.29, 0.803, 702, 1.24),
                  (0.80, 0.20, 0.830, 489, 1.20), (0.85, 0.12, 0.859, 284, 1.16))
# ≥90% намеренно не включён: 83.8% на 111 картах, монотонность ломается.


# --- Пороговая сетка предматчевой модели на НАСТОЯЩИХ офлайн-турнирах ---------
# Точечная калибровка (уверенность -> фактический винрейт), 2 456 карт из шести
# forward-окон, открытые квалификации исключены: они профи против любителей и
# завышали кривую. Выше 85% значение заморожено — в полосах там меньше 40 карт.
#
# ПРАВКА 20.08.2026 по сквозному аудиту боевого пути (11 064 карты,
# `audit_live_path_raw.npz`). Правило одностороннее: значение заменяется, только
# если односторонняя нижняя граница Уилсона 90% ВЫШЕ действующего и в группе не
# меньше 60 карт, — так правка может лишь добавить запас. Единица правки —
# ГРУППА процентов с одним значением, а не произвольная полоса: иначе граница
# считается по картам, которых изменение не касается.
#
#   58-63:  0.5817 -> 0.600, кэф 1.72 -> 1.67   (2 319 карт, факт 0.613)
#   77-80:  0.7541 -> 0.760, кэф 1.33 -> 1.32   (  651 карта, факт 0.782)
#
# Остальные девять групп не тронуты: их нижняя граница не выше действующего.
#
# ТАМ ЖЕ ИСПРАВЛЕНО ОКРУГЛЕНИЕ. У двух групп кэф был округлён ВНИЗ, то есть ниже
# безубыточного: 66-70 стояло 1.54 при 1/0.6474 = 1.5447, а 82 — 1.25 при
# 1/0.7985 = 1.2523. Ставка по такой цене приносит −0.30% и −0.19% при ЗАЯВЛЕННОМ
# винрейте, то есть обещание не покрывало цену. Поправлено до 1.55 и 1.26.
# Инварианты закреплены тестом `base/tests/test_lan_odds_grid.py`.
# Отдача на единицу ставки при точном `min_odds` остаётся положительной и после
# правки (+2.4% и +3.2%), то есть добираемые ставки прибыльны. Замер идёт по
# снимку, который в аудите стареет на месяцы, поэтому он ЗАНИЖАЕТ качество —
# и повышение тем более безопасно.
LAN_ODDS_GRID = {
    50: (0.5083, 1.97),
    51: (0.5083, 1.97),
    52: (0.5083, 1.97),
    53: (0.5387, 1.86),
    54: (0.5387, 1.86),
    55: (0.5387, 1.86),
    56: (0.5387, 1.86),
    57: (0.5387, 1.86),
    58: (0.6, 1.67),
    59: (0.6, 1.67),
    60: (0.6, 1.67),
    61: (0.6, 1.67),
    62: (0.6, 1.67),
    63: (0.6, 1.67),
    64: (0.6267, 1.6),
    65: (0.6342, 1.58),
    66: (0.6474, 1.55),
    67: (0.6474, 1.55),
    68: (0.6474, 1.55),
    69: (0.6474, 1.55),
    70: (0.6474, 1.55),
    71: (0.7057, 1.42),
    72: (0.7057, 1.42),
    73: (0.7057, 1.42),
    74: (0.7324, 1.37),
    75: (0.7324, 1.37),
    76: (0.7324, 1.37),
    77: (0.76, 1.32),
    78: (0.76, 1.32),
    79: (0.76, 1.32),
    80: (0.76, 1.32),
    81: (0.7763, 1.29),
    82: (0.7985, 1.26),
    83: (0.8207, 1.22),
    84: (0.8207, 1.22),
    85: (0.8207, 1.22),
    86: (0.8207, 1.22),
    87: (0.8207, 1.22),
    88: (0.8207, 1.22),
    89: (0.8207, 1.22),
    90: (0.8207, 1.22),
    91: (0.8207, 1.22),
    92: (0.8207, 1.22),
    93: (0.8207, 1.22),
    94: (0.8207, 1.22),
    95: (0.8207, 1.22),
    96: (0.8207, 1.22),
    97: (0.8207, 1.22),
    98: (0.8207, 1.22),
    99: (0.8207, 1.22),
}
LAST_RELIABLE_CONF = 85


def lan_min_odds(confidence: float) -> float:
    """Минимальный кэф, при котором ставка по модели окупается на LAN."""
    pct = max(50, min(99, int(round(confidence * 100))))
    return LAN_ODDS_GRID[pct][1]


def lan_expected_wr(confidence: float) -> float:
    """Фактический винрейт на LAN для этой уверенности."""
    pct = max(50, min(99, int(round(confidence * 100))))
    return LAN_ODDS_GRID[pct][0]


class MissingData(Exception):
    """Данных не хватает — анализ не отдаём. `details` перечисляет чего именно."""

    def __init__(self, details: list[str]) -> None:
        super().__init__("; ".join(details))
        self.details = details


@dataclass
class ScoreResult:
    probability: float
    lan_winrate: float
    features: dict[str, float] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)
    # Доля входа, собранная из РЕАЛЬНЫХ данных, а не заменённая нулём. При
    # боевой строгости `teams` отказ бросается только на семь причин, а
    # отсутствующая ячейка (игрок, герой) молча становится нулём — так же, как
    # в обучении, поэтому это не ошибка. Но раньше об этом никто не узнавал:
    # при трёх занулённых слотах из десяти число выглядело так же уверенно, как
    # при полном входе. Замер 15.08: полный вход только у 17.0% карт, в среднем
    # занулено 3.29 слота из 10 (E-195).
    coverage: dict[str, float] = field(default_factory=dict)
    # Какая ветка лестницы сработала и под какие ключи данных их не было.
    # Поле обязано попадать в журнал: 75% от восьмиколоночной ветки и 75% от
    # полной — разные числа, и продавать их одинаково значит систематически
    # ошибаться в требуемом коэффициенте.
    branch: str = "full"
    missing_keys: list[str] = field(default_factory=list)
    # Вклад каждого компонента в логит. Сумма частей плюс сдвиг ветки равна
    # самому логиту — разложение точное по построению, а не оценочное.
    parts: dict[str, float] = field(default_factory=dict)

    @property
    def confidence(self) -> float:
        return max(self.probability, 1.0 - self.probability)

    @property
    def pick_radiant(self) -> bool:
        return self.probability > 0.5


class PairTable:
    """Таблица (аккаунт, герой) → строка значений на отсортированных массивах.

    ЗАЧЕМ. Словарём эта таблица стоила 2 005 МБ при 311 МБ полезных данных —
    замерено на боевом артефакте (6 784 232 ячейки). На каждую ячейку
    приходились кортеж-ключ из двух Python-int, отдельный объект-представление
    numpy и слот словаря; значения при этом были ВИДАМИ в исходный массив,
    поэтому в памяти жили одновременно и массив, и словарь. Здесь живут ровно
    два массива: упакованные ключи и значения.

    Цена — поиск 870 нс против 52 нс у словаря. В боевом пути это десять
    обращений на карту, то есть плюс восемь микросекунд к вердикту, который
    считается миллисекундами.

    Интерфейс намеренно повторяет словарь (`get`, `in`, `len`): обращения к
    таблице есть и в коде на боевой машине, которого нет в этом репозитории.
    """

    __slots__ = ("_key", "_val", "_scalar")
    _SHIFT = 20                      # id героя < 2^20; проверяется при сборке

    def __init__(self, rows: np.ndarray, nkey: int = 2,
                 scalar: bool = False) -> None:
        a = rows[:, 0].astype(np.int64)
        h = rows[:, 1].astype(np.int64)
        if len(rows) and (h.min() < 0 or h.max() >= (1 << self._SHIFT)
                          or a.min() < 0):
            raise ValueError(
                f"ключи не влезают в упаковку: аккаунты [{a.min()}, {a.max()}], "
                f"герои [{h.min()}, {h.max()}] при сдвиге {self._SHIFT}")
        k = (a << self._SHIFT) | h
        # ключи уникальны, устойчивость не нужна и стоит вдвое дороже
        order = np.argsort(k)
        self._key = np.ascontiguousarray(k[order])
        self._val = np.ascontiguousarray(rows[order, nkey:])
        self._scalar = scalar

    def _index(self, key) -> int:
        a, h = key
        k = (int(a) << self._SHIFT) | int(h)
        i = int(np.searchsorted(self._key, k))
        return i if i < len(self._key) and int(self._key[i]) == k else -1

    def get(self, key, default=None):
        i = self._index(key)
        if i < 0:
            return default
        return float(self._val[i, 0]) if self._scalar else self._val[i]

    def __contains__(self, key) -> bool:
        return self._index(key) >= 0

    def __len__(self) -> int:
        return len(self._key)


class PackedTable:
    """Таблица «кортеж целых → строка значений» на отсортированных массивах.

    ЗАЧЕМ. После чистки `PairTable`/`AccTable` в модели остались словари помельче,
    но их суммарно 1 452 177 записей при 50 МБ полезных данных, и стоят они около
    240 МБ: `vs_pos` (374 250), `h2h` (362 112), `h2h_org` (352 193), `team_merge`
    (167 094), `syn_pos` (150 525) и две плоские таблицы героев. У каждой записи
    объект-кортеж ключа, кортеж значения и слот словаря — данных на порядок
    меньше, чем обёртки вокруг них.

    Ключ пакуется в одно целое по заданной ширине полей. Ширина проверяется на
    сборке: молча склеить две разные пары в один ключ страшнее, чем упасть.
    Замеренные диапазоны боевого артефакта — герои 1..155, позиции 1..5,
    команды и организации 2..10 219 690, отрицательных нет.

    `nested=True` — для `syn_pos`, где ключ приходит как `((герой, позиция),
    (герой, позиция))`, а не плоской четвёркой.
    """

    __slots__ = ("_key", "_val", "_bits", "_scalar", "_nested", "_cast")

    def __init__(self, rows: np.ndarray, bits: Sequence[int],
                 scalar: bool = False, nested: bool = False,
                 cast=float) -> None:
        nkey = len(bits)
        if sum(bits) > 62:
            raise ValueError(f"упаковка {bits} не влезает в int64")
        cols = [rows[:, i].astype(np.int64) for i in range(nkey)]
        for i, (c, b) in enumerate(zip(cols, bits)):
            if len(rows) and (c.min() < 0 or c.max() >= (1 << b)):
                raise ValueError(
                    f"поле {i} не влезает в упаковку: [{c.min()}, {c.max()}] "
                    f"при {b} битах")
        k = np.zeros(len(rows), dtype=np.int64)
        for c, b in zip(cols, bits):
            k = (k << b) | c
        order = np.argsort(k)             # ключи уникальны, устойчивость не нужна
        self._key = np.ascontiguousarray(k[order])
        self._val = np.ascontiguousarray(rows[order, nkey:])
        self._bits = tuple(bits)
        self._scalar = scalar
        self._nested = nested
        # `cast` нужен для `team_merge`: там значение — идентификатор организации,
        # и float на его месте поехал бы дальше как ключ другой таблицы.
        self._cast = cast

    def _pack(self, key) -> int:
        if self._nested:
            (a, b), (c, d) = key
            parts = (a, b, c, d)
        elif len(self._bits) == 1:
            parts = (key,)
        else:
            parts = key
        k = 0
        for p, b in zip(parts, self._bits):
            k = (k << b) | int(p)
        return k

    def _index(self, key) -> int:
        k = self._pack(key)
        i = int(np.searchsorted(self._key, k))
        return i if i < len(self._key) and int(self._key[i]) == k else -1

    def get(self, key, default=None):
        i = self._index(key)
        if i < 0:
            return default
        return self._cast(self._val[i, 0]) if self._scalar else self._val[i]

    def __contains__(self, key) -> bool:
        return self._index(key) >= 0

    def __len__(self) -> int:
        return len(self._key)


class AccTable:
    """Таблица «аккаунт → строка значений» на отсортированных массивах.

    То же, что `PairTable`, но ключ один. Словарём на 1 553 030 аккаунтов это
    стоило 320 МБ сверх самих данных, и ещё 225 МБ держал живым исходный массив,
    потому что значения были видами в него.
    """

    __slots__ = ("_key", "_val")

    def __init__(self, rows: np.ndarray, nkey: int = 1) -> None:
        k = rows[:, 0].astype(np.int64)
        # ключи уникальны, устойчивость не нужна и стоит вдвое дороже
        order = np.argsort(k)
        self._key = np.ascontiguousarray(k[order])
        self._val = np.ascontiguousarray(rows[order, nkey:])

    def _index(self, key) -> int:
        k = int(key)
        i = int(np.searchsorted(self._key, k))
        return i if i < len(self._key) and int(self._key[i]) == k else -1

    def get(self, key, default=None):
        i = self._index(key)
        return self._val[i] if i >= 0 else default

    def __getitem__(self, key):
        i = self._index(key)
        if i < 0:
            raise KeyError(key)
        return self._val[i]

    def __contains__(self, key) -> bool:
        return self._index(key) >= 0

    def __len__(self) -> int:
        return len(self._key)


class OrgRosters:
    """Составы организаций и обратный указатель «аккаунт → её организации».

    Словарями это стоило 296 МБ при 7 МБ данных: 151 389 замороженных множеств
    по пять элементов плюс обратный словарь из ~757 тысяч множеств. Здесь два
    отсортированных массива на прямую сторону и два на обратную.
    """

    __slots__ = ("_org", "_members", "_acc", "_acc_org")

    def __init__(self, rows: np.ndarray) -> None:
        if rows is None or len(rows) == 0:
            self._org = np.zeros(0, dtype=np.int64)
            self._members = np.zeros((0, 0), dtype=np.int64)
            self._acc = np.zeros(0, dtype=np.int64)
            self._acc_org = np.zeros(0, dtype=np.int64)
            return
        org = rows[:, 0].astype(np.int64)
        mem = rows[:, 1:].astype(np.int64)
        order = np.argsort(org, kind="stable")
        self._org = np.ascontiguousarray(org[order])
        self._members = np.ascontiguousarray(mem[order])
        flat_a = mem.reshape(-1)
        flat_o = np.repeat(org, mem.shape[1])
        keep = flat_a > 0
        fa, fo = flat_a[keep], flat_o[keep]
        p = np.argsort(fa, kind="stable")
        self._acc = np.ascontiguousarray(fa[p])
        self._acc_org = np.ascontiguousarray(fo[p])

    def orgs_of(self, account: int):
        """Организации, где встречается этот аккаунт; порядок — по возрастанию id."""
        a = int(account)
        lo = int(np.searchsorted(self._acc, a, side="left"))
        hi = int(np.searchsorted(self._acc, a, side="right"))
        return self._acc_org[lo:hi]

    def overlap(self, org: int, members: set) -> int:
        """Сколько из пяти слотов организации есть в этом составе."""
        o = int(org)
        i = int(np.searchsorted(self._org, o))
        if i >= len(self._org) or int(self._org[i]) != o:
            return 0
        return sum(1 for x in self._members[i] if int(x) in members)

    def __contains__(self, org) -> bool:
        o = int(org)
        i = int(np.searchsorted(self._org, o))
        return i < len(self._org) and int(self._org[i]) == o

    def __len__(self) -> int:
        return len(self._org)


@dataclass
class Branch:
    """Веса одной ветки лестницы: свои колонки, своя нормировка, свой сдвиг.

    Ветка обучается на том наборе колонок, который в бою действительно бывает
    доступен, поэтому её уверенность честна: недостающих колонок она никогда не
    видела и подставлять им нули не приходится.
    """

    cols: list[str]
    mu: np.ndarray
    sd: np.ndarray
    coef: np.ndarray
    intercept: float


def pack_branches(branches: dict, features: Sequence[str]) -> dict:
    """Ветки → плоские массивы для `np.savez`.

    Рваные данные (у веток разное число колонок) нельзя хранить object-массивом:
    `PrematchModel.__init__` читает артефакт без `allow_pickle`, и такой ключ
    уронил бы загрузку прямо в бою. Поэтому пишутся длины и один плоский массив
    на величину.

    Колонки хранятся ИМЕНАМИ, а не индексами в `features`. Индексы работали,
    пока ветка была подмножеством боевых 35; короткие ветки получили добавочные
    колонки (рейтинг организации), которых в `feature_names` нет вовсе, и по
    индексу их не адресовать. Имена заодно снимают зависимость от порядка.
    `features` остаётся в сигнатуре для проверки: каждая колонка ветки обязана
    быть либо боевым признаком, либо известной добавкой.
    """
    known = set(features) | set(_C.EXTRA_REQUIRES)
    for name, b in branches.items():
        bad = [c for c in b.cols if c not in known]
        if bad:
            raise ValueError(f"ветка {name!r}: колонки {bad} не боевые признаки "
                             f"и не известные добавки")
    names, lens, cols, mu, sd, coef, itc = [], [], [], [], [], [], []
    for name, b in branches.items():
        names.append(name)
        lens.append(len(b.cols))
        cols.extend(str(c) for c in b.cols)
        mu.extend(float(x) for x in b.mu)
        sd.extend(float(x) for x in b.sd)
        coef.extend(float(x) for x in b.coef)
        itc.append(float(b.intercept))
    return {
        "branch_names": np.array(names, dtype="<U32"),
        "branch_lens": np.array(lens, dtype=np.int64),
        "branch_cols": np.array(cols, dtype="<U40"),
        "branch_mu": np.array(mu, dtype=np.float64),
        "branch_sd": np.array(sd, dtype=np.float64),
        "branch_coef": np.array(coef, dtype=np.float64),
        "branch_intercept": np.array(itc, dtype=np.float64),
    }


def lan_winrate(confidence: float) -> float:
    """Фактический винрейт на НАСТОЯЩИХ офлайн-турнирах для этой уверенности.

    Возвращает винрейт самого высокого порога, который уверенность проходит.
    Выше 85% значение не растёт: на 111 картах хвоста винрейт падает до 83.8%,
    и экстраполировать туда нечего.
    """
    wr = LAN_THRESHOLDS[0][2]
    for thr, _share, val, _n, _k in LAN_THRESHOLDS:
        if confidence >= thr:
            wr = val
    return wr


def veto_error_rate(confidence: float) -> tuple[float, float]:
    """Для вето: как часто оно ошибётся и какой кэф нужен ставке ПРОТИВ модели.

    Вето «не отправлять сигнал против модели» ошибается ровно в (1 - винрейт)
    случаев. Ставка против уверенной модели окупается только при коэффициенте
    выше 1/(1-винрейт): на пороге 60% это 3.45, на 70% — 4.48, на 80% — 5.88.
    Медианная цена андердога у нас 2.35 (E-76), то есть заметно ниже.
    """
    wr = lan_winrate(confidence)
    return 1.0 - wr, 1.0 / max(1.0 - wr, 1e-9)


class PrematchModel:
    def __init__(self, path: str | os.PathLike[str] = ARTIFACT_PATH) -> None:
        z = np.load(path)
        self.snapshot_ts = int(z["snapshot_ts"][0])
        self.mu, self.sd, self.coef, self.intercept = z["mu"], z["sd"], z["coef"], z["intercept"]
        # E-177: без колонок 15..17 (imp_recent10/imp30_resid/lh30_resid) `_acc_side`
        # молча подставляет A[:,6]/A[:,10] вместо них (см. строки 869-871) — та же
        # величина копируется под чужим именем, масштаб расходится в 100 раз.
        # Ночная цепочка (`scripts/run/rebuild_prematch_snapshot.sh`) уже требует
        # >= 19 колонок; здесь та же граница ловит ручные/старые артефакты при
        # самой загрузке, а не молча портит скор.
        _acc_raw = z["accounts"]
        if _acc_raw.shape[1] < 19:
            raise ValueError(
                f"в accounts {_acc_raw.shape[1]} колонок, ожидалось >= 19 "
                f"(артефакт устарел — собран до E-177, imp_recent10/imp30_resid/"
                f"lh30_resid молча получат чужую колонку)")
        # 1.55 млн аккаунтов: словарём это 320 МБ сверх данных
        self.acc = AccTable(_acc_raw)
        # 6.78 млн ячеек: словарём это стоило бы 2 005 МБ при 311 МБ данных
        self.acc_hero = PairTable(z["acc_hero"])
        # 2.95 млн ячеек (аккаунт, позиция) со скалярным значением
        self.acc_pos = PairTable(z["acc_pos"], scalar=True)
        self.hero_wr30 = {int(r[0]): r[1] for r in z["hero_wr30"]}
        self.vs = PackedTable(z["vs_pairs"], (8, 8))
        self.h2h = PackedTable(z["h2h"], (31, 31), scalar=True)
        self.hero_farm = {int(r[0]): r[1] for r in z["hero_farm"]} if "hero_farm" in z else {}
        self.ctx_mu = z["ctx_mu"] if "ctx_mu" in z else None
        self.ctx_sd = z["ctx_sd"] if "ctx_sd" in z else None
        # Набор признаков берётся ИЗ АРТЕФАКТА, а не из константы: иначе при
        # смене артефакта веса молча встанут не на свои места (цена ошибки в
        # одной колонке измерена в E-166 — 0.116 AUC).
        self.features = ([str(x) for x in z["feature_names"]]
                         if "feature_names" in z else list(FEATURES))
        # Ветки лестницы. Артефакт БЕЗ них — прежнее поведение: одна модель на
        # все колонки и отказ при неполном входе. На боевой машине лежит именно
        # такой, и он обязан продолжать работать до доставки нового.
        self.branches: dict = {}
        if "branch_names" in z:
            lens = z["branch_lens"].astype(int)
            raw = z["branch_cols"]
            # Два формата. Старый хранил индексы в `feature_names` — такой
            # артефакт лежит на боевой машине с 20.08 10:40, и читатель обязан
            # его понимать. Новый хранит имена: короткие ветки могут иметь
            # колонки, которых в `feature_names` нет вовсе.
            if raw.dtype.kind in "iu":
                cols = [self.features[int(j)] for j in raw]
            else:
                cols = [str(x) for x in raw]
            mu, sd, coef = z["branch_mu"], z["branch_sd"], z["branch_coef"]
            if int(lens.sum()) != len(cols):
                raise ValueError(
                    f"ветки артефакта повреждены: сумма длин {int(lens.sum())}, "
                    f"а колонок записано {len(cols)}")
            off = 0
            for i, nm in enumerate(z["branch_names"]):
                n = int(lens[i])
                self.branches[str(nm)] = Branch(
                    cols=list(cols[off:off + n]),
                    mu=mu[off:off + n], sd=sd[off:off + n],
                    coef=coef[off:off + n],
                    intercept=float(z["branch_intercept"][i]))
                off += n
        # Калибровка «уверенность → винрейт» по веткам. Полосы, которых здесь
        # нет, винрейта не получают вовсе: наследовать таблицу полной ветки
        # опасно в одну сторону — короткая ветка слабее, и наследование
        # продавало бы завышенный винрейт, то есть заниженный нужный кэф.
        # Рейтинг организаций для коротких веток: там, где снимок не знает
        # игроков, это единственная величина о силе сторон помимо ростерного
        # Hybrid. Ключ — организация после склейки `team_merge`.
        self.org_rating: dict = {}
        if "org_rating" in z:
            self.org_rating = {int(r[0]): (float(r[1]), float(r[2]))
                               for r in z["org_rating"]}
        self.cal: dict = {}
        if "cal_branch" in z:
            for i, nm in enumerate(z["cal_branch"]):
                self.cal.setdefault(str(nm), []).append(
                    (int(z["cal_lo"][i]), int(z["cal_hi"][i]),
                     float(z["cal_wr"][i]), int(z["cal_n"][i])))
        # Состояние под колонки E-168. Отклонения урона/нетворса лежат ДОПОЛНИТЕЛЬНЫМИ
        # колонками в `accounts` и `acc_hero` (см. `finalize_artifact.py`): отдельные
        # словари на 1.7 млн ячеек стоили +424 МБ RSS.
        # Драфтовые ячейки — на массивах, а не словарями: вместе с h2h и склейкой
        # организаций это 1 452 177 записей при 50 МБ данных, то есть около 240 МБ
        # одних обёрток. Ширина полей взята с запасом от замеренных диапазонов
        # боевого артефакта: герои 1..155 (8 бит), позиции 1..5 (3 бита).
        _HP = (8, 3, 8, 3)
        self.vs_pos = (PackedTable(z["vs_pos"], _HP)
                       if "vs_pos" in z else {})
        self.vs_flat = (PackedTable(z["vs_flat"], (8, 8))
                        if "vs_flat" in z else {})
        self.syn_pos = (PackedTable(z["syn_pos"], _HP, nested=True)
                        if "syn_pos" in z else {})
        self.syn_flat = (PackedTable(z["syn_flat"], (8, 8))
                         if "syn_flat" in z else {})

        # Идентичность ОРГАНИЗАЦИИ, а не тега: 64 710 team_id схлопнуты в 57 608
        # организаций склейкой по составу (>=4 из 5). Нужно потому, что при
        # ребрендинге team_id новый и история личных встреч обнулялась бы —
        # Iron Wing = 1win = Tundra с составом Pure/bzm/33/Ari/Whitemon.
        # 31 бит на поле: замеренный максимум id — 10 219 690, запас двухсоткратный.
        self.team_merge = (PackedTable(z["team_merge"], (31,), scalar=True, cast=int)
                           if "team_merge" in z else {})
        self.h2h_org = (PackedTable(z["h2h_org"], (31, 31), scalar=True)
                        if "h2h_org" in z else {})
        self.org_roster = OrgRosters(z["org_roster"] if "org_roster" in z else None)

    def _draft_cells(self, rh: list[tuple[int, int]], dh: list[tuple[int, int]],
                     now: int) -> tuple[float, float]:
        """(cp_lane, syn_pos_mean) из позиционных про-ячеек на момент `now`."""
        lam = math.log(2) / (DRAFT_HL_DAYS * 86400.0)

        def dec(cell):
            if cell is None:
                return 0.0, 0.0
            w, g, t0 = cell
            f = math.exp(-lam * max(now - float(t0), 0.0)) if g > 0 else 0.0
            return w * f, g * f

        def val(cell, prior_cell, k=8.0):
            w, g = dec(cell)
            pw, pg = dec(prior_cell)
            prior = (pw + 5.0) / (pg + 10.0) if pg > 0 else 0.5
            return (w + k * prior) / (g + k)

        lane_v = []
        for (h1, p1) in rh:
            for (h2, p2) in dh:
                if (p1, p2) in LANES:
                    lane_v.append(val(self.vs_pos.get((h1, p1, h2, p2)),
                                      self.vs_flat.get((h1, h2))))
        syn_v = []
        for side, own in ((rh, True), (dh, False)):
            for a, b in itertools.combinations(side, 2):
                key = (min(a, b), max(a, b))
                v = val(self.syn_pos.get(key),
                        self.syn_flat.get((min(a[0], b[0]), max(a[0], b[0]))))
                syn_v.append(v if own else 1.0 - v)
        cp_lane = (float(np.mean(lane_v)) - 0.5) if lane_v else 0.0
        syn_mean = (float(np.mean(syn_v)) - 0.5) if syn_v else 0.0
        return cp_lane, syn_mean
    def resolve_org(self, team_id: int, accounts: Sequence[int]) -> int:
        """Организация по СОСТАВУ, а тег — только запасной вариант.

        Порядок именно такой, и он куплен проверкой. Состав Pure/bzm/33/Ari/
        Whitemon опознаётся как организация 8121295, склеившая пять team_id,
        включая Tundra (8291895) и «1w» (10182357). А алиас `1win` в
        `id_to_names` указывает на team_id 9255039, чей состав пересекается с
        этой пятёркой на 0 из 5 — то есть справочник имён ведёт не туда.
        Пересечение 4 из 5 живых аккаунтов — свидетельство сильнее тега.
        """
        tid = int(team_id)
        members = {int(a) for a in accounts if int(a) > 0}
        if len(members) < 4:
            return self.team_merge.get(tid, tid if tid > 0 else -1)
        # ПОРЯДОК ОБХОДА ЗАФИКСИРОВАН, И НИЧЬЯ РАЗРЕШАЕТСЯ ЯВНО. Раньше состав
        # обходился как множество, а при равном пересечении побеждал первый
        # попавшийся — то есть исход зависел от хеш-порядка Python и мог
        # отличаться между запусками интерпретатора. Ничьи не редкость: у 4.9%
        # сторон несколько организаций дают одинаковое пересечение, потому что
        # 1 084 набора состава принадлежат сразу нескольким team_id (склейка
        # `team_merge` их не схлопнула). При равенстве берём МЕНЬШИЙ id —
        # правило произвольное, но устойчивое и одинаковое везде.
        best, best_ov = -1, 0
        seen: set[int] = set()
        for a in sorted(members):
            for org_np in self.org_roster.orgs_of(a):
                org = int(org_np)
                if org in seen:
                    continue
                seen.add(org)
                ov = self.org_roster.overlap(org, members)
                if ov > best_ov or (ov == best_ov and best_ov > 0 and org < best):
                    best, best_ov = org, ov
        if best_ov >= 4:
            return best
        return self.team_merge.get(tid, tid if tid > 0 else -1)

    def _check_positions(self, accs: Sequence[int], miss: list[str], notes: list[str]) -> None:
        """Ловит сломанную разметку позиций.

        Модель ждёт героев и аккаунтов В ПОРЯДКЕ ПОЗИЦИЙ 1..5. Если резолвер
        позиций ошибся, признаки `pos_games` и ячейки (аккаунт, герой) молча
        поедут — это ровно тот случай, когда модель не падает, а врёт.

        Порог откалиброван на корпусе: у игроков с 20+ матчами «невиданная для
        себя позиция» встречается на 5.5% карт, две сразу — на 0.3%, три и
        больше — на 0.0%. Поэтому три конфликта = разметка сломана.
        """
        hard, soft = [], []
        for i, a in enumerate(accs):
            a = int(a)
            pos = (i % 5) + 1
            row = self.acc.get(a)
            if row is None or row[1] < 20:            # мало матчей — судить не по чему
                continue
            by = {p: self.acc_pos.get((a, p), 0.0) for p in range(1, 6)}
            tot = sum(by.values()) or 1.0
            share, main = by[pos] / tot, max(by.values()) / tot
            best = max(by, key=by.get)
            if share < 0.05 and main > 0.50:
                hard.append((a, pos, best))
            if share < 0.05 and main > 0.40:
                soft.append((a, pos, best))
        # калибровка на 1 500 свежих картах: жёсткий порог даёт 0.1% ложных
        # отказов и ловит 42.7% полной перестановки позиций; мягкий — 2.5% и
        # 71.2%. Поэтому жёсткий отказывает, мягкий только предупреждает.
        if len(hard) >= 3:
            miss.append("разметка позиций противоречит истории у "
                        f"{len(hard)} слотов (аккаунт, назначено, обычная): {hard}")
        elif len(soft) >= 2:
            notes.append(f"подозрительные позиции у {len(soft)} слотов: {soft}")

    def _org_rating_cols(self, org_r: int, org_d: int) -> dict:
        """Разность рейтингов организаций и ожидаемая из неё вероятность.

        Обе величины — нули, если хоть одна организация не опознана или её нет
        в снимке. Это НЕ дефолт-из-воздуха: обучающая колонка занулялась ровно
        там же (`build_org_rating.py`), то есть модель видела ту же дырявость.
        """
        a = self.org_rating.get(int(org_r)) if org_r > 0 else None
        b = self.org_rating.get(int(org_d)) if org_d > 0 else None
        if a is None or b is None:
            return {"org_rating_diff": 0.0, "org_rating_exp": 0.0}
        (ra, _da), (rb, db) = a, b
        q = math.log(10.0) / 400.0
        g = 1.0 / math.sqrt(1.0 + 3.0 * q * q * db * db / (math.pi ** 2))
        return {"org_rating_diff": (ra - rb) / 400.0,
                "org_rating_exp": 1.0 / (1.0 + 10 ** (-g * (ra - rb) / 400.0)) - 0.5}

    def branch_winrate(self, branch: str, confidence: float) -> tuple:
        """Ожидаемый винрейт для этой уверенности НА ЭТОЙ ВЕТКЕ и откуда он взят.

        Возвращает `nan`, если полоса ветки не откалибрована. Это не сбой: по
        такой ветке просто нельзя выставлять автоматическую ставку — вероятность
        показать можно, а обещать винрейт не из чего. Дефолтов здесь нет по той
        же причине, что и во всём модуле.

        Полная ветка пользуется действующей `LAN_ODDS_GRID`: она снята на
        честных предсказаниях шести forward-окон (E-142), а не на снимке,
        который стареет на месяцы.
        """
        if branch == "full" or branch not in self.cal:
            return lan_winrate(confidence), "общая таблица"
        pct = confidence * 100.0
        for lo, hi, wr, n in self.cal[branch]:
            if lo <= pct < hi:
                return wr, f"своя таблица ветки ({n} карт в полосе)"
        return float("nan"), "полоса ветки не откалибрована"

    def _hero_side(self, h5: Sequence[int]) -> dict[str, float]:
        """Величины стороны, ключуемые ГЕРОЕМ: живут без таблицы аккаунтов.

        Обе выглядят игроцкими, но `hero_wr30` — это винрейт ГЕРОЕВ в про за
        30 дней, а `hero_farm` — фарм-зависимость героя. Поэтому ветка без
        аккаунтов их сохраняет (`prematch_components.REQUIRES`).
        """
        return {
            "farm_dep": float(np.mean([self.hero_farm.get(int(h), 0.0) for h in h5])),
            "wr30": float(np.mean([self.hero_wr30[int(h)] for h in h5])),
        }

    def _acc_side(self, a5: Sequence[int], h5: Sequence[int],
                  fill: dict, notes: list,
                  extra: Optional[dict] = None) -> dict[str, float]:
        """Величины стороны, ключуемые АККАУНТОМ и ячейкой (аккаунт, герой).

        Вынесено из `score` без единой правки формул: сдвиг масштаба здесь
        стоит дорого и не виден снаружи — E-166 обошёлся в 0.116 AUC на
        забытом делении на 100. Метод зовётся, только когда снимок знает всех
        десятерых: иначе первая же строка упала бы по KeyError, и ровно из-за
        этого ветка без аккаунтов была недостижима.

        `extra` — дозапись счётчиков после среза снимка. Накладываются только
        `games` / `hero_games` / `pos_games`: скользящие окна из дельты не
        воспроизводятся. Массивы снимка не мутируются.
        """
        extra = extra or {}
        A = np.array([self.acc[int(a)] for a in a5])
        if extra:
            A = np.array(A, copy=True)
            for i, a in enumerate(a5):
                add = extra.get(int(a))
                if add:
                    A[i, 1] = A[i, 1] + int(add.get("games") or 0)
        cells = [self.acc_hero.get((int(a), int(h))) for a, h in zip(a5, h5)]
        known = [c for c in cells if c is not None]
        # Какие ИМЕННО слоты остались без ячейки (аккаунт, герой). Добавлено на
        # боевой машине: «вход заполнен на 80%» не отвечает на вопрос, у кого
        # именно нет истории на герое, а без этого не отличить новичка в составе
        # от сломанной разметки позиций.
        side_tag = "dire" if fill.get("_second") else "rad"
        for _p, _c in enumerate(cells, 1):
            if _c is None:
                fill["miss"].append(f"{side_tag}_pos{_p}")
        fill["_second"] = True
        fill["cells"] += len(known)
        fill["pos"] += sum(1 for p, a in enumerate(a5, 1)
                           if (int(a), p) in self.acc_pos)
        if not known:
            notes.append("ни одной ячейки (аккаунт, герой) — hero_* нейтральны")
        hg = []
        for a, h, c in zip(a5, h5, cells):
            base = float(c[0]) if c is not None else 0.0
            add = extra.get(int(a)) if extra else None
            hero_map = (add or {}).get("hero_games") or {}
            hg.append(base + int(hero_map.get(int(h), 0) or 0))
        # E-177: усреднять надо по ВСЕМ пяти слотам с нулём за отсутствующую
        # ячейку — так собиралась обучающая колонка (`ideas_batch1`:
        # `gr.append(... if c_ and ca else 0.0)`). Среднее только по
        # существующим ячейкам давало другую величину: sd 1.48 против
        # обучающей при корреляции 0.85.
        gpm_rel = [c[1] if c is not None else 0.0 for c in cells]
        lh_rel = [c[2] if c is not None else 0.0 for c in cells]

        def _pos_games(a: int, p: int) -> float:
            base = float(self.acc_pos.get((int(a), p), 0.0))
            add = extra.get(int(a)) if extra else None
            pos_map = (add or {}).get("pos_games") or {}
            return base + int(pos_map.get(p, 0) or 0)

        return {
            "elo": A[:, 0].mean(), "games": A[:, 1].mean(), "opp_elo": A[:, 2].mean(),
            "hero_pool": A[:, 3].mean(), "form": A[:, 4].mean(), "imp50": A[:, 5].mean(),
            "imp30": A[:, 6].mean(), "gpm_rel_pos": A[:, 7].mean(),
            "imp_rel_pos": A[:, 8].mean(), "gpm_ewma": A[:, 9].mean(),
            "lh30": A[:, 10].mean(),
            "lvl_rel_pos": A[:, 11].mean() if A.shape[1] > 11 else 0.0,
            "kda_player": A[:, 12].mean() if A.shape[1] > 12 else 0.0,
            # E-177: три величины, которые модель учила, а снимок не отдавал.
            # Пока их в артефакте нет — старое поведение, чтобы прежний
            # артефакт продолжал работать.
            "imp_recent10": A[:, 15].mean() if A.shape[1] > 15 else A[:, 6].mean(),
            "imp30_resid": A[:, 16].mean() if A.shape[1] > 16 else A[:, 6].mean(),
            "lh30_resid": A[:, 17].mean() if A.shape[1] > 17 else A[:, 10].mean(),
            "hero_games": float(np.mean([math.log1p(max(x, 0)) for x in hg])),
            "hero_gpm_rel": float(np.mean(gpm_rel)), "lh_rel_hero": float(np.mean(lh_rel)),
            # Отклонения от норм позиции и героя — сырые (не поминутные)
            # величины, как в `ideas_batch5.build`; масштаб /1000 ниже.
            # Среднее берётся по ВСЕМ пяти слотам с нулём за отсутствующую
            # ячейку — так же, как в обучении (там ноль подставлялся ячейке
            # без истории), а не по `known`, как у hero_gpm_rel.
            "a_hdmg_rel_pos": A[:, 13].mean() if A.shape[1] > 13 else 0.0,
            "a_nw_rel_pos": A[:, 14].mean() if A.shape[1] > 14 else 0.0,
            "a_hdmg_rel_hero": float(np.mean(
                [c[3] if (c is not None and len(c) > 3) else 0.0 for c in cells])),
            "pos_games": float(np.mean([math.log1p(_pos_games(a, p))
                                        for p, a in enumerate(a5, 1)])),
        }

    def score(self, *, radiant_accounts: Sequence[int], dire_accounts: Sequence[int],
              radiant_heroes: Sequence[int], dire_heroes: Sequence[int],
              radiant_team_id: int, dire_team_id: int,
              draft_logit: Optional[float] = None,
              hybrid_strength: Optional[float] = None,
              strictness: str = "teams",
              now_ts: Optional[int] = None,
              max_age_days: float = 3.0) -> ScoreResult:
        """Вероятность победы радианта. Порядок аккаунтов и героев — позиции 1..5.

        Бросает `MissingData`, если данных не хватает, снимок протух или
        разметка позиций противоречит истории. Дефолты не подставляются.
        """
        if strictness not in STRICTNESS:
            raise ValueError(f"strictness должен быть одним из {STRICTNESS}")
        # Причины делятся на две породы. ЖЁСТКИЕ отменяют вердикт всегда:
        # снимок протух, разметка позиций противоречит истории, вход не той
        # формы, вызывающий сам потребовал строгости, которой данные не отвечают.
        # МЯГКИЕ означают лишь, что под какой-то КЛЮЧ ДАННЫХ данных нет, — и это
        # выбор ветки, а не отказ. `miss` собирается в ПРЕЖНЕМ порядке: артефакт
        # без веток обязан вести себя ровно как раньше.
        miss: list[str] = []
        hard: list[str] = []
        notes: list[str] = []
        if now_ts is not None and max_age_days > 0:
            age = (int(now_ts) - self.snapshot_ts) / 86400.0
            if age > max_age_days:
                # wr30 — окно 30 дней, vs_wr — распад 45 дней; протухший снимок
                # означает «винрейт за 30 дней, закончившихся N дней назад»
                stale = (f"снимок протух: собран {age:.1f} дней назад "
                         f"при пороге {max_age_days:.0f}")
                miss.append(stale)
                hard.append(stale)
        accs = [int(a) for a in radiant_accounts] + [int(a) for a in dire_accounts]
        hers = [int(h) for h in radiant_heroes] + [int(h) for h in dire_heroes]
        if len(accs) != 10 or len(hers) != 10:
            raise MissingData(["ожидались 10 аккаунтов и 10 героев по позициям 1..5"])
        if draft_logit is None:
            miss.append("не передан draft_logit (паблик-модель по героям)")
        if "hybrid_strength" in self.features and hybrid_strength is None:
            # Дефолт здесь означал бы «команды равны по силе» — ровно то враньё,
            # против которого построен весь модуль.
            miss.append("не передан hybrid_strength "
                        "((сила радианта − сила дайра) / 400 по Hybrid-рейтингу)")
        zero = [i + 1 for i, a in enumerate(accs) if a <= 0]
        if zero:
            miss.append(f"нет account_id у слотов {zero}")
        unknown = [a for a in accs if a > 0 and a not in self.acc]
        if unknown:
            miss.append(f"игроки неизвестны снимку: {unknown}")
        no_wr = [h for h in hers if h not in self.hero_wr30]
        if no_wr:
            miss.append(f"нет винрейта за 30 дней у героев: {sorted(set(no_wr))}")
        if not unknown and not zero:
            pos_bad: list[str] = []
            self._check_positions(accs, pos_bad, notes)
            miss.extend(pos_bad)
            hard.extend(pos_bad)
        rt, dt = int(radiant_team_id), int(dire_team_id)
        if strictness in ("teams", "cells", "full") and (rt <= 0 or dt <= 0):
            miss.append("нет id одной из команд")
            hard.append("нет id одной из команд")
        if strictness in ("cells", "full"):
            no_cell = [(a, h) for a, h in zip(accs, hers) if (a, h) not in self.acc_hero]
            if no_cell:
                miss.append(f"игрок ещё не играл на этом герое: {no_cell}")
                hard.append(f"игрок ещё не играл на этом герое: {no_cell}")
            no_pos = [(a, p) for p, a in enumerate(accs, 1) if (a, ((p - 1) % 5) + 1) not in self.acc_pos]
            if no_pos:
                miss.append(f"нет игр на этой позиции: {no_pos}")
                hard.append(f"нет игр на этой позиции: {no_pos}")
        org_r = self.resolve_org(rt, radiant_accounts)
        org_d = self.resolve_org(dt, dire_accounts)
        # Обучение шло на "сырой" командной таблице (ideas_batch2.py:203-209),
        # поэтому она в приоритете; организационная — запасной вариант там, где
        # team_id ещё не встречались (ребрендинг, новый тег), а не наоборот.
        team_key = (min(rt, dt), max(rt, dt)) if rt > 0 and dt > 0 else None
        if team_key is not None and team_key in self.h2h:
            key, h2h_src, swap = team_key, self.h2h, rt > dt
        elif self.h2h_org and org_r > 0 and org_d > 0 and org_r != org_d:
            key = (min(org_r, org_d), max(org_r, org_d))
            h2h_src, swap = self.h2h_org, org_r > org_d
        else:
            key, h2h_src, swap = team_key, self.h2h, rt > dt
        if strictness == "full" and (key is None or key not in h2h_src):
            miss.append("нет истории личных встреч этих команд")
            hard.append("нет истории личных встреч этих команд")
        if hard:
            raise MissingData(miss)
        if not self.branches:
            # Артефакт без веток — прежнее поведение целиком: любая нехватка
            # отменяет вердикт, и считается одна модель на все колонки. Так
            # работает боевая машина, пока ей не доставлен новый артефакт.
            if miss:
                raise MissingData(miss)
            keys = {_C.ACCOUNT, _C.HERO, _C.ROSTER, _C.ORG}
            branch_name = "full"
        else:
            keys = set()
            if not zero and not unknown:
                keys.add(_C.ACCOUNT)
            if draft_logit is not None and not no_wr:
                keys.add(_C.HERO)
            if hybrid_strength is not None:
                keys.add(_C.ROSTER)
            if key is not None and key in h2h_src:
                keys.add(_C.ORG)
            branch_name = _C.pick_branch(keys, have=set(self.branches))
            if branch_name is None:
                # Ни одной обученной ветки под этот набор ключей. Врать нечем.
                raise MissingData(miss or ["нет ни рейтинга ростера, ни драфта"])
        missing_keys = sorted({_C.ACCOUNT, _C.HERO, _C.ROSTER, _C.ORG} - keys)
        if branch_name != "full":
            notes.append(f"ветка «{branch_name}»: нет данных под ключи "
                         f"{', '.join(missing_keys)} — уверенность считается "
                         f"моделью, которая этих колонок не видела")

        # ---- признаки; считаются только те, чьи ключи доступны
        # Счётчики заполненности входа: сколько величин собрано из реальных
        # данных, а сколько заменено нулём. Считаются здесь же, чтобы не
        # расходиться с тем, что действительно попало в признаки.
        fill = {"cells": 0, "pos": 0, "miss": []}
        f: dict[str, float] = {}
        lg1 = lambda x: math.log1p(max(x, 0))

        if _C.HERO in keys:
            rh_, dh_ = self._hero_side(radiant_heroes), self._hero_side(dire_heroes)

            def pair_wr(a_heroes: Sequence[int], b_heroes: Sequence[int]) -> float:
                vv = [(self.vs.get((int(x), int(y)), (0.0, 0.0)))
                      for x in a_heroes for y in b_heroes]
                if not vv:
                    return 0.5
                return float(np.mean([(w + 5.0) / (g + 10.0) for w, g in vv]))

            f["draft_logit"] = float(draft_logit)
            f["wr30"] = rh_["wr30"] - dh_["wr30"]
            f["farm_dep"] = rh_["farm_dep"] - dh_["farm_dep"]
            f["vs_wr"] = (pair_wr(radiant_heroes, dire_heroes)
                          - pair_wr(dire_heroes, radiant_heroes))
            if {"cp_lane", "syn_pos_mean"} & set(self.features):
                now_draft = int(now_ts) if now_ts is not None else self.snapshot_ts
                rh = [(int(h), pp) for pp, h in enumerate(radiant_heroes, 1)]
                dh = [(int(h), pp) for pp, h in enumerate(dire_heroes, 1)]
                cp_lane, syn_mean = self._draft_cells(rh, dh, now_draft)
                f["cp_lane"], f["syn_pos_mean"] = cp_lane, syn_mean

        if _C.ROSTER in keys and "hybrid_strength" in self.features:
            f["hybrid_strength"] = float(hybrid_strength or 0.0)

        if _C.ORG in keys:
            h2h = float(h2h_src.get(key, 0.0)) if key else 0.0
            if key and key not in h2h_src:
                notes.append("организации раньше не встречались — h2h нейтрален")
            if key and swap:
                h2h = -h2h
            f["h2h_resid"] = h2h

        if _C.ACCOUNT in keys:
            extra: dict = {}
            if _DELTA is not None:
                try:
                    extra = _DELTA.extra_for_accounts(accs, self.snapshot_ts)
                except Exception:
                    extra = {}
            r, d = (self._acc_side(radiant_accounts, radiant_heroes, fill, notes, extra),
                    self._acc_side(dire_accounts, dire_heroes, fill, notes, extra))
            f.update({
                "elo": (r["elo"] - d["elo"]) / 400.0,
                "games": lg1(r["games"]) - lg1(d["games"]),
                "hero_games": r["hero_games"] - d["hero_games"],
                "pos_games": r["pos_games"] - d["pos_games"],
                "opp_elo": (r["opp_elo"] - d["opp_elo"]) / 400.0,
                "hero_pool": lg1(r["hero_pool"]) - lg1(d["hero_pool"]),
                "form": r["form"] - d["form"],
                "hero_gpm_rel": (r["hero_gpm_rel"] - d["hero_gpm_rel"]) / 100.0,
                # train (ideas_batch1 i2_imp_recent) делит на 100; без деления
                # боевой AUC −0.116. E-177: окно ДЕСЯТЬ матчей, а не тридцать —
                # раньше сюда шло то же поле, что и в imp30, из-за чего две
                # колонки из 35 были в бою коллинеарны (отношение sd 99.99
                # против 76.04 в обучении).
                "imp_recent": (r["imp_recent10"] - d["imp_recent10"]) / 100.0,
                "gpm_rel_pos": (r["gpm_rel_pos"] - d["gpm_rel_pos"]) / 100.0,
                "imp50": r["imp50"] - d["imp50"],
                "imp_rel_pos": r["imp_rel_pos"] - d["imp_rel_pos"],
                "lh_rel_hero": (r["lh_rel_hero"] - d["lh_rel_hero"]) / 100.0,
                "gpm_ewma": (r["gpm_ewma"] - d["gpm_ewma"]) / 100.0,
                # E-177: обучающие v_lh30 и v_imp30 — ОСТАТКИ против нормы
                # позиции (`ideas_batch5b`: r = st[k] - pn[k]), а не сырые
                "lh30": (r["lh30_resid"] - d["lh30_resid"]) / 100.0,
                "imp30": r["imp30_resid"] - d["imp30_resid"],
                "lvl_rel_pos": r["lvl_rel_pos"] - d["lvl_rel_pos"],
                "kda_player": r["kda_player"] - d["kda_player"],
            })
            new6acc = {
                # scale "raw1k" в `ideas_batch5.NEW`
                "a_hdmg_rel_pos": (r["a_hdmg_rel_pos"] - d["a_hdmg_rel_pos"]) / 1000.0,
                "a_hdmg_rel_hero": (r["a_hdmg_rel_hero"] - d["a_hdmg_rel_hero"]) / 1000.0,
                "a_nw_rel_pos": (r["a_nw_rel_pos"] - d["a_nw_rel_pos"]) / 1000.0,
            }
            # в отчёт попадает только то, что артефакт действительно использует
            f.update({k: v for k, v in new6acc.items() if k in self.features})
            if self.ctx_mu is not None:
                # контекст матча как МНОЖИТЕЛЬ антисимметричных признаков:
                # признак уровня матча в разностной схеме тождественно нулевой
                # (E-95 §3), а множителем законен. Нормировка берётся из
                # артефакта — иначе веса окажутся не на своих местах.
                ctx = np.array([abs(f["elo"]), abs(f["games"])])
                zc = (ctx - self.ctx_mu) / self.ctx_sd
                for j, cname in enumerate(("elo_gap", "games_exp")):
                    for k in INTER_KEYS:
                        if k in f:      # без драфта интеракции драфта не бывает
                            f[f"{k}_x_{cname}"] = f[k] * float(zc[j])

        parts: dict[str, float] = {}
        if self.branches:
            b = self.branches[branch_name]
            if any(c in _C.EXTRA_REQUIRES for c in b.cols):
                # Пустая таблица при обученных на ней весах — молчаливая
                # деградация: колонки станут нулями, и никто не узнает. Один раз
                # это уже случилось на аудите, где ключ `org_rating` не доехал.
                if not self.org_rating:
                    raise ValueError(
                        f"ветка «{branch_name}» обучена с рейтингом организаций, "
                        "а таблицы `org_rating` в артефакте нет — веса встанут "
                        "на нули")
                f.update(self._org_rating_cols(org_r, org_d))
            miss_cols = [c for c in b.cols if c not in f]
            if miss_cols:
                raise MissingData(
                    [f"ветка «{branch_name}» требует колонок, которых нет: "
                     f"{miss_cols} — разметка ключей разошлась с обучением"])
            x = np.array([f[c] for c in b.cols])
            zb = (x - b.mu) / b.sd
            contrib = zb * b.coef
            for c, w in zip(b.cols, contrib):
                comp = _C.component_of(c)
                parts[comp] = parts.get(comp, 0.0) + float(w)
            p = 1.0 / (1.0 + math.exp(-(float(contrib.sum()) + b.intercept)))
        else:
            x = np.array([f[k] for k in self.features])
            ps = []
            for i in range(len(self.coef)):
                zz = (x - self.mu[i]) / self.sd[i]
                ps.append(1.0 / (1.0 + math.exp(
                    -(float(zz @ self.coef[i]) + float(self.intercept[i])))))
            p = float(np.mean(ps))
        # h2h входит в знаменатель, только если его вообще спрашивали: при
        # строгости `accounts` id команд не передаются (боевой вызов в
        # `win_model_veto._prematch_index`), ключа нет, и записывать это в дыры
        # неверно — заполненность тогда упиралась бы в 20/21 навсегда.
        h2h_asked = key is not None
        h2h_known = 1.0 if (h2h_asked and key in h2h_src) else 0.0
        if _C.ACCOUNT not in keys:
            # Ячеек и игр на позиции на короткой ветке не бывает по построению:
            # считать их заполненность нулём значило бы пугать нулевым покрытием
            # там, где эти величины не спрашивались.
            cov = {"cells": float("nan"), "pos": float("nan"),
                   "h2h": h2h_known if h2h_asked else float("nan"),
                   "missing_cells": [], "filled": 1.0}
            wr, src = self.branch_winrate(branch_name, max(p, 1.0 - p))
            if src != "общая таблица":
                notes.append(f"винрейт: {src}")
            return ScoreResult(p, wr, f, notes, cov, branch=branch_name,
                               missing_keys=missing_keys, parts=parts)
        cov = {"cells": fill["cells"] / 10.0, "pos": fill["pos"] / 10.0,
               "h2h": h2h_known if h2h_asked else float("nan"),
               "missing_cells": list(fill["miss"])}
        cov["filled"] = ((fill["cells"] + fill["pos"] + (h2h_known if h2h_asked else 0.0))
                         / (21.0 if h2h_asked else 20.0))
        if cov["filled"] < 1.0:
            notes.append(
                f"вход заполнен на {cov['filled']:.0%}: ячеек (игрок,герой) "
                f"{fill['cells']}/10, игр на позиции {fill['pos']}/10"
                + (f", личные встречи {'есть' if h2h_known else 'нет'}"
                   if h2h_asked else ""))
        wr, src = self.branch_winrate(branch_name, max(p, 1.0 - p))
        if src != "общая таблица":
            notes.append(f"винрейт: {src}")
        return ScoreResult(p, wr, f, notes, cov, branch=branch_name,
                           missing_keys=missing_keys, parts=parts)


_MODEL: Optional[PrematchModel] = None


def get_model(path: str | os.PathLike[str] = ARTIFACT_PATH) -> PrematchModel:
    global _MODEL
    if _MODEL is None:
        _MODEL = PrematchModel(path)
    return _MODEL

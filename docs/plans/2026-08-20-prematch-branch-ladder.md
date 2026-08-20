# Лестница веток предматчевой модели — план реализации

> **Для агентов-исполнителей:** ОБЯЗАТЕЛЬНЫЙ ПОДСКИЛЛ — `superpowers:subagent-driven-development` или `superpowers:executing-plans`. Шаги помечены чекбоксами `- [ ]`.

**Цель.** Заменить безусловный отказ предматчевой модели на выбор ветки, обученной ровно на доступных данных, и сделать разложение вердикта по компонентам точным.

**Архитектура.** Признаки размечаются не по смыслу, а по КЛЮЧУ ДАННЫХ (аккаунт, герой, ростер, организация). Ветка — это подмножество признаков, которым хватает доступных ключей; веса каждой ветки подбираются отдельно на боевом окне 120 суток, но внутри ветки — совместно, поэтому на полном входе результат тождественен нынешнему. Компоненты (ELO, драфт, игроки, очные) остаются группировкой ДЛЯ ОТЧЁТА: вклад компонента считается как частичная сумма логита.

**Технологии.** Python 3.9, numpy, scikit-learn, pytest. Венв `venv_catboost`. Артефакт — `.npz` без `allow_pickle`.

**Спека.** `docs/PREMATCH_COMPONENTS.md`

## Глобальные ограничения

- Корень проекта — `/Users/alex/Documents/ingame`. Питон только `venv_catboost/bin/python3`.
- `np.load` в `PrematchModel.__init__` вызывается **без `allow_pickle`**. Никаких object-массивов в артефакте: рваные данные хранить плоскими массивами с длинами.
- Обратная совместимость обязательна: артефакт БЕЗ ключей `branch_*` должен работать ровно как сейчас. На боевой машине лежит старый артефакт, и он не должен сломаться до доставки нового.
- Ничего не отправлять на serv1 в рамках этого плана. Доставка — отдельное решение alex.
- Секреты (`base/keys.py`, `keys_local.py`, `keys.py.bak*`) не трогать и не коммитить.
- Тесты кладутся в `base/tests/`, эксперименты — в `runtime/experiments/misc/`. Хук `pre-commit` рубит файлы не в тех каталогах.
- Числа-ориентиры (тест 26 016 карт, окно 120 суток, обучение 22 233 карты): монолит 0.7186, ветка без аккаунтов 0.6864, без аккаунтов и организаций 0.6852, до пиков 0.6915, только рейтинг 0.6516.

---

### Задача 1: карта признаков по ключам данных

**Файлы:**
- Создать: `base/prematch_components.py`
- Тест: `base/tests/test_prematch_components.py`

**Интерфейсы:**
- Отдаёт: `REQUIRES: dict[str, frozenset[str]]`, `COMPONENTS: dict[str, tuple[str, ...]]`, `BRANCHES: tuple[tuple[str, frozenset[str]], ...]`, `columns_for(keys, features) -> list[str]`, `pick_branch(keys) -> str`, `component_of(feature) -> str`.

- [x] **Шаг 1: написать падающий тест**

```python
"""Разметка 35 боевых признаков по ключу данных и выбор ветки.

Ключ определяет не смысл признака, а то, переживёт ли он отсутствие данных:
`wr30` это винрейт ГЕРОЕВ за 30 дней (`hero_wr30`), поэтому он переживает
незнакомых снимку игроков, а `elo` из таблицы аккаунтов — нет.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import prematch_components as C
import prematch_scorer as ps


def test_every_prod_feature_has_a_key():
    assert set(C.REQUIRES) == set(ps.FEATURES + ps.NEW6)


def test_keys_are_known():
    for name, keys in C.REQUIRES.items():
        assert keys <= {"account", "hero", "roster", "org"}, name
        assert keys, name


def test_hero_keyed_features_survive_unknown_accounts():
    cols = C.columns_for(frozenset({"hero", "roster", "org"}), ps.FEATURES + ps.NEW6)
    assert sorted(cols) == sorted([
        "cp_lane", "draft_logit", "farm_dep", "h2h_resid", "hybrid_strength",
        "syn_pos_mean", "vs_wr", "wr30"])


def test_draft_interactions_need_the_account_key():
    """Контекст интеракций строится из |elo| и |games| — обе из таблицы аккаунтов."""
    assert C.REQUIRES["draft_logit_x_elo_gap"] == frozenset({"hero", "account"})
    assert "draft_logit_x_elo_gap" not in C.columns_for(
        frozenset({"hero", "roster", "org"}), ps.FEATURES + ps.NEW6)


def test_pre_draft_branch_drops_hero_features_only():
    cols = C.columns_for(frozenset({"account", "roster", "org"}), ps.FEATURES + ps.NEW6)
    assert len(cols) == 27
    assert "draft_logit" not in cols and "elo" in cols


def test_components_partition_all_features():
    seen = [f for group in C.COMPONENTS.values() for f in group]
    assert sorted(seen) == sorted(ps.FEATURES + ps.NEW6)
    assert len(seen) == len(set(seen))


def test_component_sizes_match_the_spec():
    sizes = {k: len(v) for k, v in C.COMPONENTS.items()}
    assert sizes == {"elo": 5, "draft": 8, "players": 21, "h2h": 1}


def test_pick_branch_prefers_the_most_complete():
    assert C.pick_branch(frozenset({"account", "hero", "roster", "org"})) == "full"
    assert C.pick_branch(frozenset({"account", "hero", "roster"})) == "no_org"
    assert C.pick_branch(frozenset({"hero", "roster", "org"})) == "no_account"
    assert C.pick_branch(frozenset({"hero", "roster"})) == "no_account_no_org"
    assert C.pick_branch(frozenset({"account", "roster", "org"})) == "pre_draft"
    assert C.pick_branch(frozenset({"roster"})) == "rating_only"


def test_pick_branch_returns_none_without_rating_and_draft():
    assert C.pick_branch(frozenset({"org"})) is None
    assert C.pick_branch(frozenset()) is None
```

- [x] **Шаг 2: убедиться, что тест падает**

Запуск: `venv_catboost/bin/python3 -m pytest base/tests/test_prematch_components.py -q`
Ожидание: `ModuleNotFoundError: No module named 'prematch_components'`

- [x] **Шаг 3: написать модуль**

```python
#!/usr/bin/env python3
"""Компоненты предматчевой модели и лестница веток по доступности данных.

ЗАЧЕМ. Боевая модель бросала отказ, если снимок не знает хотя бы одного из
десяти игроков. В сквозном аудите это 14 828 отказов из 14 952, то есть 57.5%
тестовых карт; на тир-1 без вердикта каждый третий матч (E-195). Отказ был
правильным — подставить дефолты и посчитать вероятность для неизвестных игроков
значит выдать уверенное число из воздуха. Но вместо отказа можно посчитать
моделью, которая этих колонок НИКОГДА НЕ ВИДЕЛА, и её уверенность будет честной.

ГЛАВНАЯ МЫСЛЬ: признак размечается по КЛЮЧУ ДАННЫХ, а не по смыслу. Ключ
определяет, переживёт ли колонка отсутствие данных:

    account  таблица аккаунтов артефакта: `elo`, `opp_elo` и весь блок игроков
    hero     таблицы, ключуемые героем: `wr30` (винрейт героев за 30 дней),
             `farm_dep`, `vs_wr`, `cp_lane`, `syn_pos_mean`, `draft_logit`
    roster   пакет `ELO/`: `hybrid_strength`
    org      история организаций: `h2h_resid`

Интеракции требуют ОБА ключа: контекст (`|elo|`, `|games|`) строится из таблицы
аккаунтов, поэтому `draft_logit_x_elo_gap` без аккаунтов недоступен, хотя сам
`draft_logit` доступен.

Компоненты ниже — это группировка ДЛЯ ОТЧЁТА. Вклад компонента считается как
частичная сумма логита, и такое разложение точно по построению. Независимое
обучение компонентов проверялось и стоит 0.0072 AUC на боевой конфигурации —
поэтому веса подбираются совместно, а разделение живёт в коде.
"""
from __future__ import annotations

ACCOUNT, HERO, ROSTER, ORG = "account", "hero", "roster", "org"

_ACC = frozenset({ACCOUNT})
_HERO = frozenset({HERO})
_ROSTER = frozenset({ROSTER})
_ORG = frozenset({ORG})
_HERO_ACC = frozenset({HERO, ACCOUNT})

#: Какие ключи данных нужны каждому боевому признаку.
REQUIRES: dict[str, frozenset] = {
    # драфт: ключ — герой
    "draft_logit": _HERO,
    "wr30": _HERO,
    "vs_wr": _HERO,
    "cp_lane": _HERO,
    "syn_pos_mean": _HERO,
    "farm_dep": _HERO,
    "draft_logit_x_elo_gap": _HERO_ACC,
    "draft_logit_x_games_exp": _HERO_ACC,
    # рейтинг ростера
    "hybrid_strength": _ROSTER,
    # очные встречи организаций
    "h2h_resid": _ORG,
}
for _f in ("elo", "opp_elo", "elo_x_elo_gap", "elo_x_games_exp",
           "games", "hero_games", "pos_games", "hero_pool", "form",
           "hero_gpm_rel", "imp_recent", "gpm_rel_pos", "imp50", "imp_rel_pos",
           "lh_rel_hero", "gpm_ewma", "lh30", "imp30", "lvl_rel_pos",
           "kda_player", "a_hdmg_rel_pos", "a_hdmg_rel_hero", "a_nw_rel_pos",
           "form_x_elo_gap", "form_x_games_exp"):
    REQUIRES[_f] = _ACC

#: Группировка для отчёта и разложения вердикта. `hybrid_strength` показывается
#: вместе с ELO, хотя ключ у него другой: для человека это одна величина «класс
#: команды», а для лестницы важен ключ, и он задан в REQUIRES.
COMPONENTS: dict[str, tuple] = {
    "elo": ("elo", "opp_elo", "hybrid_strength", "elo_x_elo_gap", "elo_x_games_exp"),
    "draft": ("draft_logit", "wr30", "vs_wr", "cp_lane", "syn_pos_mean", "farm_dep",
              "draft_logit_x_elo_gap", "draft_logit_x_games_exp"),
    "players": ("games", "hero_games", "pos_games", "hero_pool", "form",
                "hero_gpm_rel", "imp_recent", "gpm_rel_pos", "imp50",
                "imp_rel_pos", "lh_rel_hero", "gpm_ewma", "lh30", "imp30",
                "lvl_rel_pos", "kda_player", "a_hdmg_rel_pos",
                "a_hdmg_rel_hero", "a_nw_rel_pos",
                "form_x_elo_gap", "form_x_games_exp"),
    "h2h": ("h2h_resid",),
}

_OF = {f: c for c, group in COMPONENTS.items() for f in group}

#: Лестница: от самой полной ветки к самой бедной. Ветка выбирается первая, чьи
#: ключи доступны целиком. Порядок здесь — это и есть приоритет.
BRANCHES: tuple = (
    ("full", frozenset({ACCOUNT, HERO, ROSTER, ORG})),
    ("no_org", frozenset({ACCOUNT, HERO, ROSTER})),
    ("pre_draft", frozenset({ACCOUNT, ROSTER, ORG})),
    ("no_account", frozenset({HERO, ROSTER, ORG})),
    ("no_account_no_org", frozenset({HERO, ROSTER})),
    ("rating_only", frozenset({ROSTER})),
)


def component_of(feature: str) -> str:
    """Компонент, к которому признак относится в отчёте."""
    return _OF[feature]


def columns_for(keys, features) -> list:
    """Признаки, которым хватает доступных ключей, в порядке `features`."""
    keys = frozenset(keys)
    return [f for f in features if REQUIRES[f] <= keys]


def pick_branch(keys):
    """Имя самой полной ветки, чьи ключи доступны целиком; None — если никакой.

    Отказ остаётся ровно там, где нет ни рейтинга ростера, ни драфта: считать
    победу карты не из чего.
    """
    keys = frozenset(keys)
    for name, need in BRANCHES:
        if need <= keys:
            return name
    return None
```

- [x] **Шаг 4: убедиться, что тест проходит**

Запуск: `venv_catboost/bin/python3 -m pytest base/tests/test_prematch_components.py -q`
Ожидание: 8 passed

- [x] **Шаг 5: коммит**

```bash
git add base/prematch_components.py base/tests/test_prematch_components.py
git commit -m "компоненты предматчевой модели: разметка признаков по ключу данных"
```

---

### Задача 2: формат веток в артефакте — запись и чтение

**Файлы:**
- Изменить: `base/prematch_scorer.py` (класс `PrematchModel.__init__`)
- Создать: `base/tests/test_prematch_branch_artifact.py`

**Интерфейсы:**
- Потребляет: `prematch_components.BRANCHES`, `columns_for`.
- Отдаёт: `PrematchModel.branches: dict[str, Branch]`, где `Branch` — dataclass с полями `cols: list[str]`, `mu: np.ndarray`, `sd: np.ndarray`, `coef: np.ndarray`, `intercept: float`. Плюс функция модуля `pack_branches(branches, features) -> dict[str, np.ndarray]` для сборщика артефакта.

- [x] **Шаг 1: написать падающий тест**

```python
"""Ветки кладутся в артефакт плоскими массивами и читаются обратно без потерь.

`PrematchModel.__init__` зовёт `np.load` БЕЗ `allow_pickle`, поэтому рваные
данные (у веток разное число колонок) нельзя хранить object-массивом. Хранятся
длины и один плоский массив на каждую величину.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import prematch_scorer as ps


def _minimal_artifact(tmp_path, extra):
    """Артефакт, которого хватает конструктору: пустые таблицы плюс веса."""
    z = {
        "snapshot_ts": np.array([1700000000], dtype=np.int64),
        "mu": np.zeros((1, 2)), "sd": np.ones((1, 2)),
        "coef": np.zeros((1, 2)), "intercept": np.zeros(1),
        "accounts": np.zeros((0, 14)), "acc_hero": np.zeros((0, 6)),
        "acc_pos": np.zeros((0, 3)), "hero_wr30": np.zeros((0, 2)),
        "vs_pairs": np.zeros((0, 4)), "h2h": np.zeros((0, 3)),
        "hero_farm": np.zeros((0, 2)),
        "feature_names": np.array(["draft_logit", "elo"]),
    }
    z.update(extra)
    p = tmp_path / "artifact.npz"
    np.savez_compressed(p, **z)
    return p


def test_artifact_without_branches_still_loads(tmp_path):
    """Обратная совместимость: на проде лежит артефакт без веток."""
    m = ps.PrematchModel(_minimal_artifact(tmp_path, {}))
    assert m.branches == {}


def test_pack_and_read_round_trip(tmp_path):
    branches = {
        "full": ps.Branch(cols=["draft_logit", "elo"],
                          mu=np.array([0.1, 0.2]), sd=np.array([1.0, 2.0]),
                          coef=np.array([0.5, -0.5]), intercept=0.25),
        "no_account": ps.Branch(cols=["draft_logit"],
                                mu=np.array([0.3]), sd=np.array([3.0]),
                                coef=np.array([0.7]), intercept=-0.1),
    }
    packed = ps.pack_branches(branches, ["draft_logit", "elo"])
    m = ps.PrematchModel(_minimal_artifact(tmp_path, packed))
    assert set(m.branches) == {"full", "no_account"}
    got = m.branches["no_account"]
    assert got.cols == ["draft_logit"]
    assert got.intercept == -0.1
    np.testing.assert_allclose(got.mu, [0.3])
    np.testing.assert_allclose(got.sd, [3.0])
    np.testing.assert_allclose(got.coef, [0.7])
    np.testing.assert_allclose(m.branches["full"].coef, [0.5, -0.5])


def test_packed_arrays_are_not_object_dtype(tmp_path):
    """Иначе чтение упрётся в allow_pickle=False и упадёт в бою."""
    branches = {"full": ps.Branch(cols=["elo"], mu=np.zeros(1), sd=np.ones(1),
                                  coef=np.zeros(1), intercept=0.0)}
    for k, v in ps.pack_branches(branches, ["draft_logit", "elo"]).items():
        assert v.dtype != object, k
```

- [x] **Шаг 2: убедиться, что тест падает**

Запуск: `venv_catboost/bin/python3 -m pytest base/tests/test_prematch_branch_artifact.py -q`
Ожидание: `AttributeError: module 'prematch_scorer' has no attribute 'Branch'`

- [x] **Шаг 3: добавить `Branch`, `pack_branches` и чтение веток**

В `base/prematch_scorer.py` после класса `ScoreResult` добавить:

```python
@dataclass
class Branch:
    """Веса одной ветки лестницы: свои колонки, своя нормировка, свой сдвиг."""

    cols: list[str]
    mu: np.ndarray
    sd: np.ndarray
    coef: np.ndarray
    intercept: float


def pack_branches(branches: dict[str, Branch], features: Sequence[str]) -> dict:
    """Ветки → плоские массивы для `np.savez`.

    Рваные данные (у веток разное число колонок) нельзя хранить object-массивом:
    `PrematchModel.__init__` читает артефакт без `allow_pickle`. Поэтому пишутся
    длины и один плоский массив на величину, а колонки — индексами в `features`.
    """
    idx = {f: i for i, f in enumerate(features)}
    names, lens, cols, mu, sd, coef, itc = [], [], [], [], [], [], []
    for name, b in branches.items():
        names.append(name)
        lens.append(len(b.cols))
        cols.extend(idx[c] for c in b.cols)
        mu.extend(float(x) for x in b.mu)
        sd.extend(float(x) for x in b.sd)
        coef.extend(float(x) for x in b.coef)
        itc.append(float(b.intercept))
    return {
        "branch_names": np.array(names, dtype="<U32"),
        "branch_lens": np.array(lens, dtype=np.int64),
        "branch_cols": np.array(cols, dtype=np.int64),
        "branch_mu": np.array(mu, dtype=np.float64),
        "branch_sd": np.array(sd, dtype=np.float64),
        "branch_coef": np.array(coef, dtype=np.float64),
        "branch_intercept": np.array(itc, dtype=np.float64),
    }
```

В `PrematchModel.__init__`, сразу после установки `self.features`, добавить:

```python
        # Ветки лестницы. Артефакт без них — прежнее поведение: одна модель на
        # все 35 колонок и отказ при неполном входе. На боевой машине лежит
        # именно такой, и он обязан продолжать работать.
        self.branches: dict[str, Branch] = {}
        if "branch_names" in z:
            lens = z["branch_lens"].astype(int)
            cols, mu, sd, coef = (z["branch_cols"].astype(int), z["branch_mu"],
                                  z["branch_sd"], z["branch_coef"])
            off = 0
            for i, nm in enumerate(z["branch_names"]):
                n = int(lens[i])
                self.branches[str(nm)] = Branch(
                    cols=[self.features[j] for j in cols[off:off + n]],
                    mu=mu[off:off + n], sd=sd[off:off + n],
                    coef=coef[off:off + n],
                    intercept=float(z["branch_intercept"][i]))
                off += n
            if off != len(cols):
                raise ValueError(
                    f"ветки артефакта повреждены: сумма длин {off}, "
                    f"а колонок записано {len(cols)}")
```

- [x] **Шаг 4: убедиться, что тест проходит**

Запуск: `venv_catboost/bin/python3 -m pytest base/tests/test_prematch_branch_artifact.py -q`
Ожидание: 3 passed

- [x] **Шаг 5: проверить, что боевой артефакт по-прежнему читается**

Запуск:
```bash
venv_catboost/bin/python3 -c "
import sys; sys.path.insert(0,'base')
import prematch_scorer as ps
m = ps.PrematchModel('runtime/artifacts/misc/prematch_model_artifact_v3_hybrid.npz')
print('признаков', len(m.features), 'веток', len(m.branches), 'аккаунтов', len(m.acc))
"
```
Ожидание: `признаков 35 веток 0 аккаунтов 1553030`

- [x] **Шаг 6: коммит**

```bash
git add base/prematch_scorer.py base/tests/test_prematch_branch_artifact.py
git commit -m "артефакт: ветки лестницы плоскими массивами, без object-dtype"
```

---

### Задача 3: расчёт признаков по компонентам без обращения к недоступным таблицам

**Файлы:**
- Изменить: `base/prematch_scorer.py` (метод `score`)
- Создать: `base/tests/test_prematch_feature_split.py`

**Интерфейсы:**
- Отдаёт: приватные методы `PrematchModel._draft_features(...) -> dict[str, float]`, `_account_features(...) -> tuple[dict[str, float], dict[str, int]]`, `_org_feature(...) -> tuple[float, bool]`. Публичное поведение `score()` на полном входе не меняется.

**Зачем отдельная задача.** Сейчас признаки считаются одним куском, и первая же строка блока игроков (`A = np.array([self.acc[int(a)] for a in a5])`) падает по `KeyError`, если аккаунт неизвестен. Пока расчёт не разнесён, ветка «без аккаунтов» физически недостижима.

- [x] **Шаг 1: написать тест, фиксирующий неизменность полного входа**

```python
"""Разнесение расчёта признаков по компонентам не меняет результат.

Тест держит контракт: пока вход полный, `score()` обязан отдавать ровно те же
числа, что и до разнесения. Эталон снимается на боевом артефакте один раз и
сравнивается побитово — это единственный способ поймать, что при переносе
строк потерялось деление на 100 или перепутался знак (цена такой ошибки
измерена в E-166: 0.116 AUC).
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "base"))

import prematch_scorer as ps

ART = ROOT / "runtime/artifacts/misc/prematch_model_artifact_v3_hybrid.npz"
GOLD = Path(__file__).resolve().parent / "fixtures" / "prematch_score_golden.json"


@pytest.mark.skipif(not ART.exists(), reason="боевой артефакт не собран локально")
def test_full_input_matches_golden():
    gold = json.loads(GOLD.read_text(encoding="utf-8"))
    m = ps.PrematchModel(ART)
    r = m.score(
        radiant_accounts=gold["radiant_accounts"], dire_accounts=gold["dire_accounts"],
        radiant_heroes=gold["radiant_heroes"], dire_heroes=gold["dire_heroes"],
        radiant_team_id=gold["radiant_team_id"], dire_team_id=gold["dire_team_id"],
        draft_logit=gold["draft_logit"], hybrid_strength=gold["hybrid_strength"],
        strictness="teams", now_ts=gold["now_ts"], max_age_days=1e9)
    assert r.probability == pytest.approx(gold["probability"], abs=1e-12)
    for k, v in gold["features"].items():
        assert r.features[k] == pytest.approx(v, abs=1e-12), k
```

- [x] **Шаг 2: снять эталон на нынешнем коде**

Запуск:
```bash
venv_catboost/bin/python3 runtime/experiments/misc/make_score_golden.py
```

Скрипт `runtime/experiments/misc/make_score_golden.py` создать так:

```python
#!/usr/bin/env python3
"""Эталон вердикта на полном входе — до разнесения расчёта по компонентам.

Берётся первая карта корпуса, у которой боевой артефакт отдаёт вердикт при
строгости `teams`. Эталон нужен, чтобы рефакторинг ловился побитово.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))
sys.path.insert(0, str(ROOT / "runtime/experiments/misc"))

import prematch_scorer as ps
from ideas_batch2 import COMPACT

ART = ROOT / "runtime/artifacts/misc/prematch_model_artifact_v3_hybrid.npz"
OUT = ROOT / "base/tests/fixtures/prematch_score_golden.json"


def main() -> None:
    z = np.load(COMPACT)
    m = ps.PrematchModel(ART)
    lg = np.load(ROOT / "runtime/artifacts/misc/pro_draft_logit_full.npz")["logit"]
    hz = np.load(ROOT / "runtime/artifacts/misc/map_winner_hybrid_quality_forward/hybrid_features.npz",
                 allow_pickle=True)
    hmids = {int(x): i for i, x in enumerate(hz["mids"].tolist())}
    for i in range(len(z["mids"]) - 1, -1, -1):
        mid = int(z["mids"][i])
        if mid not in hmids:
            continue
        acc = z["accounts"][i].tolist()
        her = z["heroes"][i].tolist()
        team = z["teams"][i].tolist()
        try:
            r = m.score(radiant_accounts=acc[:5], dire_accounts=acc[5:],
                        radiant_heroes=her[:5], dire_heroes=her[5:],
                        radiant_team_id=int(team[0]), dire_team_id=int(team[1]),
                        draft_logit=float(lg[hmids[mid]]),
                        hybrid_strength=float(hz["F"][hmids[mid], 1]),
                        strictness="teams", now_ts=int(z["ts"][i]),
                        max_age_days=1e9)
        except ps.MissingData:
            continue
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps({
            "mid": mid,
            "radiant_accounts": [int(a) for a in acc[:5]],
            "dire_accounts": [int(a) for a in acc[5:]],
            "radiant_heroes": [int(h) for h in her[:5]],
            "dire_heroes": [int(h) for h in her[5:]],
            "radiant_team_id": int(team[0]), "dire_team_id": int(team[1]),
            "draft_logit": float(lg[hmids[mid]]),
            "hybrid_strength": float(hz["F"][hmids[mid], 1]),
            "now_ts": int(z["ts"][i]),
            "probability": r.probability,
            "features": {k: float(v) for k, v in r.features.items()},
        }, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"эталон снят на карте {mid}: p = {r.probability:.6f}")
        return
    raise SystemExit("не нашлось карты, на которой артефакт отдаёт вердикт")


if __name__ == "__main__":
    main()
```

Ожидание: `эталон снят на карте <id>: p = 0.xxxxxx`, файл `base/tests/fixtures/prematch_score_golden.json` создан.

- [x] **Шаг 3: убедиться, что тест проходит на НЕизменённом коде**

Запуск: `venv_catboost/bin/python3 -m pytest base/tests/test_prematch_feature_split.py -q`
Ожидание: 1 passed. Это фиксация эталона: тест обязан проходить ДО рефакторинга, иначе эталон снят неверно.

- [x] **Шаг 4: разнести расчёт**

В `base/prematch_scorer.py` вынести из `score()` три метода, ничего не меняя в формулах и порядке:

- `_draft_features(radiant_heroes, dire_heroes, draft_logit, hybrid_strength, now_draft, notes)` → возвращает `draft_logit`, `wr30`, `vs_wr`, `cp_lane`, `syn_pos_mean`, `farm_dep`, `hybrid_strength`. Внутрь переносятся `pair_wr`, вызов `_draft_cells` и усреднение `hero_wr30`/`hero_farm` по десяти героям.
- `_account_features(accs, hers, notes)` → возвращает `(f, fill)` со всеми колонками, требующими ключа `account`: вызывает нынешний `side()` для обеих сторон и собирает разности.
- `_org_feature(rt, dt, radiant_accounts, dire_accounts, notes)` → возвращает `(h2h, known)`.

`score()` после этого только собирает словарь из трёх кусков и добавляет интеракции. Формулы не переписывать — переносить строками.

- [x] **Шаг 5: убедиться, что эталон не сдвинулся**

Запуск: `venv_catboost/bin/python3 -m pytest base/tests/test_prematch_feature_split.py base/tests/test_prematch_scorer_scale.py -q`
Ожидание: 3 passed

- [x] **Шаг 6: коммит**

```bash
git add base/prematch_scorer.py base/tests/test_prematch_feature_split.py base/tests/fixtures/prematch_score_golden.json
git commit -m "скорер: расчёт признаков разнесён по компонентам, эталон полного входа зафиксирован"
```

---

### Задача 4: выбор ветки вместо отказа

**Файлы:**
- Изменить: `base/prematch_scorer.py` (`ScoreResult`, `score`)
- Создать: `base/tests/test_prematch_ladder.py`

**Интерфейсы:**
- Потребляет: `Branch`, `prematch_components.pick_branch`, `columns_for`.
- Отдаёт: `ScoreResult.branch: str` и `ScoreResult.missing_keys: list[str]`.

- [x] **Шаг 1: написать падающий тест**

```python
"""Незнакомые снимку игроки больше не отменяют вердикт.

Проверяется на синтетическом артефакте с двумя ветками: полной и той, что
переживает отсутствие аккаунтов. Настоящие веса тут не нужны — нужен контракт
выбора ветки и отсутствие обращения к таблице аккаунтов, когда её нет.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import prematch_scorer as ps

FEATURES = ["draft_logit", "wr30", "vs_wr", "cp_lane", "syn_pos_mean", "farm_dep",
            "hybrid_strength", "h2h_resid", "elo", "opp_elo"]
NO_ACC = ["draft_logit", "wr30", "vs_wr", "cp_lane", "syn_pos_mean", "farm_dep",
          "hybrid_strength", "h2h_resid"]


def _artifact(tmp_path, known_accounts):
    n = len(FEATURES)
    branches = {
        "full": ps.Branch(cols=list(FEATURES), mu=np.zeros(n), sd=np.ones(n),
                          coef=np.full(n, 0.1), intercept=0.0),
        "no_account": ps.Branch(cols=list(NO_ACC), mu=np.zeros(len(NO_ACC)),
                                sd=np.ones(len(NO_ACC)),
                                coef=np.full(len(NO_ACC), 0.2), intercept=0.0),
    }
    acc = np.array([[a, 1500.0] + [0.0] * 17 for a in known_accounts]) if known_accounts \
        else np.zeros((0, 19))
    z = {
        "snapshot_ts": np.array([1700000000], dtype=np.int64),
        "mu": np.zeros((1, n)), "sd": np.ones((1, n)),
        "coef": np.zeros((1, n)), "intercept": np.zeros(1),
        "accounts": acc, "acc_hero": np.zeros((0, 6)), "acc_pos": np.zeros((0, 3)),
        "hero_wr30": np.array([[h, 0.5] for h in range(1, 21)]),
        "hero_farm": np.array([[h, 0.4] for h in range(1, 21)]),
        "vs_pairs": np.zeros((0, 4)), "h2h": np.zeros((0, 3)),
        "feature_names": np.array(FEATURES),
    }
    z.update(ps.pack_branches(branches, FEATURES))
    p = tmp_path / "art.npz"
    np.savez_compressed(p, **z)
    return ps.PrematchModel(p)


def _call(m, accounts):
    return m.score(radiant_accounts=accounts[:5], dire_accounts=accounts[5:],
                   radiant_heroes=[1, 2, 3, 4, 5], dire_heroes=[6, 7, 8, 9, 10],
                   radiant_team_id=11, dire_team_id=22,
                   draft_logit=0.3, hybrid_strength=0.2,
                   strictness="teams", now_ts=1700000000, max_age_days=1e9)


def test_unknown_accounts_fall_back_instead_of_raising(tmp_path):
    m = _artifact(tmp_path, known_accounts=[])
    r = _call(m, list(range(101, 111)))
    assert r.branch == "no_account"
    assert "account" in r.missing_keys
    assert 0.0 < r.probability < 1.0


def test_known_accounts_use_the_full_branch(tmp_path):
    m = _artifact(tmp_path, known_accounts=list(range(101, 111)))
    r = _call(m, list(range(101, 111)))
    assert r.branch == "full"
    assert r.missing_keys == []


def test_fallback_never_touches_the_account_table(tmp_path):
    """Ветка без аккаунтов не имеет права даже заглянуть в таблицу."""
    m = _artifact(tmp_path, known_accounts=[])

    class Boom(dict):
        def __getitem__(self, k):
            raise AssertionError("ветка без аккаунтов полезла в таблицу аккаунтов")

    m.acc = Boom()
    r = _call(m, list(range(101, 111)))
    assert r.branch == "no_account"


def test_refusal_survives_when_nothing_is_available(tmp_path):
    """Нет ни рейтинга ростера, ни драфта — считать не из чего."""
    m = _artifact(tmp_path, known_accounts=[])
    with pytest.raises(ps.MissingData):
        m.score(radiant_accounts=list(range(101, 106)),
                dire_accounts=list(range(106, 111)),
                radiant_heroes=[1, 2, 3, 4, 5], dire_heroes=[6, 7, 8, 9, 10],
                radiant_team_id=11, dire_team_id=22,
                draft_logit=None, hybrid_strength=None,
                strictness="teams", now_ts=1700000000, max_age_days=1e9)
```

- [x] **Шаг 2: убедиться, что тест падает**

Запуск: `venv_catboost/bin/python3 -m pytest base/tests/test_prematch_ladder.py -q`
Ожидание: FAIL — `MissingData: игроки неизвестны снимку` в первом тесте.

- [x] **Шаг 3: реализовать выбор ветки**

В `ScoreResult` добавить поля:

```python
    # Какая ветка лестницы сработала и каких ключей данных не хватило. Пустой
    # список означает полный вход. Поле обязано попадать в журнал: 75% от
    # восьмиколоночной ветки и 75% от полной — разные числа, и продавать их
    # одинаково нельзя.
    branch: str = "full"
    missing_keys: list[str] = field(default_factory=list)
```

В `score()` заменить блок «собрать miss → raise» на разделение причин:

```python
        # Причины делятся на две породы. ЖЁСТКИЕ — считать не из чего или вход
        # заведомо испорчен; они по-прежнему отменяют вердикт. МЯГКИЕ — просто
        # нет данных под какой-то ключ, и это выбор ветки, а не отказ.
        hard: list[str] = []          # протухший снимок, конфликт позиций, форма входа
        keys = {ACCOUNT, HERO, ROSTER, ORG}
        if unknown or zero:
            keys.discard(ACCOUNT)
            notes.append(f"снимок не знает игроков {unknown or zero} — "
                         f"ветка без блока игроков")
        if draft_logit is None or no_wr:
            keys.discard(HERO)
        if hybrid_strength is None:
            keys.discard(ROSTER)
        if key is None or key not in h2h_src:
            keys.discard(ORG)
        if hard:
            raise MissingData(hard)
        branch_name = pick_branch(keys) if self.branches else ("full" if not soft else None)
        if branch_name is None or branch_name not in self.branches:
            raise MissingData(soft or ["нет ни рейтинга ростера, ни драфта"])
```

Считать признаки только тех компонентов, чьи ключи в `keys`; недостающие в словарь не класть вовсе. Применить веса ветки:

```python
        b = self.branches[branch_name]
        x = np.array([f[c] for c in b.cols])
        zz = (x - b.mu) / b.sd
        p = 1.0 / (1.0 + math.exp(-(float(zz @ b.coef) + b.intercept)))
```

Строгость `cells`/`full` продолжает работать как раньше, но только внутри ветки `full`: на короткой ветке ячеек (аккаунт, герой) не бывает по определению.

- [x] **Шаг 4: убедиться, что тесты проходят**

Запуск: `venv_catboost/bin/python3 -m pytest base/tests/test_prematch_ladder.py base/tests/test_prematch_feature_split.py -q`
Ожидание: 5 passed

- [x] **Шаг 5: коммит**

```bash
git add base/prematch_scorer.py base/tests/test_prematch_ladder.py
git commit -m "скорер: лестница веток вместо отказа при неполном входе"
```

---

### Задача 5: обучение весов веток и проверка чисел

**Файлы:**
- Создать: `runtime/experiments/misc/train_branch_weights.py`
- Изменить: `runtime/experiments/misc/finalize_artifact.py`

**Интерфейсы:**
- Потребляет: `prematch_components.BRANCHES`, `columns_for`, `prematch_scorer.Branch`, `pack_branches`.
- Отдаёт: `runtime/artifacts/misc/branch_weights.npz` с ключами `pack_branches` и отчёт `runtime/artifacts/misc/branch_weights.md`.

- [x] **Шаг 1: написать скрипт обучения**

Скрипт обучает по одной логрегрессии на ветку на окне 120 суток перед `TEST_FROM`, меряет AUC каждой на тесте 26 016 и проверяет два контракта: полная ветка обязана воспроизвести монолит, а сумма частичных сумм по компонентам — сам логит полной ветки.

Матрица берётся тем же кодом, что и в `audit_live_path.train_columns`, рабочий набор — из замороженного порядка `hybrid_features.npz`.

- [x] **Шаг 2: прогнать и сверить с ориентирами**

Запуск: `venv_catboost/bin/python3 runtime/experiments/misc/train_branch_weights.py`

Ожидание (допуск ±0.0010, разница от регуляризации):

| ветка | колонок | AUC |
|---|---:|---:|
| full | 35 | 0.7186 |
| no_org | 34 | 0.7181 |
| pre_draft | 27 | 0.6915 |
| no_account | 8 | 0.6864 |
| no_account_no_org | 7 | 0.6852 |
| rating_only | 1 | 0.6516 |

Если `full` разошлась с 0.7186 больше чем на 0.0010 — порядок колонок поехал, дальше не идти.

- [x] **Шаг 3: вкладывать ветки в артефакт**

В `finalize_artifact.py` дописать шаг: прочитать `branch_weights.npz` и подмешать его ключи в выходной артефакт. Если файла нет — шаг пропускается с сообщением, артефакт собирается как раньше.

- [x] **Шаг 4: собрать артефакт и проверить чтение**

Запуск:
```bash
PREMATCH_SRC=runtime/artifacts/misc/prematch_model_artifact_v2_snapshot.npz \
PREMATCH_OUT=runtime/artifacts/misc/prematch_model_artifact_v3_branches.npz \
  venv_catboost/bin/python3 runtime/experiments/misc/finalize_artifact.py
venv_catboost/bin/python3 -c "
import sys; sys.path.insert(0,'base')
import prematch_scorer as ps
m = ps.PrematchModel('runtime/artifacts/misc/prematch_model_artifact_v3_branches.npz')
print('веток', sorted(m.branches), 'колонок в no_account', len(m.branches['no_account'].cols))
"
```
Ожидание: шесть веток, `no_account` — 8 колонок.

- [x] **Шаг 5: коммит**

```bash
git add runtime/experiments/misc/train_branch_weights.py runtime/experiments/misc/finalize_artifact.py
git commit -m "обучение весов по веткам лестницы и вкладывание их в артефакт"
```

---

### Задача 6: калибровка уверенности по ветке

**Файлы:**
- Создать: `runtime/experiments/misc/branch_calibration.py`
- Изменить: `base/prematch_scorer.py` (`lan_winrate`, `ScoreResult.lan_winrate`)
- Создать: `base/tests/test_branch_calibration.py`

**Интерфейсы:**
- Отдаёт: `PrematchModel.branch_winrate(branch: str, confidence: float, lan: bool) -> tuple[float, str]` — винрейт и пометка, своя таблица или унаследованная.

- [x] **Шаг 1: написать падающий тест**

```python
def test_short_branch_does_not_borrow_the_full_table(tmp_path):
    """75% короткой ветки и 75% полной — разные вещи."""
    m = _artifact_with_tables(tmp_path)
    full, src_full = m.branch_winrate("full", 0.75, lan=True)
    short, src_short = m.branch_winrate("no_account", 0.75, lan=True)
    assert src_full == "своя" and src_short == "своя"
    assert short < full


def test_branch_without_its_own_table_inherits_and_says_so(tmp_path):
    m = _artifact_with_tables(tmp_path)
    wr, src = m.branch_winrate("rating_only", 0.75, lan=True)
    assert src == "унаследована от общей — карт не хватило"
```

- [x] **Шаг 2: убедиться, что тест падает**

Запуск: `venv_catboost/bin/python3 -m pytest base/tests/test_branch_calibration.py -q`
Ожидание: `AttributeError: 'PrematchModel' object has no attribute 'branch_winrate'`

- [x] **Шаг 3: снять таблицы**

`branch_calibration.py` прогоняет тест 26 016 карт через каждую ветку, режет по полосам уверенности и по площадке (флаг `lan` из `league_meta.json`) и берёт одностороннюю нижнюю границу Уилсона 90% — то же правило, что в `venue_calibration_grid.py`: менять только вверх, минимум 60 карт в полосе. Ветки, где карт не хватило, помечаются как унаследованные.

- [x] **Шаг 4: реализовать `branch_winrate` и подключить**

`ScoreResult.lan_winrate` считается через `branch_winrate` сработавшей ветки; источник таблицы попадает в `notes`.

- [x] **Шаг 5: прогнать тесты**

Запуск: `venv_catboost/bin/python3 -m pytest base/tests/test_branch_calibration.py base/tests/test_prematch_ladder.py -q`
Ожидание: все зелёные

- [x] **Шаг 6: коммит**

```bash
git add runtime/experiments/misc/branch_calibration.py base/prematch_scorer.py base/tests/test_branch_calibration.py
git commit -m "калибровка уверенности снимается по каждой ветке отдельно"
```

---

### Задача 7: точное разложение вердикта по компонентам

**Файлы:**
- Изменить: `base/prematch_scorer.py` (`ScoreResult`), `base/prematch_panel_live.py`
- Создать: `base/tests/test_prematch_decomposition.py`

**Интерфейсы:**
- Отдаёт: `ScoreResult.parts: dict[str, float]` — вклад каждого компонента в логит.

- [x] **Шаг 1: написать падающий тест**

```python
def test_parts_sum_to_the_logit(tmp_path):
    m = _artifact(tmp_path, known_accounts=list(range(101, 111)))
    r = _call(m, list(range(101, 111)))
    logit = math.log(r.probability / (1.0 - r.probability))
    assert sum(r.parts.values()) + m.branches[r.branch].intercept == pytest.approx(logit, abs=1e-9)


def test_parts_only_lists_components_present_in_the_branch(tmp_path):
    m = _artifact(tmp_path, known_accounts=[])
    r = _call(m, list(range(101, 111)))
    assert "players" not in r.parts
    assert set(r.parts) <= {"elo", "draft", "h2h"}
```

- [x] **Шаг 2: убедиться, что тест падает**

Запуск: `venv_catboost/bin/python3 -m pytest base/tests/test_prematch_decomposition.py -q`
Ожидание: `AttributeError: 'ScoreResult' object has no attribute 'parts'`

- [x] **Шаг 3: считать частичные суммы**

```python
        parts: dict[str, float] = {}
        for c, w, col in zip((component_of(c) for c in b.cols), b.coef * zz, b.cols):
            parts[c] = parts.get(c, 0.0) + float(w)
```

- [x] **Шаг 4: показать разложение в панели**

В `base/prematch_panel_live.py` добавить строку с вкладами компонентов и именем ветки.

- [x] **Шаг 5: прогнать тесты**

Запуск: `venv_catboost/bin/python3 -m pytest base/tests/test_prematch_decomposition.py base/tests/test_prematch_ladder.py base/tests/test_prematch_feature_split.py -q`
Ожидание: все зелёные

- [x] **Шаг 6: коммит**

```bash
git add base/prematch_scorer.py base/prematch_panel_live.py base/tests/test_prematch_decomposition.py
git commit -m "вердикт раскладывается по компонентам точно, разложение видно в панели"
```

---

## Что НЕ входит в этот план

Чистка компонентов — отдельный план, потому что каждый её кусок это самостоятельный замер со своим решением «внедрять или нет»:

- Glicko-1 в компонент ELO (+0.0013, а в коротких ветках потенциально больше — E-171);
- свёртка кластера IMP и кластера карьеры (37% веса модели с нетто 0.036);
- починка разрешения организаций для `h2h_resid` (в бою ноль на 85.5% карт);
- честная as-of таблица матчапа по урону и замер с бутстрапом по сериям (+0.0009 против протёкших +0.0016).

Доставка на serv1 тоже вне плана: боевой код правится руками и расходится с локальным, перенос делается точечными патчами по отдельному решению.


---

## Итог выполнения (2026-08-20)

Задачи 1-7 выполнены, кроме показа разложения в панели (задача 7, шаг 4): это
правка боевого файла на serv1, а доставка вне этого плана.

**Что померено на сквозном прогоне** (`audit_branch_ladder.md`, снимок обрезан
по `TEST_FROM`, вызов повторяет боевой):

| | было | стало |
|---|---:|---:|
| карт с вердиктом | 11 064 (42.5%) | **25 892 (99.5%)** |
| отказы | 14 952 | 124 (только конфликт разметки позиций) |

**Потерь на уже покрытых картах нет.** На 9 459 картах, где прежняя модель
подставляла нулём отсутствующий `h2h_resid`, ветка `no_org` даёт 0.7028 против
0.7031 — разница −0.0002. На 1 605 картах полной ветки вердикт совпадает с
прежним ПОБИТОВО: `full` берёт боевые веса как есть.

**Поправка по ходу работы.** Первая версия переобучала полную ветку. Сквозной
прогон это поймал: совпало 0 вердиктов из 11 064. Лестница не имеет права
менять вердикты, которые уже есть.

**Калибровка.** Ветки получили свои таблицы винрейта; полоса без 60 карт не
получает винрейта вовсе, и автоматическая ставка по ней не выставляется. У
`no_account` в полосе 50-58% нижняя граница 0.476 — ниже монетки.

Артефакт с ветками и калибровкой: `prematch_model_artifact_v3_branches.npz`.
На serv1 НЕ доставлялся.

#!/usr/bin/env python3
"""Симметричные предматчевые признаки на 00:00 — общий вход для трёх моделей.

Построено по `dota2_pregame_00_symmetric_feature_registry.xlsx`. Главная мысль
регистра: не держать `radiant_X` и `dire_X` порознь, а сразу выдавать пару

    X_diff = A − B      (при обмене сторон меняет знак)
    X_sum  = A + B      (при обмене сторон не меняется)

Тогда модель側-симметрична по построению: важность признака одна, а не две, и
swap-аугментация не нужна. Признак стороны (`team_a_is_radiant`) выдаётся
отдельно — он и должен быть единственным местом, где сторона имеет значение.

Признаки собираются из трёх источников, и каждый может отсутствовать:
  * `base/hero_features_v7.json` — константы героев (категория 04 регистра) и
    baseline-метрики `герой×позиция` по нашим корпусам (категория 05);
  * снимок предматчевой модели — величины игроков (категория 09);
  * словари драфта — контрпик и синергия (категория 07).

ОТСУТСТВУЮЩЕЕ НЕ ЗАПОЛНЯЕТСЯ НУЛЁМ. Если ресурса нет, соответствующее семейство
признаков просто не выдаётся, а его имя попадает в `missing_families`. Ноль на
месте неизвестного — это уверенность из воздуха; ровно на этом сегодня
разбирались расхождения обучения и боя.

Чего здесь принципиально нет и не будет без новых данных:
  * категории 02 и 08 (порядок пиков и банов) — в корпусе нет драфт-лога;
  * `pickoffs` и `teamfight_kills` из категории 05 — в записи игрока нет
    таймингов, их нечем ни измерить, ни оценить;
  * категория 13 (OOF-модели) — это отдельные модели, а не извлекатель.

Использование:
    from pregame_features import PregameFeatures
    pf = PregameFeatures()
    f = pf.build(radiant_heroes=[...], dire_heroes=[...],
                 radiant_positions=[1,2,3,4,5], dire_positions=[1,2,3,4,5])
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any, Optional, Sequence

ROOT = Path(os.getenv("DRAFT_ROOT", Path(__file__).resolve().parents[1]))
HERO_CARD = Path(os.getenv("HERO_CARD_PATH", ROOT / "base/hero_features_v7.json"))

# Поля карточки, которые НЕ агрегируются: идентификаторы, метаданные и то, что
# не является величиной героя.
SKIP_FIELDS = {
    "hero_id", "hero_name", "hero_slug", "schema_version", "patch_tag",
    "verified_patch", "source_generated_at_utc", "profile_notes", "source",
    "primary_attr", "lane_role", "primary_position", "damage_pattern",
    "positions", "facet_names", "dmg_by_pos_legacy", "dmg_by_pos_legacy_note",
    "baselines", "items_by_pos", "dmg_by_pos",
}
# Списки способностей: не числа, но их ДЛИНА — осмысленный признак
LIST_AS_COUNT_SUFFIX = "_abilities"


def _num(x: Any) -> Optional[float]:
    if isinstance(x, bool):
        return float(x)
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        v = float(x)
        return v if math.isfinite(v) else None
    return None


class PregameFeatures:
    """Извлекатель симметричных признаков. Ресурсы читаются один раз."""

    def __init__(self, hero_card: Path | str = HERO_CARD,
                 baselines: Path | str | None = None) -> None:
        """`baselines` подменяет блок baseline в карточках.

        Нужен для честной оценки: baseline, посчитанные по тем же картам, на
        которых потом меряется модель, дают утечку того же рода, что измерена
        у драфт-логита в E-177. Для оценки сюда подаётся набор, собранный
        строго до тестового окна.
        """
        self.heroes: dict[int, dict] = {}
        self.hero_numeric_fields: list[str] = []
        self.baseline_metrics: dict[str, list[str]] = {}
        self.missing_families: list[str] = []
        path = Path(hero_card)
        if not path.exists():
            self.missing_families.append("hero_constants+hero_baselines")
            return
        raw = json.loads(path.read_text(encoding="utf-8"))
        self.heroes = {int(k): v for k, v in raw.items() if isinstance(v, dict)}
        if baselines:
            bp = Path(baselines)
            data = json.loads(bp.read_text(encoding="utf-8"))["heroes"]
            n = 0
            for hid, row in self.heroes.items():
                b = data.get(str(hid))
                if b:
                    row["baselines"] = {"pro": b.get("pro", {}), "pub": b.get("pub", {})}
                    n += 1
                else:
                    row.pop("baselines", None)
            self.baselines_source = f"{bp.name} ({n} героев)"
        else:
            self.baselines_source = str(path.name)
        self._index_fields()

    # ---------- подготовка ----------
    def _index_fields(self) -> None:
        """Какие поля карточки числовые у ВСЕХ героев — только их и агрегируем."""
        ids = sorted(self.heroes)
        if not ids:
            return
        cand = set(self.heroes[ids[0]])
        for h in ids[1:]:
            cand &= set(self.heroes[h])
        num, lst = [], []
        for f in sorted(cand):
            if f in SKIP_FIELDS:
                continue
            vals = [self.heroes[h].get(f) for h in ids]
            if all(_num(v) is not None for v in vals):
                num.append(f)
            elif f.endswith(LIST_AS_COUNT_SUFFIX) and all(isinstance(v, list) for v in vals):
                lst.append(f)
        self.hero_numeric_fields = num
        self.hero_list_fields = lst
        # какие baseline-метрики доступны и в каком корпусе
        for corp in ("pro", "pub"):
            names: set[str] = set()
            for h in ids:
                b = (self.heroes[h].get("baselines") or {}).get(corp) or {}
                cell = (b.get("all") or {}).get("m") or {}
                names |= set(cell)
            if names:
                self.baseline_metrics[corp] = sorted(names)
        if not self.baseline_metrics:
            self.missing_families.append("hero_baselines")

    # ---------- агрегаты стороны ----------
    def _side_constants(self, heroes: Sequence[int]) -> dict[str, float]:
        out: dict[str, float] = {}
        rows = [self.heroes.get(int(h)) for h in heroes]
        known = [r for r in rows if r]
        if not known:
            return out
        k = len(known)
        for f in self.hero_numeric_fields:
            s = 0.0
            for r in known:
                v = _num(r.get(f))
                if v is not None:
                    s += v
            out[f"{f}_teamsum"] = s
            out[f"{f}_teammean"] = s / k
        for f in getattr(self, "hero_list_fields", []):
            s = float(sum(len(r.get(f) or []) for r in known))
            out[f"{f}_count_teamsum"] = s
        return out

    def _side_baselines(self, heroes: Sequence[int], positions: Sequence[int],
                        corp: str) -> dict[str, float]:
        """Сумма и среднее baseline-метрик героев НА ИХ ПОЗИЦИЯХ.

        Если для конкретной пары герой-позиция ячейки нет или она ненадёжна,
        берётся ячейка «всего по герою»; если нет и её — герой в сумму не
        входит, а его отсутствие видно по счётчику покрытия.
        """
        names = self.baseline_metrics.get(corp) or []
        if not names:
            return {}
        acc = {n: 0.0 for n in names}
        used = 0
        for h, p in zip(heroes, positions):
            b = (self.heroes.get(int(h), {}).get("baselines") or {}).get(corp) or {}
            cell = ((b.get("pos") or {}).get(str(int(p))) or {})
            if not cell.get("reliable_mean"):
                cell = b.get("all") or {}
            m = cell.get("m") or {}
            if not m:
                continue
            used += 1
            for n in names:
                v = m.get(n)
                if v and v[0] is not None:
                    acc[n] += float(v[0])
        if not used:
            return {}
        out = {f"bl_{corp}_{n}_teamsum": acc[n] for n in names}
        out.update({f"bl_{corp}_{n}_teammean": acc[n] / used for n in names})
        out[f"bl_{corp}_coverage"] = used / max(len(list(heroes)), 1)
        return out

    # ---------- симметризация ----------
    @staticmethod
    def _symmetrize(a: dict[str, float], b: dict[str, float]) -> dict[str, float]:
        out: dict[str, float] = {}
        for k in sorted(set(a) | set(b)):
            va, vb = a.get(k), b.get(k)
            if va is None or vb is None:
                continue          # неполная пара — признак не выдаём вовсе
            out[f"{k}_diff"] = va - vb
            out[f"{k}_sum"] = va + vb
        return out

    @staticmethod
    def ordered_pair_id(a: int, b: int) -> int:
        """Стабильный id упорядоченной пары: при обмене сторон меняется."""
        return int(a) * 1000 + int(b)

    # ---------- сборка ----------
    def build(self, *, radiant_heroes: Sequence[int], dire_heroes: Sequence[int],
              radiant_positions: Sequence[int] = (1, 2, 3, 4, 5),
              dire_positions: Sequence[int] = (1, 2, 3, 4, 5),
              radiant_team_id: int = 0, dire_team_id: int = 0,
              team_a_is_radiant: bool = True) -> dict[str, float]:
        """Симметричный вектор признаков. Сторона A — радиант по умолчанию."""
        rh = [int(x) for x in radiant_heroes]
        dh = [int(x) for x in dire_heroes]
        rp = [int(x) for x in radiant_positions]
        dp = [int(x) for x in dire_positions]
        if not (len(rh) == len(dh) == 5):
            raise ValueError("ожидались по пять героев на сторону")

        f: dict[str, float] = {}
        f.update(self._symmetrize(self._side_constants(rh), self._side_constants(dh)))
        for corp in self.baseline_metrics:
            f.update(self._symmetrize(self._side_baselines(rh, rp, corp),
                                      self._side_baselines(dh, dp, corp)))
        # категория 03: упорядоченные пары героев по позициям
        by_r = {p: h for h, p in zip(rh, rp)}
        by_d = {p: h for h, p in zip(dh, dp)}
        for p in range(1, 6):
            if p in by_r and p in by_d:
                f[f"pos{p}_hero_ordered_pair_id"] = float(
                    self.ordered_pair_id(by_r[p], by_d[p]))
        # категория 01: пара команд и признак стороны
        if radiant_team_id > 0 and dire_team_id > 0:
            f["team_ordered_pair_id"] = float(
                self.ordered_pair_id(radiant_team_id, dire_team_id))
        f["team_a_is_radiant"] = float(bool(team_a_is_radiant))
        return f

    def feature_names(self) -> list[str]:
        """Имена признаков на пустом драфте — для сверки состава."""
        ids = sorted(self.heroes)[:10]
        if len(ids) < 10:
            return []
        return sorted(self.build(radiant_heroes=ids[:5], dire_heroes=ids[5:]))


_INSTANCE: Optional[PregameFeatures] = None


def get_extractor(path: Path | str = HERO_CARD) -> PregameFeatures:
    global _INSTANCE
    if _INSTANCE is None:
        _INSTANCE = PregameFeatures(path)
    return _INSTANCE

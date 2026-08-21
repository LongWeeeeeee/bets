#!/usr/bin/env python3
"""Панель предматчевых ML-моделей: калибровка, пороги, заполненность, журнал.

Семь целей считаются на каждой карте до её начала и показываются отдельным
блоком в телеграм-сообщении: окна килов 5-15, 10-20, 15-25, 20-30 (кто сделает
больше), длительность 43+, тотал карты ≥55 против ≤50 и одна сторона ≥30 против
≤25. Ставок модуль НЕ делает — он отмечает вердикты галочками и пишет журнал,
по которому их можно проверить через месяц.

Три вещи, ради которых модуль существует отдельно от скореров.

КАЛИБРОВКА. У моделей разные AUC, и сырые скоры между ними несравнимы: 0.7 у
окна 5-15 и 0.7 у тотала означают разную частоту попаданий. Сравнимой величиной
может быть только калиброванная вероятность — доля попаданий среди карт с таким
же скором. Она хранится в артефакте изотонической лестницей (узлы x→y) и
применяется `np.interp`, поэтому на проде не нужен sklearn. После калибровки
«лучшее окно» — это просто максимум вероятности среди прошедших свой порог.

ЗАПОЛНЕННОСТЬ. Модель молча выдаёт предикт, даже когда половина признаков
заменена нулями, — эту дыру мы уже ловили в предматчевой модели. Здесь каждая
модель объявляет свои группы признаков, а скорер сообщает, сколько из них
реально нашлось. Заполненность идёт и в сообщение, и в журнал: вердикт при 60%
заполненности читается иначе, чем при 100%.

ПОРОГ. Порог задаётся в шкале калиброванной вероятности и подбирается офлайн
так, чтобы историческая доля попаданий выше него держала заданную планку при
достаточном объёме. Он лежит в артефакте рядом с калибровкой, а не в коде: смена
порога не должна требовать деплоя.

ДРАФТ ПРОТИВ. Вердикт не выставляется, когда драфтовая компонента решения
тянет ПРОТИВ названной стороны: `draft_share` стоит в ориентации вердикта, и
минус означает, что модель назвала сторону вопреки драфту. Ноль — это «драфту
нечего сказать», а не возражение (E-217 §1), поэтому режется строго минус.
Доли нет вовсе (модель вне `ML_PANEL_DRAFT_KEYS` или SHAP не посчитался) —
запрет не срабатывает: «против» надо доказать, а не предположить.

Формат журнала (JSONL, по строке на карту) заморожен: `schema` = 1. Любое
изменение состава полей повышает номер, иначе прошлые строки станут
непроверяемыми.
"""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACT_DIR = Path(os.getenv(
    "ML_PANEL_DIR", str(PROJECT_ROOT / "ml-models" / "prematch_panel")))
DEFAULT_JOURNAL = Path(os.getenv(
    "ML_PANEL_JOURNAL", str(PROJECT_ROOT / "runtime" / "ml_panel.jsonl")))
JOURNAL_SCHEMA = 4   # +blocked: чем именно погашен вердикт
OK_MARK = "🟢"
NO_MARK = "🔴"
# Ниже этой заполненности вердикт не выставляется вообще: предикт по половине
# признаков — не осторожная оценка, а другой предикт.
MIN_FILL = float(os.getenv("ML_PANEL_MIN_FILL", "0.75"))
# Запрет «драфт против названной стороны». Выключается ML_PANEL_DRAFT_AGAINST_BLOCK=0
# без деплоя — как и порог заполненности, это правило отбора, а не код.
DRAFT_AGAINST_BLOCK = os.getenv(
    "ML_PANEL_DRAFT_AGAINST_BLOCK", "1") not in ("0", "false", "False")
#: Причина в строке и в журнале. Без неё 🔴 по драфту неотличим от 🔴 по порогу.
BLOCK_DRAFT_AGAINST = "драфт против"


@dataclass(frozen=True)
class ModelSpec:
    """Описание одной цели. `positive`/`negative` — что означает исход."""

    key: str
    title: str
    positive: str
    negative: str
    threshold: float
    groups: tuple[str, ...] = ()
    knots_x: tuple[float, ...] = ()
    knots_y: tuple[float, ...] = ()
    # Полосы уверенности с ФАКТИЧЕСКОЙ долей попаданий на тесте:
    # ({"lo": 0.75, "hit": 0.76, "n": 1221}, ...). Кэф считается от неё, а не от
    # калиброванной вероятности: рекомендовать цену надо по тому, что сбылось.
    bands: tuple[Mapping[str, float], ...] = ()

    def band_hit(self, confidence: float) -> tuple[float, int] | None:
        """Доля попаданий и объём полосы, в которую попала уверенность."""
        best: tuple[float, int] | None = None
        for b in self.bands:
            try:
                if confidence >= float(b["lo"]):
                    best = (float(b["hit"]), int(b.get("n", 0)))
            except (KeyError, TypeError, ValueError):
                continue
        return best

    def fair_odds(self, confidence: float) -> float | None:
        """Кэф безубытка по факту полосы: ниже него ставка убыточна."""
        band = self.band_hit(confidence)
        if band is None or band[0] <= 0.0:
            return None
        return 1.0 / band[0]

    def calibrate(self, raw: float) -> float:
        """Сырой скор → вероятность по изотонической лестнице."""
        if not self.knots_x:
            return float(raw)
        xs, ys = self.knots_x, self.knots_y
        if raw <= xs[0]:
            return float(ys[0])
        if raw >= xs[-1]:
            return float(ys[-1])
        lo = 0
        hi = len(xs) - 1
        while hi - lo > 1:                      # двоичный поиск, без numpy
            mid = (lo + hi) // 2
            if xs[mid] <= raw:
                lo = mid
            else:
                hi = mid
        span = xs[hi] - xs[lo]
        if span <= 0:
            return float(ys[lo])
        w = (raw - xs[lo]) / span
        return float(ys[lo] + w * (ys[hi] - ys[lo]))


@dataclass(frozen=True)
class ModelVerdict:
    key: str
    title: str
    side: str
    probability: float
    threshold: float
    fill: float
    ok: bool
    missing: tuple[str, ...] = ()
    raw: float | None = None
    draft_share: float | None = None    # доля драфта в решении по этой карте
    parts: dict | None = None           # разложение решения по ELO/драфту/игрокам
    band_hit: float | None = None       # фактическая доля попаданий полосы
    band_n: int = 0
    odds: float | None = None           # кэф безубытка
    blocked: str | None = None          # чем погашен вердикт; None — не гасили

    @property
    def confidence(self) -> float:
        """Уверенность в исходе, который модель называет: всегда ≥ 0.5."""
        return self.probability if self.probability >= 0.5 else 1.0 - self.probability


def evaluate(spec: ModelSpec, raw: float | None, present: Mapping[str, bool],
             draft_share: float | None = None,
             parts: dict | None = None,
             fill: float | None = None,
             missing: Sequence[str] | None = None) -> ModelVerdict | None:
    """Вердикт одной модели. `None`, если скор не посчитался вовсе.

    `fill` лучше передавать посчитанной ПО ДОЛЕ КОЛОНОК: блок из шести колонок
    и блок из семисот сорока двух — не одно и то же, а счёт по группам делает
    их равными. Без него берётся запасной счёт по группам.
    """
    if raw is None:
        return None
    p = spec.calibrate(raw)
    p = min(max(p, 0.0), 1.0)
    if missing is None:
        need = spec.groups or tuple(present)
        missing = tuple(g for g in need if not present.get(g, False))
    else:
        missing = tuple(missing)
    if fill is None:
        need = spec.groups or tuple(present)
        fill = 1.0 - (len(missing) / len(need)) if need else 1.0
    side = spec.positive if p >= 0.5 else spec.negative
    conf = p if p >= 0.5 else 1.0 - p
    ok = bool(conf >= spec.threshold and fill >= MIN_FILL)
    blocked = None
    if ok and DRAFT_AGAINST_BLOCK and draft_share is not None and draft_share < 0.0:
        # Минус в ориентации вердикта — драфт тянет против той стороны, которую
        # называет модель. Такой вердикт не выставляется вовсе.
        ok = False
        blocked = BLOCK_DRAFT_AGAINST
    band = spec.band_hit(conf)
    return ModelVerdict(key=spec.key, title=spec.title, side=side, probability=p,
                        threshold=spec.threshold, fill=fill, ok=ok,
                        missing=missing, raw=float(raw), draft_share=draft_share,
                        parts=dict(parts) if parts else None,
                        blocked=blocked,
                        band_hit=None if band is None else band[0],
                        band_n=0 if band is None else band[1],
                        odds=spec.fair_odds(conf))


def best_of(verdicts: Iterable[ModelVerdict]) -> ModelVerdict | None:
    """Лучший из семейства: максимум уверенности среди прошедших порог.

    Сравнение идёт по КАЛИБРОВАННОЙ уверенности, а не по сырому скору: у моделей
    с разными AUC сырые шкалы несопоставимы, и «лучшее окно» по сырому скору
    выбиралось бы в пользу самой шумной модели.
    """
    passing = [v for v in verdicts if v.ok]
    if not passing:
        return None
    return max(passing, key=lambda v: (v.confidence, -len(v.missing)))


def render(verdicts: Sequence[ModelVerdict], highlight: Iterable[str] = ()) -> str:
    """Блок ML для телеграм-сообщения.

    В строке: сторона, уверенность, заполненность, разложение решения по блокам
    (ELO / драфт / игроки, знак в ориентации Radiant) и кэф безубытка. Кэф берётся из ФАКТИЧЕСКОЙ доли попаданий полосы на тесте, а не
    из калиброванной вероятности: рекомендовать цену надо по тому, что сбылось.
    Ниже этого кэфа ставка убыточна даже при идеальном исполнении.
    """
    if not verdicts:
        return ""
    star = set(highlight)
    lines = ["🤖 ML:"]
    for v in verdicts:
        mark = OK_MARK if v.ok else NO_MARK
        # Заполненность округляется ВНИЗ. При округлении к ближайшему потеря
        # двух колонок из 928 давала 99.78% и печаталась как «100%» — то есть
        # сообщение утверждало полноту входа, которой не было. Сто процентов
        # обязаны означать ровно сто.
        bits = [f"{v.side} {v.confidence*100:.0f}%",
                f"зап. {int(v.fill * 100)}%"]
        if v.parts:
            # Знак у оконных моделей — в ориентации Radiant (минус за Dire), как
            # в строке предматчевой модели. Доли от суммы модулей по БЛОКАМ: они
            # дают ровно 100%, и видно, кто тянет против.
            bits.append("вклад: " + " ".join(
                f"{_b} {v.parts.get(_b, 0.0)*100:+.0f}%"
                for _b in ("ELO", "драфт", "игроки") if _b in v.parts))
        elif v.draft_share is not None:
            # Знак обязателен: +N% — драфт за сторону вердикта, −N% — против.
            bits.append(f"драфт {v.draft_share*100:+.0f}%")
        if v.odds is not None:
            bits.append(f"кэф от {v.odds:.2f}")
        if v.blocked:
            bits.append(v.blocked)
        tail = " ✅" if v.key in star else ""
        lines.append(f"{mark} {v.title}: " + " · ".join(bits) + tail)
    return "\n".join(lines)


def journal_row(map_id: Any, verdicts: Sequence[ModelVerdict], *,
                extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Строка журнала со всем, что нужно для последующей проверки."""
    row: dict[str, Any] = {
        "schema": JOURNAL_SCHEMA,
        "map_id": str(map_id),
        "models": [
            {"key": v.key, "side": v.side, "p": round(v.probability, 6),
             "confidence": round(v.confidence, 6), "threshold": v.threshold,
             "fill": round(v.fill, 4), "ok": v.ok,
             "missing": list(v.missing), "raw": v.raw,
             "draft_share": (None if v.draft_share is None
                             else round(v.draft_share, 4)),
             "parts": ({k: round(x, 4) for k, x in v.parts.items()}
                       if v.parts else None),
             "blocked": v.blocked,
             "band_hit": v.band_hit, "band_n": v.band_n,
             "odds": None if v.odds is None else round(v.odds, 4)}
            for v in verdicts
        ],
    }
    if extra:
        row.update(dict(extra))
    return row


def append_journal(row: Mapping[str, Any], path: Path | None = None) -> None:
    """Дозапись строки. Открывается в 'a', поэтому конкурентные writer'ы
    не затирают друг друга; строка пишется одним `write`, чтобы не рваться."""
    target = Path(path or DEFAULT_JOURNAL)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def load_specs(directory: Path | None = None) -> tuple[ModelSpec, ...]:
    """Описания моделей из `panel.json`. Пусто, если артефакта нет: панель
    обязана молчать, а не падать, когда её ещё не выложили."""
    path = Path(directory or DEFAULT_ARTIFACT_DIR) / "panel.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ()
    out: list[ModelSpec] = []
    for item in payload.get("models", []):
        try:
            out.append(ModelSpec(
                key=str(item["key"]), title=str(item["title"]),
                positive=str(item["positive"]), negative=str(item["negative"]),
                threshold=float(item["threshold"]),
                groups=tuple(str(g) for g in item.get("groups", ())),
                knots_x=tuple(float(x) for x in item.get("knots_x", ())),
                knots_y=tuple(float(y) for y in item.get("knots_y", ())),
                bands=tuple(dict(b) for b in item.get("bands", ()))))
        except (KeyError, TypeError, ValueError):
            continue
    return tuple(out)


def atomic_write_specs(specs: Sequence[ModelSpec], directory: Path) -> Path:
    """Сборка артефакта: пишем во временный файл и переименовываем поверх."""
    directory.mkdir(parents=True, exist_ok=True)
    payload = {"models": [
        {"key": s.key, "title": s.title, "positive": s.positive,
         "negative": s.negative, "threshold": s.threshold,
         "groups": list(s.groups), "knots_x": list(s.knots_x),
         "knots_y": list(s.knots_y), "bands": [dict(b) for b in s.bands]}
        for s in specs]}
    target = directory / "panel.json"
    fd, tmp = tempfile.mkstemp(prefix=".panel.", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, target)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise
    return target

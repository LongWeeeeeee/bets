"""Единственный источник правды о том, КАКОЙ каталог драфт-модели раздаётся.

Зачем отдельный модуль. Путь к каталогу был продублирован в шести местах
(`win_model_veto` плюс пять скриптов в `runtime/`), общей константы не было, и
переключение одного места не меняло остальные. Именно на этом фоне прожил
незамеченным рассинхрон E-201: боевые веса предматчевой модели обучены на
логите одного каталога, а раздавался логит другого — цена −0.0046 AUC.

Что этот модуль НЕ решает. Сверка ниже ловит смешение файлов ВНУТРИ каталога
(кодировщик на 16 758 колонок против модели на 16 764). Она не ловит случай
E-201, где оба файла согласованы между собой, но не согласованы с верхней
моделью: та хранит веса, а не отпечаток входа. Для этого и заведён
`model_fingerprint` — чтобы версию раздаваемого логита можно было положить в
лог и сравнить глазами, пока сверка не автоматизирована.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CATALOG_ROOT = PROJECT_ROOT / "data/public_draft_hero10_experiment"

# Каталог, который раздаётся в бою. Меняется переменной окружения без деплоя.
# ВНИМАНИЕ: смена значения меняет живую оценку ставки (E-200/E-201), поэтому
# переключение — отдельное решение, а не побочный эффект правки кода.
#
# Значение обязано совпадать с тем, что реально загружает прод на serv1: там
# `WIN_MODEL_DIR` не задан, поэтому раздаётся именно это умолчание. Проверка —
# `ssh serv1` и сверка ширины (5 млн = 16 764 колонки, 1.19 млн = 16 758).
# До 18.08.2026 здесь стоял каталог на 1.19 млн карт, тогда как на serv1 уже
# был переведён на 5 млн: выкат этого модуля откатил бы прод обратно и вернул
# перекос E-201 (−0.0046 AUC) — ровно тот класс ошибки, ради которого модуль
# и заведён.
DEFAULT_CATALOG = "2026-08-15_all_public_5m_full"

ENCODER_FILE = "win_feature_encoder.joblib"
MODEL_FILE = "radiant_win_model.joblib"


def model_dir() -> Path:
    """Каталог раздаваемой драфт-модели с учётом `WIN_MODEL_DIR`."""
    override = os.getenv("WIN_MODEL_DIR")
    if override:
        return Path(override)
    return CATALOG_ROOT / DEFAULT_CATALOG


# Каталог, на выходе которого калибровались пороги вето `_DEFAULT_MIN_INDEX`.
# Пороги — АБСОЛЮТНЫЕ отсечки по шкале `ml_win_index`, а шкала зависит от
# каталога: у модели на 5.09 млн карт sd индекса на 12.5% шире, чем у модели на
# 1.19 млн (замер 19.08.2026 на 200 000 свежих карт). Сверки этой пары не было
# вовсе, хотя `manifest.json` рядом с моделью хранит `counts.all`.
#
# Значение обновлено 19.08.2026 после пересчёта сетки E-73 на текущем каталоге:
# на 47 124 картах кэша боевые 10/5/9/0 подтвердились, менять их не потребовалось.
THRESHOLDS_CALIBRATED_FOR = "2026-08-15_all_public_5m_full"
_CATALOG_WARNED = False


def check_thresholds_catalog(directory: Path | None = None) -> None:
    """Предупредить один раз, если пороги калибровались на другом каталоге.

    Мягко: отбор не блокируется. Задача — чтобы смена каталога перестала быть
    невидимой, как это было с рассинхроном логита в E-201 (цена −0.0046 AUC).
    """
    global _CATALOG_WARNED
    if _CATALOG_WARNED:
        return
    directory = directory or model_dir()
    if directory.name == THRESHOLDS_CALIBRATED_FOR:
        return
    _CATALOG_WARNED = True
    print(f"ВНИМАНИЕ: раздаётся каталог {directory.name}, а пороги вето "
          f"калибровались на {THRESHOLDS_CALIBRATED_FOR}. Шкала ml_win_index "
          "зависит от каталога — пороги надо пересчитать сеткой E-73.",
          flush=True)


def model_fingerprint(directory: Path | None = None) -> str:
    """Короткий отпечаток пары файлов каталога — тем же способом, что у карточки.

    Нужен, чтобы «какой логит сейчас раздаётся» было видно в логе. Считается по
    обоим файлам сразу: подмена любого из них меняет отпечаток.
    """
    directory = directory or model_dir()
    digest = hashlib.sha1()
    for name in (ENCODER_FILE, MODEL_FILE):
        path = directory / name
        if not path.exists():
            return "нет-файлов"
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
    return f"{directory.name}:{digest.hexdigest()[:12]}"


def check_width(encoder: Any, model: Any) -> None:
    """Бросает ValueError, если кодировщик и модель из разных сборок.

    Тот же приём, что в `prematch_panel_scorer` для панели: ширину спрашиваем у
    артефакта, а не верим имени каталога. У драфт-модели такой сверки не было
    вовсе — отсюда и класс ошибок «файлы из разных каталогов лежат рядом».
    """
    expected = getattr(encoder, "n_columns", None)
    actual = getattr(model, "n_features_in_", None)
    if actual is None:
        coef = getattr(model, "coef_", None)
        actual = coef.shape[1] if coef is not None and coef.ndim == 2 else None
    if expected is None or actual is None:
        return
    if int(expected) != int(actual):
        raise ValueError(
            f"артефакт несогласован: кодировщик даёт {expected} колонок, "
            f"а модель обучена на {actual} — файлы из разных сборок")

#!/usr/bin/env python3
"""Обучение простой ML meta‑модели поверх транзитивного анализатора.

Использует CSV, собранный build_transitive_ml_dataset.py, и обучает
логистическую регрессию (или другой простой классификатор) предсказывать
RadiantWin на основе детерминированных фичей:
    h2h/common/transitive scores, серии, Elo, strength и т.д.

Фокус:
- time‑based split (train на старых матчах, val на новых);
- вывод accuracy/coverage для разных порогов по P(RadiantWin);
- без сложного тюнинга, как первый шаг для оценки, есть ли прирост.

Пример запуска:
    cd transitive_modules
    python3 train_transitive_meta_model.py \
        --csv transitive_ml_dataset.csv \
        --val-frac 0.2
"""

from __future__ import annotations

import argparse
import csv
import pickle
from dataclasses import dataclass
from typing import List, Tuple

import math

try:
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler
except ImportError as e:  # pragma: no cover - зависимость извне
    raise SystemExit(
        "scikit-learn не установлен. Установите его в venv: pip install scikit-learn"
    ) from e


NUM_FEATURES: List[str] = [
    # Базовые скоры
    "h2h_score",
    "common_score",
    "transitive_score",
    "total_score",
    "h2h_series",
    "common_series",
    "transitive_series",
    "total_series",
    "info_units",
    "elo_radiant",
    "elo_dire",
    "elo_diff",
    "elo_score",
    "strength",
    "confidence",
    "period_days",
    
    # =========================================================================
    # НОВЫЕ ФИЧИ
    # =========================================================================
    
    # Form (текущая форма команды)
    "radiant_form",
    "dire_form",
    "form_diff",
    "radiant_form_raw",
    "dire_form_raw",
    "form_diff_raw",
    "radiant_form_games",
    "dire_form_games",
    
    # Streak (серия побед/поражений)
    "radiant_streak",
    "dire_streak",
    "streak_diff",
    "radiant_streak_length",
    "dire_streak_length",
    
    # Momentum (изменение Elo за 14 дней)
    "radiant_momentum",
    "dire_momentum",
    "momentum_diff",
    
    # Activity (дней с последнего матча)
    "radiant_days_since_last",
    "dire_days_since_last",
    "radiant_is_cold",
    "dire_is_cold",
    
    # Consistency (стабильность результатов)
    "radiant_consistency",
    "dire_consistency",
    "consistency_diff",
    "radiant_avg_margin",
    "dire_avg_margin",
    "radiant_clean_sweep_rate",
    "dire_clean_sweep_rate",
    
    # Side stats (винрейт по сторонам)
    "radiant_radiant_wr",
    "radiant_dire_wr",
    "dire_radiant_wr",
    "dire_dire_wr",
    "side_advantage",
    
    # Tier stats (против разных уровней команд)
    "radiant_vs_tier1_wr",
    "radiant_vs_tier2_wr",
    "dire_vs_tier1_wr",
    "dire_vs_tier2_wr",
    "radiant_sos",
    "dire_sos",
    "sos_diff",
    
    # Normalized scores
    "form_score",
    "momentum_score",
    "streak_score",
    "activity_score",
    
    # Signal Agreement
    "signals_agree",
    "signal_conflict",
    "h2h_elo_conflict",
]

CATEGORICAL_FEATURES: List[str] = [
    "decision_mode",      # кодируем one-hot
    "confidence_label",   # тоже one-hot
]

TARGET_FIELD = "radiant_win"


@dataclass
class Dataset:
    X: List[List[float]]
    y: List[int]


def _one_hot(value: str, vocabulary: List[str]) -> List[float]:
    return [1.0 if value == v else 0.0 for v in vocabulary]


def load_dataset(path: str) -> Tuple[Dataset, List[str], List[str]]:
    """Грузит CSV и возвращает X, y + словари категориальных признаков.

    Возвращает также vocab’ы для decision_mode / confidence_label.
    """
    rows: List[dict] = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get(TARGET_FIELD) in ("0", "1"):
                rows.append(row)

    # собираем уникальные значения категориальных фичей
    decision_values = sorted({r.get("decision_mode", "") for r in rows})
    conf_label_values = sorted({r.get("confidence_label", "") for r in rows})

    X: List[List[float]] = []
    y: List[int] = []

    for r in rows:
        vec: List[float] = []
        # числовые фичи
        for f in NUM_FEATURES:
            val = r.get(f)
            if val is None or val == "":
                vec.append(0.0)
            else:
                try:
                    vec.append(float(val))
                except Exception:
                    vec.append(0.0)

        # one-hot категориальные
        vec.extend(_one_hot(r.get("decision_mode", ""), decision_values))
        vec.extend(_one_hot(r.get("confidence_label", ""), conf_label_values))

        y.append(int(r[TARGET_FIELD]))
        X.append(vec)

    return Dataset(X=X, y=y), decision_values, conf_label_values


def time_based_split(ds: Dataset, val_frac: float) -> Tuple[Dataset, Dataset]:
    """Простой time-based split: первые (1-val_frac) в train, последние в val.

    Порядок строк в CSV уже соответствует времени (build_transitive_ml_dataset
    сортирует матчи по возрастанию). Мы просто режем по индексу.
    """
    n = len(ds.X)
    if n == 0:
        raise ValueError("Пустой датасет")
    cut = max(1, int(n * (1.0 - val_frac)))
    train = Dataset(X=ds.X[:cut], y=ds.y[:cut])
    val = Dataset(X=ds.X[cut:], y=ds.y[cut:])
    return train, val


def evaluate_probs(probs: List[float], y_true: List[int], threshold: float) -> Tuple[float, float, int]:
    """Возвращает accuracy, coverage, used при пороге threshold.

    coverage = used / len(y_true), где used — число примеров, где модель уверена
    (p<=1-th или p>=th). Среднюю зону можно считать "нет ставки".
    """
    assert len(probs) == len(y_true)
    used = 0
    hits = 0
    n = len(y_true)
    for p, y in zip(probs, y_true):
        if p >= threshold:
            used += 1
            hits += int(y == 1)
        elif p <= 1.0 - threshold:
            used += 1
            hits += int(y == 0)
        else:
            # зона неопределённости
            continue
    acc = hits / used if used > 0 else 0.0
    cov = used / n if n > 0 else 0.0
    return acc, cov, used


def train_and_eval(csv_path: str, val_frac: float, save_path: str | None = None) -> None:
    ds, decision_vocab, conf_vocab = load_dataset(csv_path)
    print(f"Загружено примеров: {len(ds.X)}")
    train, val = time_based_split(ds, val_frac=val_frac)
    print(f"Train: {len(train.X)}, Val: {len(val.X)}")

    # стандартизация числовых признаков, one-hot остаются как есть
    scaler = StandardScaler()
    X_train = scaler.fit_transform(train.X)
    X_val = scaler.transform(val.X)

    try:
        from sklearn.ensemble import GradientBoostingClassifier
        print("Используем GradientBoostingClassifier...")
        clf = GradientBoostingClassifier(n_estimators=200, learning_rate=0.05, max_depth=3, random_state=42)
    except ImportError:
        print("GradientBoostingClassifier не найден, используем LogisticRegression...")
        clf = LogisticRegression(max_iter=1000, n_jobs=-1)
        
    clf.fit(X_train, train.y)

    val_probs = clf.predict_proba(X_val)[:, 1]

    print("\nПороговая оценка (RadiantWin probability thresholds):")
    print(f"{'thr':<6} {'used':<8} {'coverage':<10} {'accuracy':<10}")
    print("-" * 40)
    for thr in [0.55, 0.6, 0.65, 0.7, 0.75]:
        acc, cov, used = evaluate_probs(val_probs, val.y, threshold=thr)
        print(f"{thr:<6.2f} {used:<8d} {cov*100:>8.1f}% {acc*100:>8.1f}%")

    # Общая accuracy без фильтра (thr=0.5)
    hard_preds = [1 if p >= 0.5 else 0 for p in val_probs]
    hits = sum(int(p == y) for p, y in zip(hard_preds, val.y))
    acc_full = hits / len(val.y) if val.y else 0.0
    print("\nAccuracy при пороге 0.5 (без фильтрации): {:.2%}".format(acc_full))

    # Feature importance анализ
    print("\n" + "=" * 60)
    print("FEATURE IMPORTANCE (Top 20):")
    print("=" * 60)
    
    # Собираем имена всех фичей (числовые + one-hot)
    feature_names = list(NUM_FEATURES) + \
        [f"decision_{v}" for v in decision_vocab] + \
        [f"conf_{v}" for v in conf_vocab]
    
    if hasattr(clf, 'feature_importances_'):
        # GradientBoosting / RandomForest
        importances = clf.feature_importances_
        indices = sorted(range(len(importances)), key=lambda i: importances[i], reverse=True)
        for rank, idx in enumerate(indices[:20], 1):
            name = feature_names[idx] if idx < len(feature_names) else f"feature_{idx}"
            print(f"  {rank:2d}. {name:<35} {importances[idx]:.4f}")
    elif hasattr(clf, 'coef_'):
        # LogisticRegression
        coefs = clf.coef_[0]
        indices = sorted(range(len(coefs)), key=lambda i: abs(coefs[i]), reverse=True)
        for rank, idx in enumerate(indices[:20], 1):
            name = feature_names[idx] if idx < len(feature_names) else f"feature_{idx}"
            print(f"  {rank:2d}. {name:<35} {coefs[idx]:+.4f}")
    
    # Группировка по категориям фичей
    print("\n" + "=" * 60)
    print("FEATURE IMPORTANCE BY CATEGORY:")
    print("=" * 60)
    
    categories = {
        'Base scores': ['h2h_score', 'common_score', 'transitive_score', 'total_score', 'elo_score'],
        'Elo': ['elo_radiant', 'elo_dire', 'elo_diff'],
        'Form': ['radiant_form', 'dire_form', 'form_diff', 'form_score'],
        'Streak': ['radiant_streak', 'dire_streak', 'streak_diff', 'streak_score'],
        'Momentum': ['radiant_momentum', 'dire_momentum', 'momentum_diff', 'momentum_score'],
        'Activity': ['radiant_days_since_last', 'dire_days_since_last', 'radiant_is_cold', 'dire_is_cold', 'activity_score'],
        'Consistency': ['radiant_consistency', 'dire_consistency', 'consistency_diff', 'radiant_avg_margin', 'dire_avg_margin'],
        'Side stats': ['radiant_radiant_wr', 'radiant_dire_wr', 'dire_radiant_wr', 'dire_dire_wr', 'side_advantage'],
        'Tier stats': ['radiant_vs_tier1_wr', 'radiant_vs_tier2_wr', 'dire_vs_tier1_wr', 'dire_vs_tier2_wr', 'radiant_sos', 'dire_sos', 'sos_diff'],
        'Signal agreement': ['signals_agree', 'signal_conflict', 'h2h_elo_conflict'],
    }
    
    if hasattr(clf, 'feature_importances_'):
        importances = list(clf.feature_importances_)
    elif hasattr(clf, 'coef_'):
        importances = [abs(c) for c in clf.coef_[0]]
    else:
        importances = None
    
    if importances is not None:
        for cat_name, cat_features in categories.items():
            cat_importance = 0.0
            for feat in cat_features:
                if feat in feature_names:
                    idx = feature_names.index(feat)
                    if idx < len(importances):
                        cat_importance += importances[idx]
            print(f"  {cat_name:<25} {cat_importance:.4f}")

    # Сохранение модели и scaler при необходимости
    if save_path:
        bundle = {
            "model": clf,
            "scaler": scaler,
            "decision_vocab": decision_vocab,
            "conf_label_vocab": conf_vocab,
            "num_features": NUM_FEATURES,
        }
        with open(save_path, "wb") as f:
            pickle.dump(bundle, f)
        print(f"Модель сохранена в {save_path}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Обучение meta‑модели поверх транзитивного анализатора")
    p.add_argument("--csv", type=str, default="transitive_ml_dataset.csv", help="Путь к CSV датасету")
    p.add_argument("--val-frac", type=float, default=0.2, help="Доля данных в валидации (0.1-0.3)")
    p.add_argument(
        "--save-path",
        type=str,
        default="",
        help="Куда сохранить обученную модель (pickle). По умолчанию не сохранять.",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if not (0.0 < args.val_frac < 0.5):
        raise SystemExit("val-frac должен быть в (0, 0.5)")
    save_path = args.save_path or None
    train_and_eval(args.csv, val_frac=args.val_frac, save_path=save_path)

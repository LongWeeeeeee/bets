#!/usr/bin/env python3
"""Runtime‑обёртка для ML meta‑модели поверх транзитивного анализатора.

Задача:
- загрузить pickle, сохранённый train_transitive_meta_model.py;
- по результату get_transitiv (dict) построить вектор признаков и
  вернуть P(RadiantWin) для meta‑модели.

Использование (пример):

    from transitive_meta_model import get_global_meta_model

    model = get_global_meta_model()  # по умолчанию transitive_meta_model.pkl
    p_radiant = model.predict_proba_from_result(result_dict)
"""

from __future__ import annotations

import os
import pickle
from typing import Any, Dict, List


DEFAULT_MODEL_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "transitive_meta_model.pkl")


class TransitiveMetaModel:
    """Обёртка над (scaler, model, vocab’ами) для инференса."""

    def __init__(self, path: str = DEFAULT_MODEL_PATH) -> None:
        self.path = path
        if not os.path.exists(path):
            raise FileNotFoundError(f"Meta‑модель не найдена по пути: {path}")
        with open(path, "rb") as f:
            bundle = pickle.load(f)

        self.model = bundle["model"]
        self.scaler = bundle["scaler"]
        self.decision_vocab: List[str] = list(bundle.get("decision_vocab", []))
        self.conf_label_vocab: List[str] = list(bundle.get("conf_label_vocab", []))
        self.num_features: List[str] = list(bundle.get("num_features", []))

        if not self.num_features:
            # Фоллбек на текущий порядок фичей, если по какой‑то причине не сохранили
            self.num_features = [
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
            ]

    @staticmethod
    def _one_hot(value: str, vocabulary: List[str]) -> List[float]:
        return [1.0 if value == v else 0.0 for v in vocabulary]

    def build_feature_vector(self, result: Dict[str, Any]) -> List[float]:
        """Строит вектор признаков из dict, возвращённого get_transitiv.

        Ожидается, что result содержит те же поля, что и строки CSV,
        собранные build_transitive_ml_dataset.py.
        """
        vec: List[float] = []

        # числовые фичи в фиксированном порядке
        for f in self.num_features:
            val = result.get(f)
            if isinstance(val, (int, float)):
                vec.append(float(val))
            elif val is None or val == "":
                vec.append(0.0)
            else:
                try:
                    vec.append(float(val))
                except Exception:
                    vec.append(0.0)

        # one‑hot по decision_mode / confidence_label
        decision_val = str(result.get("decision_mode", ""))
        conf_val = str(result.get("confidence_label", ""))
        vec.extend(self._one_hot(decision_val, self.decision_vocab))
        vec.extend(self._one_hot(conf_val, self.conf_label_vocab))

        return vec

    def predict_proba_from_result(self, result: Dict[str, Any]) -> float:
        """Возвращает P(RadiantWin) по результату get_transitiv()."""
        x = [self.build_feature_vector(result)]
        x_scaled = self.scaler.transform(x)
        proba = self.model.predict_proba(x_scaled)[0][1]
        return float(proba)


_GLOBAL_MODEL: TransitiveMetaModel | None = None


def get_global_meta_model(path: str = DEFAULT_MODEL_PATH) -> TransitiveMetaModel:
    """Ленивый загрузчик singleton‑экземпляра meta‑модели.

    Удобно вызывать из get_transitiv(use_ml_meta=True), чтобы не грузить
    pickle при каждом матче.
    """
    global _GLOBAL_MODEL
    if _GLOBAL_MODEL is None:
        _GLOBAL_MODEL = TransitiveMetaModel(path)
    return _GLOBAL_MODEL

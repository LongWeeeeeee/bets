> Documented on 2026-07-31 against commit `e08790c`.

# Team kills ≥27: эксперимент и production-модель

## Итог

Production-модель оценивает вероятность того, что выбранная команда сделает
минимум 27 убийств на карте. Расчётный коэффициент — `1.80`, поэтому порог
отправки фиксирован на break-even:

```text
1 / 1.80 = 0.555555... = 55.56%
```

Модель использует 95 pre-map признаков, не применяет ручные ограничения по
среднему числу убийств и не использует исход текущей карты. Отдельный Telegram
sender включён на production; основной STAR/stake dispatch от него не зависит.

Код и артефакты:

- `base/team_kills25_shadow.py` — live features, inference, audit и sender;
- `base/train_team_kills25_shadow.py` — обучение frozen logistic artifact;
- `ml-models/team_kills27/team_kills27_shadow.json` — production artifact,
  schema `team_kills27_shadow.v2`;
- `ELO/output/live_team_elo_snapshot.json` — ELO и roster kills-history schema v2.

Имена модуля и env prefix `TEAM_KILLS25_*` оставлены legacy-совместимыми;
фактический target и пути runtime относятся к 27+.

## Данные и защита от data leak

Label всегда пересчитывается из финальных данных карты:

```python
target = int(target_kills >= 27)
```

Старый `target_ge25` не переиспользуется. Все признаки доступны до результата
предсказываемой карты.

Chronological evaluation:

| Выборка | Период | Карт | Назначение |
|---|---|---:|---|
| Old train | начало old-периода | 574 | обучение кандидатов |
| Old validation | после train | 191 | выбор `C` по log loss |
| Old test | конец old-периода | 192 | контроль после выбора |
| Forward | 2026-06-23 — 2026-07-31 | 299 | out-of-time диагностика, без model fit |

Полный old dataset содержит 957 карт с доступными `early_nw`, `early_win`,
`late`, `all` и pre-map ELO. Порядок split — только по времени. Ни один forward
outcome не участвовал в model fit, выборе `C` или threshold. Однако результаты
forward просматривались при исследовании семейств roster/consensus признаков;
поэтому это out-of-time diagnostic, а не полностью нетронутый one-shot holdout.
Окончательное подтверждение доходности должен дать новый будущий shadow cohort.

Перебирались:

```text
C ∈ {0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0}
```

По old validation выбран `C=0.003`. Ставочный threshold не оптимизируется по
profit/ROI и всегда равен `1/1.8`.

## Признаки

### 90 draft/ELO признаков

Используются все четыре смысловых блока:

- `early_nw`;
- `early_win`;
- `late`;
- `all`.

Для каждого блока передаются шесть метрик:

- `counterpick_1vs1`;
- `counterpick_1vs2`;
- `pos1_vs_pos1`;
- `solo`;
- `synergy_duo`;
- `synergy_trio`.

Значения выравниваются относительно target side. В модель входят raw и
absolute значения, число согласных/несогласных метрик, `aligned_sum`,
`abs_sum`, `abs_max`, а также согласованность одной и той же метрики между
блоками. Дополнительно используются `nw_hit_count`, `nw_max_wr`, target-aligned
ELO probability и ELO diff.

### Три block-consensus признака

Направление блока — знак суммы его target-aligned метрик. Если доступны минимум
три блока, рассчитываются:

- `blocks_target_count` — сколько блоков направлено на target;
- `blocks_opponent_count` — сколько блоков направлено против target;
- `blocks_consensus_target` — все доступные 3/4 блока направлены на target.

Отдельный флаг «все блоки против target» не используется: такой случай был
слишком редким и нестабильным.

Эмпирическая связь consensus с исходом карты:

| Период | Состояние | Карт | Победа target | Target kills ≥27 |
|---|---|---:|---:|---:|
| Old | 4/4 за target | 88 | 67.0% | 56.8% |
| Old | раскол 2/2 | 292 | 47.3% | 49.7% |
| Forward | 4/4 за target | 60 | 65.0% | 68.3% |
| Forward | раскол 2/2 | 117 | 43.6% | 57.3% |

Consensus является признаком модели 27+, а не отдельным target победы карты.
Связь с победой используется как дополнительная информация о силе драфта.

### Два confidence-weighted roster признака

Roster history берётся только из более ранних карт того же `team_id`, где с
текущей пятёркой совпадают минимум четыре account ID. Текущая карта, будущие
карты и повторные `match_id` исключаются. Окно — последние 30 подходящих карт;
для model features остаются карты latest patch.

Для `n` карт текущего патча:

```text
reliability = n / (n + 6)

roster_patch_mean_edge_confident =
    (patch_mean_kills - 27) * reliability

roster_patch_ge27_edge_confident =
    (patch_ge27_rate - 1/1.8) * reliability
```

Число `6` — scale доверия, а не hard gate. При маленькой выборке roster-эффект
плавно уменьшается. Raw `n`, среднее, медиана и доля 27+ сохраняются в audit и
Telegram, но raw games не входит отдельным model feature: в эксперименте его
коэффициент получался контринтуитивным и мог повышать прогноз при малом sample.

Hard gates `games>=6`, `mean>=23/25` не применяются: в эксперименте они
снижали абсолютную прибыль и forward ROI. При недоступном roster source sender
работает fail-closed; валидный источник с нулевым sample обрабатывается frozen
median imputer.

## Результаты при коэффициенте 1.80

Все результаты ниже взяты из production artifact.

| Evaluation | Bets | Hits | Hit rate | Profit | ROI | Max DD |
|---|---:|---:|---:|---:|---:|---:|
| Validation | 32 | 22 | 68.8% | +7.6u | 23.8% | 2.2u |
| Old test | 39 | 23 | 59.0% | +2.4u | 6.2% | 5.0u |
| Forward untouched | 141 | 98 | 69.5% | +35.4u | 25.1% | 3.6u |
| Forward, production refit on all old | 156 | 104 | 66.7% | +31.2u | 20.0% | 4.4u |

Поле artifact `forward_untouched` означает, что модель после train/validation
selection не обучалась на forward outcomes. Оно не означает, что исследователь
никогда не видел forward-диагностику при проектировании feature family.
`Forward production refit` соответствует frozen artifact, переобученному на
всех old rows с уже выбранными `C` и threshold. Ни один forward outcome не
попал в обучение production artifact.

### Tier-срез production refit на forward

| Сегмент | Bets | Hits | Hit rate | Profit | ROI |
|---|---:|---:|---:|---:|---:|
| T1–T1 | 13 | 8 | 61.5% | +1.4u | 10.8% |
| T1–T2 | 18 | 10 | 55.6% | 0.0u | 0.0% |
| T1–T3 | 7 | 4 | 57.1% | +0.2u | 2.9% |
| T2–T2 | 23 | 15 | 65.2% | +4.0u | 17.4% |
| T2–T3 | 49 | 35 | 71.4% | +14.0u | 28.6% |
| T3–T3 | 46 | 32 | 69.6% | +11.6u | 25.2% |

Матчи с tier1-командой вместе: 38/22, `+1.6u`, ROI `4.2%`. Выборка мала,
поэтому отдельный tier gate по forward не подбирался.

## Проверка на кейсе GLYPH

Для состава с overlap минимум 4 игрока перед картой `8922300474`:

- все патчи, окно 30: среднее `20.2`, 27+ в `10/30`;
- latest patch 7.41d: 7 карт, среднее `19.14`, 27+ в `3/7`;
- `roster_patch_mean_edge_confident = -4.23`;
- `roster_patch_ge27_edge_confident = -0.068`.

Оба roster-признака понижают вероятность. Слабое среднее не является
механическим запретом: исключительно сильный draft/ELO consensus всё ещё может
перевесить историю, но модель больше не игнорирует низкую результативность.

## Runtime и антидубликат

Production использует:

```text
TEAM_KILLS25_SHADOW_MODEL_PATH=/root/main/ml-models/team_kills27/team_kills27_shadow.json
TEAM_KILLS25_SHADOW_LOG_PATH=/root/main/runtime/team_kills27_shadow.jsonl
TEAM_KILLS25_TELEGRAM_SENT_PATH=/root/main/runtime/team_kills27_telegram_sent.jsonl
TEAM_KILLS25_TELEGRAM_ENABLED=1
```

Telegram token и chat ID находятся только в закрытом service env-файле.

Антидубликат нормализует DLTV poll/score key до стабильного Dota `match_id`.
Persistent at-most-once claim записывается до Telegram HTTP request, поэтому
изменение URL suffix, timeout или restart не создают повторную ставку. Старый
sent-log 25+ сохранён отдельно и не смешивается с 27+.

На rollout snapshot v2 был атомарно пересобран из 27 377 pro-карт; latest patch
— 7.41d. Production tests: 49 passed.

## Ограничения

- Old test содержит только 39 ставок: положительный результат не гарантирует
  будущую прибыль.
- Forward имеет более высокий общий kill rate, поэтому drift патча необходимо
  продолжать контролировать.
- Forward-результаты просматривались во время feature research; считать их
  окончательной независимой оценкой нельзя. Нужен следующий future cohort.
- T1-срез мал; текущие данные не оправдывают отдельный tier gate.
- Коэффициент принят фиксированным `1.80`; при реальном другом коэффициенте
  должен меняться break-even threshold.
- Модель предсказывает убийства одной команды, а не победителя карты и не total
  kills двух команд.

## Воспроизводимость

При наличии сформированных chronological CSV:

```bash
/Users/alex/Documents/ingame/venv_catboost/bin/python3 \
  base/train_team_kills25_shadow.py \
  --old runtime/team_kills27_old_patch_roster_features.csv \
  --forward runtime/team_kills27_forward_patch_roster_features.csv \
  --artifact ml-models/team_kills27/team_kills27_shadow.json \
  --report runtime/team_kills27_shadow_training_report.json
```

После обучения необходимо проверить schema, feature order/hash, target 27,
threshold `1/1.8` и запустить тесты `base/tests/test_team_kills25_shadow*.py` и
`ELO/tests/`.

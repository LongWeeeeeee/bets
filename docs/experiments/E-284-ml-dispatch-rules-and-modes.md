---
id: E-284
title: "ML-диспатч: правила владельца и режимы star/shadow/ml"
date: "2026-09-12"
area: model
status: "in-progress"
corpus: "код `base/ml_dispatch.py` (457 строк, unit-тесты `test_ml_dispatch.py`) + гейт `DISPATCH_MODE` в `cyberscore_try.py`; офлайн-обоснование — E-282 (калибровка/бэктест) и E-283 (replay по журналу отправленных ставок); прод-числа shadow-режима ещё не собраны"
verdict: "Реализация (stage 1+2 плана `swirling-giggling-kurzweil.md`) завершена и покрыта тестами; правила соответствуют владельческим решениям E-282/E-283 (порог 0.60, вето Late/All ПОСЛЕ поддержки по стороне, U/F по ELO-диффу 50, тайминг lane->00/600с). Прод-эффект (доля решений, расхождение сторон со STAR-панелью, реальные skip-причины) НЕ измерен — включение в shadow ещё не запускалось на момент записи; addendum обязателен после 24-48ч shadow."
harness: "base/ml_dispatch.py; base/cyberscore_try.py:_ml_dispatch_tick/_dispatch_mode_reject_for_delivery; pytest base/tests/test_ml_dispatch.py base/tests/test_dispatch_mode_gate.py -q"
---

# E-284 — ML-диспатч: правила владельца и режимы star/shadow/ml

## Вопрос

Переход диспатча ставок со словарных STAR-путей (какая линия слов даёт какую
ставку) на решение по пяти ML-моделям (Early NW, Early Win, Late, All, ML
Laning). Владелец задал конкретные правила (12.09.2026, докстринг
`ml_dispatch.py`): единый порог уверенности 0.60 на все пять моделей; андердог
= сторона с ELO ниже минимум на 50; win-ставка идёт при поддержке хотя бы
одной из настроенных win-моделей (default `late,all,early_win`) и при
отсутствии вето Late/All за другую сторону, причём вето разрешается ПОСЛЕ
проверки поддержки, по каждой стороне отдельно («правка 0» — иначе genuine
conflict путался с обычным veto); kills-маркеты только при наличии андердога,
через Early NW/Early Win (+опционально All); тайминг win-маркета — немедленно
("00"), если ML Laning подтверждает таргет, иначе ждать 600 секунд игрового
времени; предматчевая 35-признаковая модель explicitly НЕ применяется к
ml_dispatch-решениям (несётся в `Ctx.prematch_index` только для лога).

Задача этой записи — не новый замер, а формализация уже принятых решений и
протокола перехода: что уже проверено офлайн (E-282/E-283), что реализовано в
коде, и как безопасно включать (`DISPATCH_MODE=star|shadow|ml`) без потери
прод-ставок и без слепой замены проверенного STAR-пути.

## Данные и обоснование (ссылки на предыдущие записи)

- **E-282** (калибровка четырёх phase-моделей + бэктест правил на ELO-полосах,
  status=full): калибровка честная на public out-of-time (gap <=1пп), но на
  pro Early Win завышает себя на верхних порогах вне своей популяции (63.0%
  факт против заявленных 85% на `early_win>=0.80`, все карты); правила
  ELO-полоса + модель>=0.65 (варианты C/D/ALL) дают устойчивую прибавку
  +16..+23пп к P_elo на популяциях «все лиги» и «prod allowlist» согласованно
  (n=45..1856); вето `against_F` по late/all снижает WR фаворита на 6-25пп —
  подтверждает раннюю переуверенность моделей на встречной стороне, что и
  оправдывает вето Late/All в `ml_dispatch._evaluate_win`. Килы-прокси (10-20
  мин, тотал>=медиана) дают lift вдвое слабее NW-маркера — поэтому в коде
  kills-маркеты по-прежнему держатся на Early NW/Early Win, а не на медианном
  прокси.
- **E-283** (replay правил C/D на журнале РЕАЛЬНО отправленных ставок,
  status=full): на 206 отправленных prematch_model-ставках правила C/D сами
  по себе НЕ отличают WR/ROI от остального потока (отобранные n=28: WR 78.6%,
  ROI +56.5%; отклонённые n=173: WR 75.0%, ROI +87.3% — правило хуже) —
  популяция уже self-selected действующим диспетчером, который и так требует
  согласия моделей. Вето late>=0.60 ни разу не сработало на 76 картах с полем
  late (максимум 0.5735). Канонический контрфактуал (нужна All-модель)
  оказался невозможен — героев для карт 11-12.09 не было ни в одном локальном
  корпусе (freshness chain отставала на сутки, см. память
  `freshness-chain-fails-silently.md`). Вывод E-283 — офлайн-replay на уже
  отфильтрованном журнале не может ни подтвердить, ни опровергнуть новые
  правила; нужен shadow-режим на неотфильтрованном потоке карт.

## Реализация

- `base/ml_dispatch.py` — чистый решатель, без импортов из `cyberscore_try.py`
  и без побочных эффектов кроме `SentLedger` (явный, вызывается снаружи).
  `Config.from_env()` (строки 169-192) читает env при каждом вызове.
  `_evaluate_win` (строки ~256-303) реализует «правку 0» — `survives[side] =
  bool(support[side]) and not vetoers[side]`, `conflict` только если обе
  стороны survives. `_evaluate_kills` (строки ~305-360) — андердог-only,
  `kills_windows_open` фильтрует `kills_window` отдельно от `kills_total`.
- `base/cyberscore_try.py:dispatch_mode()` (~11747) и
  `_dispatch_mode_reject_for_delivery` (~11760) — гейт режима, ПЕРВЫЙ в
  `_deliver_and_persist_signal` (~32704-32726), терминален для delayed-очереди
  (`_drop_delayed_match(reason="star_dispatch_disabled")`).
- `_ml_dispatch_tick` (~12052-12246) — тик карты: собирает `Ctx` из
  `early_output/mid_output/all_output` (через `win_model_veto.last_early_nw/
  last_early_win/last_late`) + `laning_serving.verdicts` (All/ML Laning),
  зовёт `evaluate`, пишет `runtime/ml_dispatch_decisions.jsonl`
  (`_ml_dispatch_record_decisions`, дедуп по sha256 от `dedup_view`), и в
  `mode=="ml"` доставляет `timing=="now"` решения через
  `_ml_dispatch_deliver_decision` → `_deliver_and_persist_signal` с
  `stake_multiplier_context={"origin":"ml_dispatch", ...}`. Единственная точка
  вызова — сразу после `_try_dispatch_prematch_model_bet(...)`, строки
  ~39674-39713 основного цикла.
- `_win_model_reject_for_delivery` (~12339) и
  `_late_win_model_reject_for_delivery` (~12428) явно пропускают
  `origin=="ml_dispatch"` без повторной проверки — вето Late/All уже
  применено внутри `ml_dispatch.evaluate` до появления `Decision`.
- ★-подсветка: `_format_win_model_line` (~7571-7614, инлайновое чтение
  `ML_DISPATCH_MIN_CONF` — функция гоняется тестами в изолированном `exec()`
  без остального модуля) и `laning_serving.panel_lines`/`_dispatch_min_conf()`
  — информационная, не гейт напрямую.
- Новый заголовок `_format_signal_header(special_header_mode="kills_total")`
  (~11654-11656): `"СТАВКА НА Тотал килов {team} БОЛЬШЕ"`.

## Протокол включения

1. **shadow 24-48ч** (`DISPATCH_MODE=shadow` через systemd drop-in, см.
   `docs/RUNTIME_RULES.md`): STAR-ставки идут как раньше, ml_dispatch только
   пишет `runtime/ml_dispatch_decisions.jsonl`. Мерить по логу:
   - доля тиков карт с хотя бы одним `decisions` (не пустым) — как часто
     ml_dispatch вообще готов был бы поставить;
   - распределение `underdog_side` (U/F/None) — соответствует ли ожиданию
     `ML_DISPATCH_UNDERDOG_MIN_DIFF=50`;
   - какие рынки чаще (`win` vs `kills_window`/`kills_total`);
   - причины `skipped` (`conflict`, `veto`, `model_missing`, `below_threshold`,
     `dedup`, `no_underdog`) — доля `model_missing` покажет, насколько часто
     каким-то из пяти вердиктов просто неоткуда взяться;
   - расхождение `target_side` решений ml_dispatch с фактической стороной
     STAR-ставки на тех же картах (если STAR всё же отправила) — не должно
     быть систематического конфликта направлений.
2. Переключение на `ml` — только после ручного разбора shadow-лога владельцем;
   эта запись НЕ содержит критерия автоматического перехода (владелец решает).
3. Откат — `DISPATCH_MODE=star` (или удалить drop-in) + рестарт (та же
   процедура, без дополнительных действий — старые пути никуда не делись).

## Оговорки

- Единственная точка вызова тика (`~39674`, сразу после
  `_try_dispatch_prematch_model_bet`) — ПРОВЕРЯЕТСЯ: план упоминал «из трёх
  веток» по аналогии с `_try_dispatch_prematch_model_bet` (ранние локальные
  метрики / star-ветка / без звёзд), но в текущем коде найден только один
  вызов `_ml_dispatch_tick(` (grep, `cyberscore_try.py:39713`). Если тик
  реально должен стоять и в других двух ветках прематч-диспатча — это
  расхождение план/код, требует подтверждения владельцем перед `shadow`.
- Ценовой пол ML-ставки (`min_odds = 1/(expected_wr-margin)`, margin по
  умолчанию 0.0) — тот же «нулевой пол без запаса», что уже описан в памяти
  `bet-price-floor-has-zero-margin.md` для STAR-ставок; `ML_DISPATCH_MIN_ODDS_MARGIN`
  существует именно чтобы это исправить, но default пока не даёт запаса —
  ПРОВЕРЯЕТСЯ отдельно перед боевым включением `ml`.
- Окно kills_window (`band_start-180` lead, `band_start-120` deadline) —
  ПРОВЕРЯЕТСЯ соответствие `KILLS_WINDOW_POLICY` (первое окно с
  `band_start=0` никогда не открывается для этого маркета — намеренно, но не
  перепроверено на реальном потоке).
- Модели не независимы: E-282 показал корреляцию `early_nw↔early_win=0.765`
  (общие входные признаки) — поддержка win-маркета через
  `late,all,early_win` не три независимых голоса, а скорее два с половиной.
- Прод-числа shadow-режима отсутствуют на момент записи (`status:
  "in-progress"`) — все выводы о качестве взяты из офлайн E-282/E-283, не из
  живого потока карт.

## Команды запуска

```bash
venv_catboost/bin/python3 -m pytest base/tests/test_ml_dispatch.py base/tests/test_dispatch_mode_gate.py -q
```

Смоук `evaluate()` без сети/сервера:

```bash
venv_catboost/bin/python3 - <<'PY'
from base import ml_dispatch as md
cfg = md.Config.from_env({})
ctx = md.Ctx(
    match_key="smoke", base_url="smoke", map_num=1, game_time=300.0,
    radiant_team="R", dire_team="D", heroes=None,
    elo_radiant=1500.0, elo_dire=1400.0,
    early_nw=md.ModelVerdict("Dire", 0.65), early_win=None,
    late=md.ModelVerdict("Dire", 0.70), all=md.ModelVerdict("Dire", 0.68),
    lane=None, prematch_index=None, kills_windows_open=[], already_sent=set(),
)
result = md.evaluate(ctx, cfg)
print(result.decisions, result.skipped, result.underdog_side)
PY
```

## Где искать ошибку

- **Ориентация сторон Radiant/Dire**: `verdicts()` в `laning_serving.py`
  строго возвращает `"Radiant"`/`"Dire"` (никогда `"tie"`), но `heroes`
  вектор собирается `win_model_veto._heroes_vector(radiant_heroes_and_pos,
  dire_heroes_and_pos)` — если порядок аргументов где-то перепутан, все пять
  вердиктов синхронно смотрят не в ту сторону, и это не всплывёт как
  `model_missing`/`conflict`, а даст тихо противоположные ставки.
- **Дедуп-ключ `base_url`**: `_signal_fingerprint_registry_key(match_key)` —
  если два разных матча схлопываются в один `base_url` (или наоборот, один
  матч даёт разные `base_url` между тиками), `SentLedger` либо не дедуплицирует
  повторные ставки, либо блокирует новую карту как «уже отправленную».
- **Env читается при вызове, не при импорте**: `Config.from_env()` и
  `dispatch_mode()` оба читают `os.environ`/`env` заново на каждый вызов —
  если где-то в коде `Config`/`dispatch_mode()` закэшированы в модульную
  переменную при импорте, systemd drop-in перестанет применяться без
  перезапуска процесса (проверить grep на `_DISPATCH_MODE_CACHE` или похожее,
  которого сейчас нет, но могло бы появиться при рефакторинге).
- **Регэкспы панели со ★**: `_format_win_model_line` и `laning_serving.panel_lines`
  читают `ML_DISPATCH_MIN_CONF` независимо друг от друга (два разных куска
  кода, один и тот же env-ключ) — рассинхрон дефолтов между ними даст ★ на
  разных порогах для Early NW/Late вместо All/ML Laning, что незаметно на
  глаз, если пороги близки.
- **Таймфрейм тика**: `_ml_dispatch_tick` вызывается один раз в основном
  цикле опроса карты — если поле `game_time_seconds` приходит с задержкой
  (см. память `map-end-is-seen-20-minutes-late.md` про похожий класс
  проблем в другом месте pipeline), `_timing_for_win`/`_ml_dispatch_open_kills_windows`
  могут посчитать окно открытым/закрытым по устаревшему времени.

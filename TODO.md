# TODO — Аудит кода (scribe, 2026-06-03)

> Найдено при обновлении docs/. Исправлять по одному, с ревью после каждого этапа.

## Критично — latent NameError

- [ ] **`_get_kills_draft_predictor` не определён** — `base/cyberscore_try.py:16019,16261`
- [ ] **`POSITION_ORDER` не определён** — `base/cyberscore_try.py:20079`
- [ ] **`match_id` не в скоупе** — `base/cyberscore_try.py:20887-20888`
- [ ] **Мёртвый `_create_driver`** — `base/dota2protracker.py:406-422`

## Чистка

- [ ] Неиспользуемые импорты (cyberscore_try.py, dota2protracker.py, functions.py, check_old_maps.py)
- [ ] Мёртвые локальные переменные + бесполезные global/nonlocal
- [ ] f-строки без плейсхолдеров (особенно metrics_winrate.py)

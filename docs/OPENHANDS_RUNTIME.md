> Generated manually on 2026-07-24 against commit 87049d4 plus the current uncommitted OpenHands gateway source. If code contradicts this document, trust the code.

# OpenHands Telegram Runtime

## Назначение

`runtime/openhands-universal-gateway/` заменяет самописный обязательный RuFlo planner/DAG fan-out на persistent end-to-end OpenHands developer. С 2026-07-24 OpenHands обслуживает все три production-бота; `ruflo-universal-gateway.service` и три legacy-оркестратора остановлены, отключены и сохранены только для rollback.

## Поток

1. Telegram adapter строит `session_key = bot_id + chat_id + topic_id`.
2. Per-bot ledger возвращает стабильный UUID OpenHands conversation.
3. При первом OpenHands-start каждого slot новый ledger одноразово импортирует Telegram offset из legacy per-bot ledger, чтобы уже обработанные updates не проигрывались повторно; последующие старты сохраняют собственный offset OpenHands.
4. `LocalConversation` загружает предыдущие events, tool observations и `TASKS.md`; новый run продолжает progress, а не начинает исследование заново.
5. Leader выполняет задачу end-to-end. Обязательного вызова трёх planners нет.
6. При необходимости Leader вызывает только `worker_qwen`.
7. После finish отдельная read-only Reviewer conversation проверяет workspace. `ISSUES` возвращаются в ту же Leader conversation; максимум 3 цикла. Только `APPROVE` отправляет финальный ответ.

## Модели

| Role | Policy |
|---|---|
| Leader / Commander | `fable/claude-fable-5` high → `codex/gpt-5.6-sol-xhigh` xhigh → `opencode-go/kimi-k3` max |
| Reviewer | тот же ordered fallback; custom `AvailabilityFallbackStrategy` также считает route-level 403/404/502/invalid JSON недоступностью primary |
| Worker | только `qoder/qwen3.8-max-preview`, effort high; fallback отсутствует |

Worker policy — 10 retries после первого запроса, то есть 11 total attempts. Между retry используются все интервалы `1,2,4,8,16,32,64,128,256,512` секунд. Retry охватывает transport, timeout, 429, 502/503/internal errors, no response и invalid provider JSON/schema. Ошибка worker прерывает parent conversation, сохраняет оба progress tree и явно сообщает отсутствие fallback.

## Live guidance и resume

- Текст или attachment во время active run сразу добавляется через `send_message()` в ту же conversation.
- `/stop` вызывает `interrupt()` и переводит run в `PAUSED`; conversation не удаляется.
- Следующее сообщение, включая «Продолжи», использует тот же UUID.
- `/new` создаёт новый UUID; старые events не удаляются.
- При первом OpenHands run adapter может подмешать последние RuFlo prompt/final/error/artifacts как `<LEGACY_PROGRESS>`, после чего source of truth — OpenHands persistence.

## Изоляция

Каждый slot имеет отдельный root mode `0700`:

```text
/root/.local/state/openhands-universal-gateway/orchestrationN/
  conversations/
  workers/
  reviews/
  profiles/
  attachments/
  cipher-key
  ledger.sqlite3
```

Conversation ID — UUID, а не Telegram topic number. Ledgers/profiles/cipher keys физически раздельны. Однако `LocalWorkspace=/root/main` общий: без Docker это state isolation, не полноценный filesystem sandbox. Docker/worktree sandbox — отдельный этап после освобождения диска.

## Credentials и selected slots

Gateway загружает только профили, относящиеся к `OPENHANDS_BOT_SLOTS`.

- Telegram tokens: encrypted systemd credentials `bot1-token`, `bot2-token` и
  `bot3-token`.
- OmniRoute API key: root-only systemd credential `omniroute-api-key`, источник
  `/root/.omniroute/local_api_key`.
- Telegram allowlist: `OPENHANDS_ALLOWED_USERS`; он не смешивается с API key или
  Telegram token.
- Legacy `BOT_1_ENV_FILE`/`BOT_2_ENV_FILE` остаются необязательным fallback;
  production-unit от них не зависит.

Это позволяет безопасно проверять и перезапускать любой slot после вывода из
эксплуатации старых Hermes profile-файлов.

## Проверки

```bash
python3 -m unittest discover -s runtime/openhands-universal-gateway -p 'test_*.py' -v

OPENHANDS_SUPPRESS_BANNER=1 \
PYTHONPATH=runtime/openhands-universal-gateway \
/opt/openhands-universal-gateway/venv/bin/python \
runtime/openhands-universal-gateway/gateway.py --check
```

Unit tests не требуют OpenHands package: backend fake. Дополнительно native smoke должен проверить create, real model response и cold resume одного UUID. Не запускать Telegram polling smoke параллельно с другим service на том же bot token.

## Cutover

`runtime/systemd/openhands-universal-gateway.service` по умолчанию содержит:

```ini
Environment=OPENHANDS_BOT_SLOTS=orchestration1,orchestration2,orchestration3
```

Нельзя запускать два `getUpdates` poller на одном Telegram token: updates будут
конкурировать и теряться. RuFlo сохранён только для rollback и должен оставаться
остановленным, пока production обслуживает OpenHands.

## Текущее production-состояние

На 2026-07-24 runtime атомарно опубликован в `/opt/openhands-universal-gateway`, а
`openhands-universal-gateway.service` активен для `orchestration1`,
`orchestration2` и `orchestration3`. Все три Telegram-токена передаются только
как encrypted systemd credentials. Удалённые Hermes profile-файлы не блокируют
перезапуск. Перед cutover проверено отсутствие active RuFlo runs; legacy offsets
импортируются одноразово, поэтому старые updates не проигрываются повторно.
`ruflo-universal-gateway.service` и
`ruflo-orchestrator@orchestration{1,2,3}.service` остановлены и отключены.
Rollback-копии имеют stamp `20260723T133555Z` в `/etc/systemd/system/` и
`/opt/ruflo-universal-gateway/`; дополнительные snapshots находятся в
`runtime/openhands-migration-20260724/rollback-pre-selected-slot-fix` и
`runtime/openhands-migration-20260724/rollback-pre-all-slots-cutover`.

Native smoke 2026-07-24 успешно проверил create, реальный model response,
отдельный Reviewer, persistent storage и cold resume одного conversation UUID
`2bdf244a-1c23-4a20-b24f-7bb1c036f0f2`. Во время smoke primary Fable сначала
вернул `503`, Codex fallback сообщил об истёкшей авторизации, после чего Kimi
fallback успешно завершил Leader и Reviewer; позднее Fable снова отвечал.
Fallback-контракт работает. После smoke в OmniRoute подключены четыре активных
Codex OAuth-аккаунта.

Filesystem на 2026-07-24 показывает около `41 GB` свободного места (`67%` used);
предыдущий disk-space blocker снят без удаления runtime state. Финальный
preflight и deployed suite прошли 11/11 тестов; после cutover service имеет
`NRestarts=0`, а в журнале нет `409 Conflict`, traceback или runtime errors.

# Telegram bridge: OpenAB + Cursor Agent

> Актуально на 2026-07-11. Документ описывает production-сервис на `root@23.26.193.167`.

## Назначение и текущий статус

Telegram bridge позволяет управлять официальным Cursor Agent через личные сообщения Telegram.

```text
Telegram DM -> OpenAB -> cursor-agent -> Cursor backend
                                  |
                                  +-> root-only US proxy
```

- Bridge: OpenAB `0.1.9` с локальным hardening-патчем.
- Cursor Agent: `2026.07.09-a3815c0`.
- Модель: `grok-4.5-fast-high`.
- `high` уже является частью model ID; суффикс `[effort=high]` для этой модели CLI не принимает.
- Прямой xAI API не используется.
- Telegram работает через long polling; одновременно должен работать ровно один poller.

## Production-пути

| Назначение | Путь |
|---|---|
| systemd unit | `/etc/systemd/system/openab-telegram-candidate.service` |
| OpenAB package | `/opt/openab/releases/0.1.9-candidate-20260710T144815` |
| OpenAB config | `/etc/openab/config-candidate.json` |
| Cursor/proxy environment | `/etc/openab/cursor-proxy.env` |
| Cursor CLI | `/root/.local/bin/cursor-agent` |
| Workspace | `/root/main` |
| Emergency rollback | `/root/backups/openab-fix-20260710T144815+0300/rollback.sh` |

Config и proxy environment должны принадлежать `root:root` и иметь режим `0600`. Не выводить их содержимое через `cat`, `systemctl show Environment`, `/proc/<pid>/environ` или диагностические команды.

## Доступ и полномочия

Bridge принимает команды только при одновременном выполнении условий:

- сообщение пришло в личном чате;
- Telegram user находится в allowlist;
- `telegram.allow_all=false`.

Cursor запускается от `root` с полной автономией:

```text
--model grok-4.5-fast-high --trust --force --approve-mcps
```

Обычный текст и `/run <задача>` выполняются без дополнительного подтверждения. Агент может читать и изменять файлы, выполнять shell-команды и обращаться к MCP-серверам с root-правами. Доступ к Telegram-боту следует считать эквивалентом root-доступа к серверу.

## Поведение Telegram-бота

- Обычный текст — запустить задачу Cursor.
- `/run <задача>` — то же выполнение с полной автономией, без approval-кнопки.
- `/cancel` — отменить активную задачу и завершить всю process group Cursor.
- Если задача уже выполняется, новый запрос получает `busy`; второй Cursor-процесс не запускается.
- Сообщения из групп, supergroup и каналов игнорируются.

## Управление systemd

```bash
# Статус
systemctl status openab-telegram-candidate.service --no-pager
systemctl is-active openab-telegram-candidate.service
systemctl is-enabled openab-telegram-candidate.service

# Перезапуск
systemctl restart openab-telegram-candidate.service

# Остановка и запуск
systemctl stop openab-telegram-candidate.service
systemctl start openab-telegram-candidate.service

# Последние логи
journalctl -u openab-telegram-candidate.service -n 200 --no-pager
journalctl -u openab-telegram-candidate.service -f
```

После рестарта проверить:

```bash
systemctl show openab-telegram-candidate.service \
  -p ActiveState -p SubState -p MainPID -p NRestarts --no-pager

pgrep -af 'python3 -m openab'
journalctl -u openab-telegram-candidate.service --since '-5 minutes' --no-pager \
  | grep -E '409|Conflict|Traceback|ERROR'
```

Исправное состояние: `active/running`, `NRestarts=0`, один OpenAB process/poller, отсутствуют `409 Conflict`, traceback и restart loop.

## Проверка через Telegram

1. Отправить боту: `Ответь ровно OPENAB_LIVE_OK`.
2. Убедиться, что ответ пришёл от `grok-4.5-fast-high`.
3. Запустить долгую безопасную задачу и отправить второй запрос — бот должен вернуть `busy`.
4. Во время долгой задачи отправить `/cancel`.
5. После отмены отправить новую задачу — она должна запуститься нормально.

## Proxy и Cursor authentication

Cursor/Grok доступен через основной US proxy. Proxy применяется только к дочернему `cursor-agent`; Telegram-транспорт не должен использовать этот proxy.

- Секреты proxy находятся только в `/etc/openab/cursor-proxy.env`.
- Не помещать proxy URL, IP, логин или пароль в git, документацию, unit-файл или журнал.
- Резервный endpoint не используется автоматически, пока не проходит transport-проверку.
- Слепой retry после отправки agent-запроса запрещён: он может дважды выполнить команды.

Безопасная проверка Cursor authentication:

```bash
/root/.local/bin/cursor-agent status
/root/.local/bin/cursor-agent --version
```

Вывод `status` может содержать email аккаунта; не публиковать его в общих логах или отчётах.

## Типовые проблемы

### `409 Conflict`

Запущено два Telegram poller. Остановить лишний bridge и оставить только `openab-telegram-candidate.service`.

Старые units не должны работать параллельно с OpenAB:

```bash
systemctl is-active opencode-telegram-bridge.service
systemctl is-active opencode.service
```

В штатном состоянии оба старых unit отключены; `opencode-telegram-bridge.service` может отображаться как `failed` из-за старого дефекта graceful shutdown, но его `MainPID` должен быть `0`.

### Cursor `401/403` или модель недоступна

- проверить `/root/.local/bin/cursor-agent status`;
- проверить доступность основного proxy без вывода credentials;
- проверить, что config использует `grok-4.5-fast-high`;
- не переключаться на xAI и не менять модель молча.

### Задача долго выполняется

- отправить `/cancel`;
- проверить дочерние Cursor-процессы;
- убедиться, что после отмены process group завершилась;
- проверить journal на timeout/network errors.

### Restart loop

```bash
systemctl status openab-telegram-candidate.service --no-pager
systemctl show openab-telegram-candidate.service -p NRestarts --value
journalctl -u openab-telegram-candidate.service -n 200 --no-pager
```

## Секреты и логирование

В документацию и диагностический вывод запрещено включать:

- Telegram bot token;
- Telegram user/chat ID;
- proxy endpoint, IP, логин или пароль;
- Cursor authentication tokens;
- содержимое `/etc/openab/cursor-proxy.env` и secret-полей config.

В OpenAB включено подавление URL-логов `httpx/httpcore` и redaction Telegram token. После обновления OpenAB или библиотек Telegram повторно проверить, что свежий journal не содержит Bot API URL с token.

## Обновление bridge

1. Не редактировать установленный active package вслепую.
2. Подготовить новую release/candidate-директорию.
3. Сохранить DM-only, allowlist, `busy`, `/cancel`, process-group cleanup и redaction.
4. Проверить явную модель и флаги автономии.
5. Обновить `PYTHONPATH` в unit.
6. Выполнить `systemd-analyze verify`.
7. Перезапустить только OpenAB unit.
8. Проверить один poller, `NRestarts`, `409`, ошибки и Telegram smoke-test.

## Rollback

Не запускать старый и новый Telegram bridge одновременно.

```bash
systemctl stop openab-telegram-candidate.service
/root/backups/openab-fix-20260710T144815+0300/rollback.sh
```

После rollback проверить, что работает ровно один poller и в journal отсутствует `409 Conflict`. Не удалять staging, старые units, логи или rollback-артефакты без отдельного решения.


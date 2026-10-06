# Pro-chain Python scripts
Скопированы 02.10.2026 из `runtime/experiments/misc/` основного checkout.
Исходные SHA-1 и размеры сохранены без изменений в `SOURCES.sha1`.
Класс правок 1: переносимые пути и локальные импорты соседних скриптов.
Корень данных задаёт `DRAFT_ROOT`; без него используется корень checkout.
Импорты модулей `base/` по-прежнему используют этот корень данных.
Примеры запуска указывают на `scripts/pro_chain/`.
Класс правок 2: чтение частей `.json` и `.json.gz`, включая подсчёт файлов.
Порядок частей при извлечении одинаковый; вычисления сохранены.
Оригиналы в `runtime/experiments/misc/` больше не используются цепочкой.

## ID backfill after the team topup
`topup_pro_corpus.py` calls `backfill_by_id.py` under its existing lock and restores
visited teams even if backfill fails. OpenDota pages back to the same `TOPUP_DAYS`
window (default 10 days, cap 15 pages); Stratz batches use the shared proxy pool.
Only recent parts are read; a migration that resets mtimes does not force a full
scan: unchanged cached parts of patch buckets that ended before the window and
the outside-patch bucket (`historical`, when the patch table is gap-free and
open-ended) are skipped, their IDs come from the manifest/`processed_ids.txt`.
Incomplete records are replaced in their original
JSON/gzip part. New parsed maps use the existing patch buckets, 500 MiB rotation,
`PRO_CORPUS_GZIP`, counters, processed-ID cache and scan manifest. Null matches
request `retryMatchDownload` at most once per three days; the attempt date is
saved before the request in `backfill_retry_download.json`. HTTP 429/522 stops
the step. A partial publication is recovered by the next recent-part scan.

Read-only audit (UTC date):
`python3 scripts/pro_chain/backfill_by_id.py --since 2026-08-25 --dry-run --max-pages 9 --max-stratz-calls 9`
`--corpus-dir` overrides the default derived from `maps_research.PRO_HEROES_DIR`.
Dry-run never writes files or sends mutations; its write counts are zero.
`fetched` includes still-unparsed non-null records. The Stratz cap counts physical
POSTs, including internal retries. Manual writes acquire the same topup lock.
`SOURCES.sha1` remains the original pre-copy inventory, not current-file hashes.

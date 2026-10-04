#!/bin/bash
# Ночная пересборка снимка предматчевой модели и доставка на serv1.
#
# Одна цепочка, две машины (scripts/run/lib_pro_chain.sh):
#   remote (Мак)  — сборка в этом checkout, прод = serv1:/root/main по ssh/scp;
#   local  (serv1) — сборка в ОТДЕЛЬНОМ checkout (/root/pro_chain), прод =
#                    /root/main на той же машине по cp/bash.
# PRO_CHAIN_SHADOW=1 — собрать всё и ничего не доставить (ни записей под
# $PROD_ROOT, ни перебазировки, ни systemctl); что уехало бы — в $PRO_CHAIN_SUMMARY.
#
# Зачем каждую ночь. Два признака в снимке привязаны ко времени и молча
# протухают: `wr30` — окно 30 дней, `vs_wr` — распад с полураспадом 45 дней.
# Снимок недельной давности означает «винрейт за 30 дней, закончившихся неделю
# назад», и модель об этом не сообщит — она посчитает и вернёт число.
#
# Цена устаревания ИЗМЕРЕНА (E-177, сквозной прогон боевого пути через снимок,
# обрезанный по границе теста): AUC 0.7313 на снимке моложе трёх суток против
# 0.6883 на снимке старше месяца. Это дороже всех расхождений определений вместе
# взятых, поэтому свежесть снимка — не гигиена, а деньги.
#
# Почему не в боевом checkout. Снимок собирается в отдельном дереве (Мак — его
# checkout, serv1 — /root/pro_chain) и уезжает на прод готовым, с проверкой sha1:
# сборка внутри /root/main подменила бы боевые файлы недособранными. Про-корпус
# живёт в дереве сборки (`pro_heroes_data/` закрыт .gitignore), rsync кода между
# машинами мы не используем.
#
# ВЕСА НЕ ПЕРЕОБУЧАЮТСЯ. Пересобирается только снимок; коэффициенты, нормировки
# и набор признаков берутся из предыдущего артефакта через `finalize_artifact`.
# Переобучение — отдельная редкая операция, у неё свои замеры.
#
# Порядок: выжимки корпуса -> снимок v1 -> добавки v2 -> живые карты E-168 ->
# опознание организаций -> сборка v3 -> доставка -> РЕСТАРТ ПРОДА.
#
# Чего скрипт НЕ делает: не добирает свежие про-матчи в корпус. Он приводит
# снимок в соответствие с тем корпусом, который лежит на диске; наполнение
# корпуса — отдельный процесс (`base/maps_research.py`).
#
# Рестарт обязателен: `prematch_scorer.get_model()` держит модель в модульном
# синглтоне `_MODEL`, поэтому новый файл без перезапуска процесса не читается.
set -e
# Общая обвязка: ROOT (дерево сборки), PROD_ROOT, SERV1 (ssh-алиас, IP не писать:
# зашитый IP устарел при переезде serv1 22.09.2026 и доставка молча падала 4 ночи),
# PY, PRO_CHAIN_MODE, PRO_CHAIN_SHADOW и prod_*-помощники.
source "$(dirname "${BASH_SOURCE[0]}")/lib_pro_chain.sh"
cd "$ROOT"
LOG="${LOG:-runtime/prematch_rebuild_$(date +%Y%m%d_%H%M).log}"
pro_chain_guard || exit 2
# Дочерние скрипты (добор, kv3) и дочерний `bash "$0" --run-chain` видят то же.
export PRO_CHAIN_MODE PROD_ROOT PRO_CHAIN_SHADOW PRO_CHAIN_SUMMARY
mkdir -p runtime runtime/artifacts/misc ELO/output

# The rebuild child is a process-group leader (run_with_notify below). The
# watchdog has its own group, so it can kill all build descendants and exit,
# leaving the notifying parent and cyberscore untouched. No watchdog on the Mac.
WATCHDOG_PID=""
stop_memory_watchdog() {
  if [ -n "$WATCHDOG_PID" ]; then
    kill -TERM -- "-$WATCHDOG_PID" 2>/dev/null || :
    wait "$WATCHDOG_PID" 2>/dev/null || :
    WATCHDOG_PID=""
  fi
}

memory_watchdog() {
  set +m
  local chain_pid="$1" available
  while kill -0 "$chain_pid" 2>/dev/null; do
    sleep "${PRO_CHAIN_WATCHDOG_INTERVAL_SECONDS:-20}"
    kill -0 "$chain_pid" 2>/dev/null || return 0
    if live_map_active; then
      available="$(awk '/^MemAvailable:/ {print $2; found=1; exit} END {if (!found) exit 1}' \
        "${PRO_CHAIN_MEMINFO_PATH:-/proc/meminfo}")" || {
        echo "ВНИМАНИЕ: watchdog памяти — MemAvailable недоступен"; continue; }
      if [ "$available" -lt "${PRO_CHAIN_MIN_AVAILABLE_KB:-2097152}" ]; then
        echo "ОШИБКА: watchdog памяти — карта живая или состояние неизвестно, MemAvailable=${available} kB; завершаю группу пересборки $chain_pid"
        kill -KILL -- "-$chain_pid"
        return 0
      fi
    fi
  done
}

run_chain() {
  ELO_SNAPSHOT_STAGED=0
  echo "=== $(date '+%F %T') пересборка снимка предматчевой модели ==="
  if [ "$PRO_CHAIN_MODE" = local ] && [ -r "${PRO_CHAIN_MEMINFO_PATH:-/proc/meminfo}" ]; then
    set -m
    memory_watchdog "$$" &
    WATCHDOG_PID=$!
    set +m
    trap 'stop_memory_watchdog' EXIT
    trap 'exit 143' TERM
    trap 'exit 130' INT
    echo "watchdog памяти: PID=$WATCHDOG_PID"
  fi
  # Overlay динамического tier2-onboarding'а (см. base/tier_dynamic_overlay.py)
  # пишется рантаймом ТОЛЬКО на serv1: локальный добор без него не видит
  # команды, онбордженные после 02.09.2026 (Uralan, клубы WINLINE Star Series),
  # и никогда не берёт их карты как сиды. Забор ограничен по времени и
  # некритичен: падение оставляет локальный overlay как есть (или отсутствующим)
  # — `_seed_team_ids()` тогда отдаёт только статические сиды, без падения.
  TIER2_OVERLAY_LOCAL="base/id_to_names_dynamic_tier2.json"
  prod_fetch base/id_to_names_dynamic_tier2.json "$TIER2_OVERLAY_LOCAL.tmp" \
    && mv "$TIER2_OVERLAY_LOCAL.tmp" "$TIER2_OVERLAY_LOCAL" \
    || echo "ВНИМАНИЕ: overlay tier2 с serv1 не получен; сиды только статические"
  # Страховка от молчащего добора: 25.08–01.09.2026 launchd не запускал 04:30-джобу
  # 8 ночей, и снимок уезжал на прод с корпусом 23.08 (sha1 не менялся). Если за
  # 20 ч лога добора нет — добираем здесь; падение добора пересборку не останавливает.
  if [ -z "$(find runtime -maxdepth 1 -name 'pro_topup_*.log' -mmin -1200 2>/dev/null)" ]; then
    echo "свежего лога добора нет (>20 ч) — добираю корпус перед пересборкой"
    bash scripts/run/topup_pro_corpus.sh || echo "добор упал (rc=$?), пересобираю на текущем корпусе"
  fi
  # 1. выжимки корпуса (компактная и богатая)
  $PY scripts/pro_chain/pro_corpus_extract.py
  $PY scripts/pro_chain/pro_corpus_rich.py
  # 2. снимок: v1 -> добавки v2. Оба шага обязаны видеть один и тот же корпус.
  #    v2 идёт в режиме ТОЛЬКО СНИМОК: матрица признаков и переобучение весов
  #    требуют кэшей (`ideas_batch*.npz`, EXT, драфт-логит), а они привязаны к
  #    длине корпуса — как только корпус подрос, склейка падает по форме. Веса
  #    устаревают куда медленнее снимка и обновляются отдельно, с замером.
  PREMATCH_SNAPSHOT_ONLY=1 $PY scripts/pro_chain/build_prematch_artifact.py
  PREMATCH_SNAPSHOT_ONLY=1 $PY scripts/pro_chain/build_prematch_artifact_v2.py
  # 3. состояние под шесть колонок E-168: отклонения урона и нетворса по
  #    аккаунту и позиционные ячейки контрпика/синергии
  $PY scripts/pro_chain/add_live_maps.py
  # 4. опознание организаций по составу + история личных встреч
  $PY scripts/pro_chain/add_org_identity.py
  # 5. сборка боевого артефакта: снимок + веса из отдельного файла весов.
  #    ИМЯ ВЫХОДА ЗАДАЁТСЯ ЯВНО. До 15.08 шаг молча писал в
  #    `prematch_model_artifact_v3_nohybrid.npz` (умолчание finalize_artifact),
  #    а шаги 6-7 проверяли и отправляли `_v3_hybrid.npz`, которого в этой
  #    цепочке никто не писал. Пересборка отрабатывала каждую ночь без ошибок и
  #    доставляла на прод один и тот же файл от 14.08: снимок 11.08 и 354 тыс.
  #    аккаунтов вместо 1.55 млн. Именно это и выглядело как «модель бесполезна»:
  #    половина составов ей неизвестна, и она отказывается считать.
  PREMATCH_SRC=runtime/artifacts/misc/prematch_model_artifact_v2_snapshot.npz \
  PREMATCH_OUT=runtime/artifacts/misc/prematch_model_artifact_v3_hybrid.npz \
    $PY scripts/pro_chain/finalize_artifact.py
  # 5b. справочник написаний из цепочек ПЕРЕИМЕНОВАНИЙ (для поиска карточки у
  #     букмекера). Имена берём с прода: записи вида `tier_two_teams['ironwing']`
  #     дописывает рантайм на serv1, и локальная копия про новые теги не знает.
  #     С 02.09.2026 рантайм пишет их в JSON-overlay рядом со справочником —
  #     забираем оба файла (overlay на свежем проде может ещё отсутствовать).
  NAMES_DIR="$(mktemp -d)"
  prod_fetch base/id_to_names.py "$NAMES_DIR/id_to_names.py"
  prod_fetch base/id_to_names_dynamic_tier2.json "$NAMES_DIR/" || true
  TEAM_NAMES_DIR="$NAMES_DIR" $PY base/tools/build_team_org_aliases.py
  rm -rf "$NAMES_DIR"

  # 5c. ELO-снимок: рейтинги команд и история килов, из которой kills27-shadow
  #     берёт ростерные выборки. До 02.09.2026 снимок собирался ВРУЧНУЮ и возился
  #     файлом: на проде лежал срез 11.08 (569 493 матча, 242 МБ), пока локальный
  #     корпус вырос до 1 395 282 (срез 01.09, 646 МБ) — рекомендации в Telegram
  #     считались на ростер-истории трёхнедельной давности (E-249). Пересборка
  #     ~11 мин; отказ НЕ рвёт цепочку (доставка предматчевого артефакта важнее),
  #     а молчаливое протухание ловит freshness-watchdog.
  wait_no_live_map "${PRO_CHAIN_HEAVY_WAIT_SECONDS:-2700}" "ELO-снимок"
  if $PY ELO/live_team_strength.py --snapshot-path ELO/output/live_team_elo_snapshot.json; then
    # prod_stage кладёт файл в `<прод>/…json.tmp` и сверяет sha1; при расхождении
    # сам удаляет .tmp и возвращает 1. В тени только пишет строку в summary.
    L=$(sha1_of ELO/output/live_team_elo_snapshot.json)
    if prod_stage ELO/output/live_team_elo_snapshot.json ELO/output/live_team_elo_snapshot.json; then
      ELO_SNAPSHOT_STAGED=1
      echo "ELO-снимок подготовлен: sha1 $L; замена после сохранения живых результатов"
    else
      echo "ВНИМАНИЕ: ELO-снимок доехал битым (против $L) — на проде остался прежний"
    fi
  else
    echo "ВНИМАНИЕ: ELO-снимок не пересобрался — на проде останется прежний"
  fi

  # 6. проверка ПЕРЕД доставкой: артефакт обязан читаться и содержать тот же
  #    набор признаков, что и боевой. Иначе прод останется на старом файле.
  $PY - <<'CHECK'
import sys, time, numpy as np
from pathlib import Path
R = Path.cwd()  # дерево сборки: скрипт сделал cd "$ROOT"
sys.path.insert(0, str(R / "base"))
import prematch_scorer as ps
new = R / "runtime/artifacts/misc/prematch_model_artifact_v3_hybrid.npz"
# Файл ОБЯЗАН быть написан этим прогоном. Проверка структуры такого не ловит: с
# 14.08 по 15.08 цепочка отправляла на прод файл, который сама не собирала
# (шаг 5 писал под другим именем), и все структурные проверки проходили (E-193).
age_min = (time.time() - new.stat().st_mtime) / 60.0
assert age_min <= 120, f"артефакт написан {age_min:.0f} минут назад — это не результат этого прогона"
m = ps.PrematchModel(new)
snap_days = (time.time() - m.snapshot_ts) / 86400.0
assert snap_days <= 10, f"снимок старше 10 суток ({snap_days:.1f}) — корпус не пополняется"
if snap_days > 3:
    print(f"ВНИМАНИЕ: снимку {snap_days:.1f} суток, корпус пора пополнить "
          f"(base/maps_research.py); по E-177 это стоит до 0.04 AUC")
assert len(m.features) == len(m.coef[0]), (len(m.features), len(m.coef[0]))
need = ("hybrid_strength", "cp_lane", "syn_pos_mean",
        "a_hdmg_rel_pos", "a_hdmg_rel_hero", "a_nw_rel_pos")
missing = [n for n in need if n not in m.features]
assert not missing, f"в новом артефакте нет колонок: {missing}"
acc = np.load(new)["accounts"]
assert acc.shape[1] >= 19, f"в accounts {acc.shape[1]} колонок, ожидалось >= 19"
print(f"проверка пройдена: признаков {len(m.features)}, колонок в accounts {acc.shape[1]}, "
      f"аккаунтов {len(m.acc):,}, моделей {len(m.coef)}, снимку {snap_days:.1f} суток")
CHECK

  # 7. доставка. Атомарно: пишем .tmp и переименовываем поверх, чтобы прод
  #    никогда не увидел недокачанный файл.
  prod_stage runtime/artifacts/misc/prematch_model_artifact_v3_hybrid.npz \
      data/prematch_model_artifact_v3.npz \
    || { echo "ОШИБКА: артефакт не доставлен (.tmp не совпал по sha1), рестарт не делаю"; exit 1; }
  prod_commit data/prematch_model_artifact_v3.npz
  # Сверка ПОСЛЕ доставки: единственная проверка, которая поймала бы E-193.
  # Совпадение sha1 локального и боевого файла — доказательство, что уехало
  # именно то, что собрано, а не одноимённый файл прошлой недели.
  LOCAL_SHA=$(sha1_of runtime/artifacts/misc/prematch_model_artifact_v3_hybrid.npz)
  if shadow_on; then
    # В тени на проде ничего не меняется — сверять боевой файл не с чем.
    echo "[тень] сверку боевого артефакта пропускаю: sha1 собранного $LOCAL_SHA"
  else
    REMOTE_SHA=$(prod_sha1 data/prematch_model_artifact_v3.npz)
    if [ "$LOCAL_SHA" != "$REMOTE_SHA" ]; then
      echo "ОШИБКА: на сервере другой файл ($REMOTE_SHA против $LOCAL_SHA), рестарт не делаю"
      exit 1
    fi
    echo "доставка подтверждена: sha1 $LOCAL_SHA"
  fi
  prod_stage data/team_org_aliases.json data/team_org_aliases.json || exit 1
  prod_commit data/team_org_aliases.json

  # --- снимки предматчевой панели -----------------------------------------
  # Панель на serv1 читает три снимка накопленного состояния, а пересобрать их
  # там НЕЛЬЗЯ: ни сборщиков, ни про-корпуса на боевой машине нет. Значит их
  # строит эта машина и привозит сюда же. Без этого они просто гниют: 19.08.2026
  # при переносе провайдеров им было 7-8 суток, а порог предупреждения 14, и у
  # `pair_priors` предупреждения нет вовсе.
  #
  # ОТКАЗ ЗДЕСЬ НЕ ДОЛЖЕН РОНЯТЬ ЦЕПОЧКУ. Выше едет предматчевый артефакт — это
  # деньги; панель только показывает числа. Поэтому каждый шаг обёрнут и в
  # худшем случае оставляет вчерашний снимок, а не срывает доставку модели.
  panel_snapshots() {
    $PY scripts/pro_chain/build_prior_snapshot.py
    $PY scripts/pro_chain/build_rating_snapshot.py
    $PY scripts/pro_chain/build_pair_snapshot.py
    for f in prior_snapshot.npz rating_snapshot.npz pair_prior_snapshot.npz; do
      L=$(sha1_of "data/$f")
      if ! prod_stage "data/$f" "data/$f"; then
        echo "снимок $f доехал битым (против $L) — оставляю прежний"
        return 1
      fi
      prod_commit "data/$f"
      if shadow_on; then
        echo "[тень] снимок $f не доставлен: sha1 $L"
      else
        echo "снимок $f доставлен: sha1 $L"
      fi
    done
  }
  if panel_snapshots; then
    echo "снимки панели обновлены"
  else
    echo "ВНИМАНИЕ: снимки панели не обновились, на боевой машине остались прежние"
  fi

  # kills-v3 state модели B боевой панели (E-329). Не фатален для цепочки —
  # при устаревании state старше 3 суток панель показывает старую модель
  # с пометкой « (старая)». Собирается, только если модель B лежит локально;
  # на serv1 уезжает при маркере runtime/kv3_state_deliver.on (включён
  # 24.09.2026). Прод перечитывает файл сам по mtime/размеру.
  if [ -f ml-models/prematch_panel_kv3/manifest.json ]; then
    KV3_ARGS=""
    [ -f runtime/kv3_state_deliver.on ] && KV3_ARGS="--deliver"
    bash scripts/ops/build_kv3_state.sh $KV3_ARGS || echo "ВНИМАНИЕ: kv3 state не обновился (rc=$?)"
  fi

  # --- фактическая доходность по реальным котировкам ----------------------
  # Только ЧТЕНИЕ с боевой машины: сводит журнал отправленных предматчевых
  # ставок с архивом котировок Winline и считает ROI. Раньше это считалось
  # невозможным («архив несоединим», E-103) — на деле не совпадал ключ: в
  # архиве нет `match_id`, join идёт по (имена команд, номер карты).
  # Разовое число тут бессмысленно, выборка мала; смысл в накоплении.
  if $PY scripts/pro_chain/prematch_bet_roi.py > /dev/null 2>&1; then
    echo "доходность пересчитана: runtime/artifacts/misc/prematch_bet_roi.md"
  else
    echo "ВНИМАНИЕ: отчёт о доходности не собрался (не критично, читает только)"
  fi

  # 8. рестарт прода и чистка map_id_check — иначе новый снимок не читается,
  #    а уже разобранные карты не переоцениваются.
  #    Живые результаты после среза должны пережить замену базы. Новый снимок
  #    остаётся .tmp, пока перебазировка не проверит и не сохранит их. При
  #    отказе прод возвращается на прежний снимок, а цепочка сообщает ошибку.
  #    Скрипт идёт как есть и на Маке (ssh), и на serv1 (локальный bash -s): путь
  #    /root/main в нём — боевой checkout на обеих машинах, дерево сборки он не трогает.
  # Disarm and reap before anything can stop prod: a group kill during rebase
  # could otherwise leave cyberscore stopped. Wait on the CHAIN host before
  # opening the prod transaction session; shadow mode never restarts prod.
  stop_memory_watchdog
  echo "watchdog памяти остановлен перед рестартом"
  if ! shadow_on; then
    wait_no_live_map "${PRO_CHAIN_RESTART_WAIT_SECONDS:-5400}" "рестарт cyberscore"
  fi
  prod_script "$ELO_SNAPSHOT_STAGED" <<'ELO_REBASE_REMOTE'
set -e
cd /root/main
snapshot=ELO/output/live_team_elo_snapshot.json
staged_snapshot="$snapshot"
if [ "$1" = 1 ]; then
  staged_snapshot="$snapshot.tmp"
fi
systemctl stop cyberscore.service
if venv/bin/python3 ELO/rebase_runtime_model_state.py --snapshot "$staged_snapshot"; then
  :
else
  rebase_status=$?
  if [ "$rebase_status" -ne 1 ]; then
    echo 'ОШИБКА: целостность runtime ELO не подтверждена; сервис оставлен остановленным'
    exit "$rebase_status"
  fi
  echo 'ОШИБКА: перебазировка ELO отклонена; новый снимок не установлен'
  : > /root/.local/state/ingame/map_id_check.txt
  systemctl start cyberscore.service
  exit 1
fi
if [ "$1" = 1 ]; then
  mv "$staged_snapshot" "$snapshot"
fi
venv/bin/python3 ELO/convert_state_to_delta.py --if-stale || \
  echo 'ВНИМАНИЕ: обновление ELO-дельты не удалось; сохранено полное состояние'
venv/bin/python3 ELO/build_state_arrays.py || \
  echo 'ВНИМАНИЕ: sidecar массивов ELO не собрался'
: > /root/.local/state/ingame/map_id_check.txt
systemctl start cyberscore.service
sleep 3
systemctl is-active cyberscore.service
ELO_REBASE_REMOTE
  # Sidecar собирается ИМЕННО здесь, на только что доставленном снимке и при
  # остановленном проде: сборщик платит потоковым проходом по 616 МБ (~40 c,
  # пик ~1.8 ГБ), а живой процесс потом читает готовые массивы за 0.2 c и
  # ~0.4 ГБ (E-254). Штамп sidecar — mtime_ns и размер источника, поэтому
  # протухший sidecar молча игнорируется, а не отдаёт старые числа.

  # 9. свежесть источников: возраст каждого артефакта, от которого зависят
  #    признаки и гейты (снимок, ELO, дельта, словари, кэши). Шаг НЕ фатален, но
  #    печатает строки «ВНИМАНИЕ: свежесть ...», которые notify_chain ловит
  #    своим grep'ом и шлёт в админ-чат. Без этого протухание молчаливо:
  #    E-193 (8 ночей возился один и тот же артефакт) и E-249 (ELO-снимок отстал
  #    на 22 суток, и никто не сообщил).
  $PY scripts/ops/feature_freshness.py || true

  echo "=== $(date '+%F %T') готово ==="
}

# Под launchd работать НАДО В ПЕРЕДНЕМ ПЛАНЕ. Если уйти в фон и выйти, launchd
# считает задание завершённым и убивает всю группу процессов — цепочка умирает
# через секунду, оставляя пустой лог (проверено 14.08: лог создавался нулевого
# размера, ни одного шага не выполнялось). В терминале, наоборот, удобнее фон.
# Цепочка идёт в ДОЧЕРНЕМ процессе (`bash "$0" --run-chain`): так `set -e` внутри
# неё работает как прежде, а родитель всё равно получает код выхода и шлёт итог
# в админ-чат. В контексте `if`/`||` errexit внутри функции был бы отключён.
if [ "${1:-}" = "--run-chain" ]; then
  run_chain
  exit $?
fi

notify_chain() {
  local rc="$1" pfx=""
  shadow_on && pfx="[тень] "
  if [ "$rc" -ne 0 ] || grep -qE 'ОШИБКА|ВНИМАНИЕ|Traceback|AssertionError|Connection closed|scp:' "$LOG"; then
    { echo "${pfx}⚠️ пересборка снимка: rc=$rc ($(date '+%F %T'))";
      grep -E 'ОШИБКА|ВНИМАНИЕ|Traceback|AssertionError|Connection closed|scp:|снимку|доставка' "$LOG" | tail -8; } \
      | $PY scripts/ops/notify_admin.py
  else
    { echo "${pfx}✅ пересборка снимка ($(date '+%F %T'))";
      grep -E 'проверка пройдена|доставка подтверждена|снимок .* доставлен|^active' "$LOG" | tail -5; } \
      | $PY scripts/ops/notify_admin.py
  fi
}

run_with_notify() {
  local rc=0 chain_pid
  # Bash job control gives the child a separate process group on both 3.2 and
  # 5.2, without depending on Linux-only setsid or using wait -n.
  set -m
  LOG="$LOG" bash "$0" --run-chain &
  chain_pid=$!
  set +m
  # A signal to the notifying parent must not orphan a delivering child or
  # cancel the critical rebase. Keep waiting until the child has finished.
  trap 'wait "$chain_pid" || :; exit 143' TERM
  trap 'wait "$chain_pid" || :; exit 130' INT
  wait "$chain_pid" || rc=$?
  trap - TERM INT
  notify_chain "$rc"
  return "$rc"
}

if [ -t 1 ]; then
  run_with_notify > "$LOG" 2>&1 &
  PID=$!
  echo "$PID" > runtime/prematch_rebuild.pid
  echo "PID=$PID"
  echo "лог: $LOG"
  echo "проверка: kill -0 $PID && tail -3 $LOG"
else
  echo $$ > runtime/prematch_rebuild.pid
  run_with_notify > "$LOG" 2>&1
fi

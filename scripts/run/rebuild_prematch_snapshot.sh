#!/bin/bash
# Ночная пересборка снимка предматчевой модели и доставка на serv1.
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
# Почему локально. Про-корпус живёт только на этой машине (10 ГБ против 206 МБ
# на serv1), синхронизации нет: `pro_heroes_data/` закрыт .gitignore, rsync кода
# между машинами мы не используем. Снимок собирается здесь и уезжает готовым.
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
cd /Users/alex/Documents/ingame
PY=venv_catboost/bin/python3
LOG="${LOG:-runtime/prematch_rebuild_$(date +%Y%m%d_%H%M).log}"
SERV1=root@23.26.193.167

run_chain() {
  echo "=== $(date '+%F %T') пересборка снимка предматчевой модели ==="
  # Страховка от молчащего добора: 25.08–01.09.2026 launchd не запускал 04:30-джобу
  # 8 ночей, и снимок уезжал на прод с корпусом 23.08 (sha1 не менялся). Если за
  # 20 ч лога добора нет — добираем здесь; падение добора пересборку не останавливает.
  if [ -z "$(find runtime -maxdepth 1 -name 'pro_topup_*.log' -mmin -1200 2>/dev/null)" ]; then
    echo "свежего лога добора нет (>20 ч) — добираю корпус перед пересборкой"
    bash scripts/run/topup_pro_corpus.sh || echo "добор упал (rc=$?), пересобираю на текущем корпусе"
  fi
  # 1. выжимки корпуса (компактная и богатая)
  $PY runtime/experiments/misc/pro_corpus_extract.py
  $PY runtime/experiments/misc/pro_corpus_rich.py
  # 2. снимок: v1 -> добавки v2. Оба шага обязаны видеть один и тот же корпус.
  #    v2 идёт в режиме ТОЛЬКО СНИМОК: матрица признаков и переобучение весов
  #    требуют кэшей (`ideas_batch*.npz`, EXT, драфт-логит), а они привязаны к
  #    длине корпуса — как только корпус подрос, склейка падает по форме. Веса
  #    устаревают куда медленнее снимка и обновляются отдельно, с замером.
  PREMATCH_SNAPSHOT_ONLY=1 $PY runtime/experiments/misc/build_prematch_artifact.py
  PREMATCH_SNAPSHOT_ONLY=1 $PY runtime/experiments/misc/build_prematch_artifact_v2.py
  # 3. состояние под шесть колонок E-168: отклонения урона и нетворса по
  #    аккаунту и позиционные ячейки контрпика/синергии
  $PY runtime/experiments/misc/add_live_maps.py
  # 4. опознание организаций по составу + история личных встреч
  $PY runtime/experiments/misc/add_org_identity.py
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
    $PY runtime/experiments/misc/finalize_artifact.py
  # 5b. справочник написаний из цепочек ПЕРЕИМЕНОВАНИЙ (для поиска карточки у
  #     букмекера). Имена берём с прода: записи вида `tier_two_teams['ironwing']`
  #     дописывает рантайм на serv1, и локальная копия про новые теги не знает.
  #     С 02.09.2026 рантайм пишет их в JSON-overlay рядом со справочником —
  #     забираем оба файла (overlay на свежем проде может ещё отсутствовать).
  NAMES_DIR="$(mktemp -d)"
  scp -q "$SERV1:/root/main/base/id_to_names.py" "$NAMES_DIR/id_to_names.py"
  scp -q "$SERV1:/root/main/base/id_to_names_dynamic_tier2.json" "$NAMES_DIR/" || true
  TEAM_NAMES_DIR="$NAMES_DIR" $PY base/tools/build_team_org_aliases.py
  rm -rf "$NAMES_DIR"

  # 5c. ELO-снимок: рейтинги команд и история килов, из которой kills27-shadow
  #     берёт ростерные выборки. До 02.09.2026 снимок собирался ВРУЧНУЮ и возился
  #     файлом: на проде лежал срез 11.08 (569 493 матча, 242 МБ), пока локальный
  #     корпус вырос до 1 395 282 (срез 01.09, 646 МБ) — рекомендации в Telegram
  #     считались на ростер-истории трёхнедельной давности (E-249). Пересборка
  #     ~11 мин; отказ НЕ рвёт цепочку (доставка предматчевого артефакта важнее),
  #     а молчаливое протухание ловит freshness-watchdog.
  if $PY ELO/live_team_strength.py --snapshot-path ELO/output/live_team_elo_snapshot.json; then
    scp -q ELO/output/live_team_elo_snapshot.json \
        "$SERV1:/root/main/ELO/output/live_team_elo_snapshot.json.tmp"
    L=$(shasum -a 1 ELO/output/live_team_elo_snapshot.json | cut -d' ' -f1)
    R=$(ssh "$SERV1" "sha1sum /root/main/ELO/output/live_team_elo_snapshot.json.tmp | cut -d' ' -f1")
    if [ "$L" = "$R" ]; then
      ssh "$SERV1" "mv /root/main/ELO/output/live_team_elo_snapshot.json.tmp \
                       /root/main/ELO/output/live_team_elo_snapshot.json"
      echo "ELO-снимок доставлен: sha1 $L"
    else
      ssh "$SERV1" "rm -f /root/main/ELO/output/live_team_elo_snapshot.json.tmp"
      echo "ВНИМАНИЕ: ELO-снимок доехал битым ($R против $L) — на проде остался прежний"
    fi
  else
    echo "ВНИМАНИЕ: ELO-снимок не пересобрался — на проде останется прежний"
  fi

  # 6. проверка ПЕРЕД доставкой: артефакт обязан читаться и содержать тот же
  #    набор признаков, что и боевой. Иначе прод останется на старом файле.
  $PY - <<'CHECK'
import sys, time, numpy as np
from pathlib import Path
R = Path("/Users/alex/Documents/ingame")
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
  scp -q runtime/artifacts/misc/prematch_model_artifact_v3_hybrid.npz \
      "$SERV1:/root/main/data/prematch_model_artifact_v3.npz.tmp"
  ssh "$SERV1" "mv /root/main/data/prematch_model_artifact_v3.npz.tmp \
                   /root/main/data/prematch_model_artifact_v3.npz"
  # Сверка ПОСЛЕ доставки: единственная проверка, которая поймала бы E-193.
  # Совпадение sha1 локального и боевого файла — доказательство, что уехало
  # именно то, что собрано, а не одноимённый файл прошлой недели.
  LOCAL_SHA=$(shasum -a 1 runtime/artifacts/misc/prematch_model_artifact_v3_hybrid.npz | cut -d' ' -f1)
  REMOTE_SHA=$(ssh "$SERV1" "sha1sum /root/main/data/prematch_model_artifact_v3.npz | cut -d' ' -f1")
  if [ "$LOCAL_SHA" != "$REMOTE_SHA" ]; then
    echo "ОШИБКА: на сервере другой файл ($REMOTE_SHA против $LOCAL_SHA), рестарт не делаю"
    exit 1
  fi
  echo "доставка подтверждена: sha1 $LOCAL_SHA"
  scp -q data/team_org_aliases.json \
      "$SERV1:/root/main/data/team_org_aliases.json.tmp"
  ssh "$SERV1" "mv /root/main/data/team_org_aliases.json.tmp \
                   /root/main/data/team_org_aliases.json"

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
    $PY runtime/experiments/misc/build_prior_snapshot.py
    $PY runtime/experiments/misc/build_rating_snapshot.py
    $PY runtime/experiments/misc/build_pair_snapshot.py
    for f in prior_snapshot.npz rating_snapshot.npz pair_prior_snapshot.npz; do
      scp -q "data/$f" "$SERV1:/root/main/data/$f.tmp"
      L=$(shasum -a 1 "data/$f" | cut -d' ' -f1)
      R=$(ssh "$SERV1" "sha1sum /root/main/data/$f.tmp | cut -d' ' -f1")
      if [ "$L" != "$R" ]; then
        ssh "$SERV1" "rm -f /root/main/data/$f.tmp"
        echo "снимок $f доехал битым ($R против $L) — оставляю прежний"
        return 1
      fi
      ssh "$SERV1" "mv /root/main/data/$f.tmp /root/main/data/$f"
      echo "снимок $f доставлен: sha1 $L"
    done
  }
  if panel_snapshots; then
    echo "снимки панели обновлены"
  else
    echo "ВНИМАНИЕ: снимки панели не обновились, на боевой машине остались прежние"
  fi

  # --- фактическая доходность по реальным котировкам ----------------------
  # Только ЧТЕНИЕ с боевой машины: сводит журнал отправленных предматчевых
  # ставок с архивом котировок Winline и считает ROI. Раньше это считалось
  # невозможным («архив несоединим», E-103) — на деле не совпадал ключ: в
  # архиве нет `match_id`, join идёт по (имена команд, номер карты).
  # Разовое число тут бессмысленно, выборка мала; смысл в накоплении.
  if $PY runtime/experiments/misc/prematch_bet_roi.py > /dev/null 2>&1; then
    echo "доходность пересчитана: runtime/artifacts/misc/prematch_bet_roi.md"
  else
    echo "ВНИМАНИЕ: отчёт о доходности не собрался (не критично, читает только)"
  fi

  # 8. рестарт прода и чистка map_id_check — иначе новый снимок не читается,
  #    а уже разобранные карты не переоцениваются.
  ssh "$SERV1" "systemctl restart cyberscore.service && sleep 3 && \
                : > /root/.local/state/ingame/map_id_check.txt && \
                systemctl is-active cyberscore.service"

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
  local rc="$1"
  if [ "$rc" -ne 0 ] || grep -qE 'ОШИБКА|ВНИМАНИЕ|Traceback|AssertionError|Connection closed|scp:' "$LOG"; then
    { echo "⚠️ пересборка снимка: rc=$rc ($(date '+%F %T'))";
      grep -E 'ОШИБКА|ВНИМАНИЕ|Traceback|AssertionError|Connection closed|scp:|снимку|доставка' "$LOG" | tail -8; } \
      | $PY scripts/ops/notify_admin.py
  else
    { echo "✅ пересборка снимка ($(date '+%F %T'))";
      grep -E 'проверка пройдена|доставка подтверждена|снимок .* доставлен|^active' "$LOG" | tail -5; } \
      | $PY scripts/ops/notify_admin.py
  fi
}

run_with_notify() {
  local rc=0
  if LOG="$LOG" bash "$0" --run-chain; then rc=0; else rc=$?; fi
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

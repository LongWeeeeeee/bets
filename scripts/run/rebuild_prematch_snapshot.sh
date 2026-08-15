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
LOG="runtime/prematch_rebuild_$(date +%Y%m%d_%H%M).log"
SERV1=root@23.26.193.167

run_chain() {
  echo "=== $(date '+%F %T') пересборка снимка предматчевой модели ==="
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
  NAMES_DIR="$(mktemp -d)"
  scp -q "$SERV1:/root/main/base/id_to_names.py" "$NAMES_DIR/id_to_names.py"
  TEAM_NAMES_DIR="$NAMES_DIR" $PY base/tools/build_team_org_aliases.py
  rm -rf "$NAMES_DIR"

  # 6. проверка ПЕРЕД доставкой: артефакт обязан читаться и содержать тот же
  #    набор признаков, что и боевой. Иначе прод останется на старом файле.
  $PY - <<'CHECK'
import sys, numpy as np
from pathlib import Path
R = Path("/Users/alex/Documents/ingame")
sys.path.insert(0, str(R / "base"))
import prematch_scorer as ps
new = R / "runtime/artifacts/misc/prematch_model_artifact_v3_hybrid.npz"
m = ps.PrematchModel(new)
assert len(m.features) == len(m.coef[0]), (len(m.features), len(m.coef[0]))
need = ("hybrid_strength", "cp_lane", "syn_pos_mean",
        "a_hdmg_rel_pos", "a_hdmg_rel_hero", "a_nw_rel_pos")
missing = [n for n in need if n not in m.features]
assert not missing, f"в новом артефакте нет колонок: {missing}"
acc = np.load(new)["accounts"]
assert acc.shape[1] >= 19, f"в accounts {acc.shape[1]} колонок, ожидалось >= 19"
print(f"проверка пройдена: признаков {len(m.features)}, колонок в accounts {acc.shape[1]}, "
      f"аккаунтов {len(m.acc):,}")
CHECK

  # 7. доставка. Атомарно: пишем .tmp и переименовываем поверх, чтобы прод
  #    никогда не увидел недокачанный файл.
  scp -q runtime/artifacts/misc/prematch_model_artifact_v3_hybrid.npz \
      "$SERV1:/root/main/data/prematch_model_artifact_v3.npz.tmp"
  ssh "$SERV1" "mv /root/main/data/prematch_model_artifact_v3.npz.tmp \
                   /root/main/data/prematch_model_artifact_v3.npz"
  scp -q data/team_org_aliases.json \
      "$SERV1:/root/main/data/team_org_aliases.json.tmp"
  ssh "$SERV1" "mv /root/main/data/team_org_aliases.json.tmp \
                   /root/main/data/team_org_aliases.json"

  # 8. рестарт прода и чистка map_id_check — иначе новый снимок не читается,
  #    а уже разобранные карты не переоцениваются.
  ssh "$SERV1" "systemctl restart cyberscore.service && sleep 3 && \
                : > /root/.local/state/ingame/map_id_check.txt && \
                systemctl is-active cyberscore.service"
  echo "=== $(date '+%F %T') готово ==="
}

# Под launchd работать НАДО В ПЕРЕДНЕМ ПЛАНЕ. Если уйти в фон и выйти, launchd
# считает задание завершённым и убивает всю группу процессов — цепочка умирает
# через секунду, оставляя пустой лог (проверено 14.08: лог создавался нулевого
# размера, ни одного шага не выполнялось). В терминале, наоборот, удобнее фон.
if [ -t 1 ]; then
  run_chain > "$LOG" 2>&1 &
  PID=$!
  echo "$PID" > runtime/prematch_rebuild.pid
  echo "PID=$PID"
  echo "лог: $LOG"
  echo "проверка: kill -0 $PID && tail -3 $LOG"
else
  echo $$ > runtime/prematch_rebuild.pid
  run_chain > "$LOG" 2>&1
fi

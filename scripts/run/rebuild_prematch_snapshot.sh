#!/bin/bash
# Ночная пересборка снимка предматчевой модели и доставка на serv1.
#
# Зачем каждую ночь. Два признака в снимке привязаны ко времени и молча
# протухают: `wr30` — окно 30 дней, `vs_wr` — распад с полураспадом 45 дней.
# Снимок недельной давности означает «винрейт за 30 дней, закончившихся неделю
# назад», и модель об этом не сообщит — она посчитает и вернёт число. Поэтому
# скорер отказывается считать, если снимок старше `max_age_days` (по умолчанию
# 3 дня), а этот раннер держит его свежим.
#
# Почему локально. Про-корпус живёт только на этой машине (3.9 ГБ против 217 МБ
# на serv1), синхронизации нет: `pro_heroes_data/` закрыт .gitignore, rsync кода
# между машинами мы не используем. Снимок собирается здесь и уезжает готовым.
#
# Порядок: добор свежих про-матчей -> выжимки -> артефакт -> доставка.
set -e
cd /Users/alex/Documents/ingame
PY=venv_catboost/bin/python3
LOG="runtime/prematch_rebuild_$(date +%Y%m%d_%H%M).log"
SERV1=root@23.26.193.167

{
  echo "=== $(date '+%F %T') пересборка снимка предматчевой модели ==="
  # 1. выжимки корпуса (компактная и богатая)
  $PY runtime/experiments/misc/pro_corpus_extract.py
  $PY runtime/experiments/misc/pro_corpus_rich.py
  # 2. драфт-логит паблик-модели (кэш; пересчитывается при смене корпуса)
  #    если файл уже есть — шаг пропускается внутри скрипта
  $PY runtime/experiments/misc/combined_model_eval.py > /dev/null
  # 3. признаки и артефакт
  #    v1 -> v2 (29 признаков) -> v3 (опознание организации по составу).
  #    ПРОД ЧИТАЕТ ИМЕННО v3 (`prematch_scorer.ARTIFACT_PATH`), поэтому шаг
  #    add_org_identity обязателен: без него ночная пересборка обновляла бы v2,
  #    прод продолжал бы читать вчерашний v3 и через трое суток отказал бы
  #    вообще на всех матчах по протухшему снимку.
  $PY runtime/experiments/misc/build_prematch_artifact.py
  $PY runtime/experiments/misc/build_prematch_artifact_v2.py
  $PY runtime/experiments/misc/add_org_identity.py
  # 3b. справочник написаний из цепочек ПЕРЕИМЕНОВАНИЙ (для поиска карточки у
  #     букмекера). Имена берём с прода: записи вида `tier_two_teams['ironwing']`
  #     дописывает рантайм на serv1, и локальная копия про новые теги не знает.
  NAMES_DIR="$(mktemp -d)"
  scp -q "$SERV1:/root/main/base/id_to_names.py" "$NAMES_DIR/id_to_names.py"
  TEAM_NAMES_DIR="$NAMES_DIR" $PY base/tools/build_team_org_aliases.py
  rm -rf "$NAMES_DIR"
  # 4. доставка. Атомарно: пишем .tmp и переименовываем поверх, чтобы прод
  #    никогда не увидел недокачанный файл.
  scp -q runtime/artifacts/misc/prematch_model_artifact_v3.npz \
      "$SERV1:/root/main/data/prematch_model_artifact_v3.npz.tmp"
  ssh "$SERV1" "mv /root/main/data/prematch_model_artifact_v3.npz.tmp \
                   /root/main/data/prematch_model_artifact_v3.npz"
  scp -q runtime/artifacts/misc/prematch_model_artifact_v2.npz \
      "$SERV1:/root/main/data/prematch_model_artifact_v2.npz.tmp"
  ssh "$SERV1" "mv /root/main/data/prematch_model_artifact_v2.npz.tmp \
                   /root/main/data/prematch_model_artifact_v2.npz"
  scp -q runtime/artifacts/misc/prematch_model_spec_v2.json \
      "$SERV1:/root/main/data/prematch_model_spec_v2.json"
  scp -q data/team_org_aliases.json \
      "$SERV1:/root/main/data/team_org_aliases.json.tmp"
  ssh "$SERV1" "mv /root/main/data/team_org_aliases.json.tmp \
                   /root/main/data/team_org_aliases.json"
  echo "=== $(date '+%F %T') готово ==="
} > "$LOG" 2>&1 &

PID=$!
echo "$PID" > runtime/prematch_rebuild.pid
echo "PID=$PID"
echo "лог: $LOG"
echo "проверка: kill -0 $PID && tail -3 $LOG"

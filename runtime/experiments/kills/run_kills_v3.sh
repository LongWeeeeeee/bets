#!/usr/bin/env bash
# Килы v3 одной цепочкой: сборка признаков -> обучение паблик -> обучение про ->
# перенос -> прод-политика -> готовый отчёт. Переразбор корпусов делается отдельно
# (`run_kills_v3_extract.sh`), потому что он один раз и самый длинный.
#
# Каждое пробуждение агента стоит перечитывания контекста, поэтому промежуточных
# остановок здесь нет: на выходе один короткий отчёт.
set -u
cd /Users/alex/Documents/ingame
PY=venv_catboost/bin/python3
K=runtime/experiments/kills
DIR=runtime/artifacts/kills/window_model_v2
OUT=runtime/artifacts/kills/kills_v3_report.md
ts() { date +%H:%M:%S; }

echo "[$(ts)] === 0/6 самопроверка ==="
$PY $K/test_kills_v3.py || { echo "САМОПРОВЕРКА ПРОВАЛЕНА"; exit 1; }

echo "[$(ts)] === 1/6 признаки: про ==="
$PY $K/kills_v3_build.py --corpus pro || exit 1

echo "[$(ts)] === 2/6 признаки: паблик ==="
$PY $K/kills_v3_build.py --corpus public || exit 1

echo "[$(ts)] === 3/6 обучение на паблике (все карты, 7 целей) ==="
$PY $K/kills_v3_train.py --stage public || exit 1

echo "[$(ts)] === 4/6 обучение на про (своё) ==="
$PY $K/kills_v3_train.py --stage pro || exit 1

echo "[$(ts)] === 5/6 обучение на про + паблик-модель как колонка ==="
$PY $K/kills_v3_train.py --stage pro --transfer-from v3pub || exit 1

echo "[$(ts)] === 6/6 словарь, прод-политика и 27+ ==="
$PY $K/kills_v3_policy.py || exit 1

{
  echo "# Килы v3: окна, итог карты, 27+ и тотал 51"
  echo
  echo "Прогон завершён $(date '+%Y-%m-%d %H:%M')."
  echo
  echo '## Паблик (обучение на всём корпусе)'
  echo '```json'; cat $DIR/report_v3pub.json; echo '```'
  echo
  echo '## Про (обучение на про-корпусе)'
  echo '```json'; cat $DIR/report_v3pro.json; echo '```'
  echo
  echo '## Про + паблик-модель отдельной колонкой'
  echo '```json'; cat $DIR/report_v3prot.json; echo '```'
  echo
  cat runtime/artifacts/kills/kills_v3_policy.md
} > "$OUT"
echo "[$(ts)] ВСЁ ГОТОВО -> $OUT"

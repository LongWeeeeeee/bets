#!/usr/bin/env bash
# Куда ставить следующий прогон: сверка загрузки локальной машины и serv1.
# Правило (E-53): перед запуском тяжёлого прогона сверить обе машины и ставить
# туда, где больше запаса — иначе одна стоит без дела, а вторая захлёбывается.
set -u
SERVER=${SERVER:-root@23.26.193.167}

read_local() {
  local cores load
  cores=$(sysctl -n hw.ncpu 2>/dev/null || nproc)
  load=$(uptime | sed 's/.*averages*: *//' | awk '{print $1}' | tr ',' '.')
  echo "$cores $load"
}
echo "=== ЛОКАЛЬНО ==="
set -- $(read_local)
cores=$1; load=$2
free=$(echo "$cores $load" | awk '{printf "%.1f", $1-$2}')
echo "  ядер: $cores, load: $load, свободно ≈ $free"
echo "  наших python-прогонов: $(pgrep -fc 'venv_catboost/bin/python3' 2>/dev/null || echo 0)"
df -h . | tail -1 | awk '{print "  диск: занято "$5", свободно "$4}'

echo "=== SERV1 ==="
ssh -o ConnectTimeout=15 "$SERVER" '
  cores=$(nproc)
  load=$(uptime | sed "s/.*average: //" | cut -d, -f1 | tr -d " ")
  echo "  ядер: $cores, load: $load, свободно ≈ $(echo "$cores $load" | awk "{printf \"%.1f\", \$1-\$2}")"
  echo "  python-процессов всего: $(pgrep -fc python3)"
  echo "  наших прогонов: $(pgrep -fc -f "/root/exp/" 2>/dev/null || echo 0)"
  df -h / | tail -1 | awk "{print \"  диск: занято \"\$5\", свободно \"\$4}"
' 2>/dev/null || echo "  сервер недоступен"

cat <<'HINT'

Как решать:
  - счётчик ячеек (один проход, JSON на выходе)  -> можно ставить куда угодно, диск не ест
  - сборка словаря (~2 ГБ на вариант)            -> смотреть свободный диск: на serv1 его мало
  - прогон check_old_maps (RSS ~2.5 ГБ)          -> не больше 3 параллельно на машину
  - сторожевые циклы pgrep НЕ писать в одной строке с именем целевого процесса:
    цикл найдёт сам себя и повиснет навсегда (проверено на баракaх 07.08)
HINT

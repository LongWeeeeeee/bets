#!/bin/bash
# Ставит ежедневный снимок лидербордов Valve в расписание systemd на serv1 (запускать НА
# serv1 под root). С 10.10.2026 сбор идёт только на serv1 — владелец: «убери любой сбор с
# мака ранги в том числе пусть парсятся с serv1» (карточка ingame-qe6y). Прежняя джоба
# launchd com.ingame.rank-snapshots на Mac снята; подробности в шапке
# scripts/run/collect_rank_snapshots.sh.
#
# Слоты 06/09/12/15/18 UTC; слот после полного снимка за сутки UTC ничего не делает.
# Лог прогона: /root/main/runtime/artifacts/misc/rank_snapshots/collect_<YYYYMMDD UTC>.log
# Разовый прогон: systemctl start rank-snapshots.service
# Снять:          systemctl disable --now rank-snapshots.timer
set -eu
if [ "$(uname -s)" != "Linux" ]; then
  echo "install-rank-snapshots.sh: только serv1 (Linux). На Mac сбор снят 10.10.2026." >&2
  exit 1
fi
REPO=/root/main
mkdir -p "$REPO/runtime/artifacts/misc/rank_snapshots"
# Проверка до копирования: битый юнит не должен попасть в /etc/systemd/system, где его
# подхватил бы чужой daemon-reload (hard-verifier 10.10 INFO).
systemd-analyze verify "$REPO/scripts/ops/systemd/rank-snapshots.service" "$REPO/scripts/ops/systemd/rank-snapshots.timer"
install -m 0644 "$REPO/scripts/ops/systemd/rank-snapshots.service" /etc/systemd/system/rank-snapshots.service
install -m 0644 "$REPO/scripts/ops/systemd/rank-snapshots.timer" /etc/systemd/system/rank-snapshots.timer
systemctl daemon-reload
systemctl enable --now rank-snapshots.timer
# Первое включение без сохранённой отметки ждёт следующего слота (Persistent=true
# догоняет только пропущенное после первой отметки) — сразу один прогон в фоне;
# если за сегодня UTC полный снимок уже есть, он только пишет «skip:» в лог.
systemctl start --no-block rank-snapshots.service
systemctl list-timers rank-snapshots.timer --no-pager | head -3

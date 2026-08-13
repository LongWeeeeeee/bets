#!/usr/bin/env bash
# Заливка корпусов и моделей на serv2. Порядок — от самого недостающего к мелочи,
# чтобы при обрыве успело доехать главное.
#
# --delete НЕ используется НИГДЕ: на том конце может лежать то, чего нет здесь,
# и молча снести это нельзя (правило «ничего не удалять без подтверждения»).
# rsync докладывает недостающее и обновляет изменившееся, не трогая остальное.
set -u
cd /Users/alex/Documents/ingame
DST=serv2:/Users/alexford/ingame
RS="rsync -a --partial --stats -e ssh"

ts() { date +%H:%M:%S; }
step() { echo; echo "[$(ts)] === $* ==="; }

step "0/4 место на serv2 ДО"
ssh serv2 'df -h / | tail -1'

step "1/4 про-корпус (10 ГБ, на serv2 отсутствует целиком)"
ssh serv2 'mkdir -p /Users/alexford/ingame/pro_heroes_data/json_parts_split_from_object'
$RS pro_heroes_data/json_parts_split_from_object/ \
    $DST/pro_heroes_data/json_parts_split_from_object/ || echo "ПРО: ОШИБКА rc=$?"

step "2/4 паблик-корпус (докладываю недостающие файлы)"
$RS bets_data/analise_pub_matches/json_parts_split_from_object/ \
    $DST/bets_data/analise_pub_matches/json_parts_split_from_object/ || echo "ПАБЛИК: ОШИБКА rc=$?"

step "3/4 модели и справочники"
ssh serv2 'mkdir -p /Users/alexford/ingame/{ml-models,data,runtime/artifacts/kills}'
$RS ml-models/ $DST/ml-models/ || echo "ml-models: ОШИБКА"
$RS data/ $DST/data/ || echo "data: ОШИБКА"
# модели окон килов: только сами модели и кодировщики, без 90 МБ сырых шардов
$RS --include='*/' --include='*.joblib' --include='report*.json' --exclude='*' \
    runtime/artifacts/kills/window_model/ \
    $DST/runtime/artifacts/kills/window_model/ || echo "window_model: ОШИБКА"
$RS base/hero_features_processed.json $DST/base/hero_features_processed.json || echo "hero_features: ОШИБКА"

step "4/4 сверка"
ssh serv2 'echo "паблик: $(ls /Users/alexford/ingame/bets_data/analise_pub_matches/json_parts_split_from_object/*.json 2>/dev/null | wc -l) файлов, $(du -sh /Users/alexford/ingame/bets_data/analise_pub_matches/json_parts_split_from_object 2>/dev/null | cut -f1)"
echo "про:    $(ls /Users/alexford/ingame/pro_heroes_data/json_parts_split_from_object/*.json 2>/dev/null | wc -l) файлов, $(du -sh /Users/alexford/ingame/pro_heroes_data/json_parts_split_from_object 2>/dev/null | cut -f1)"
echo "модели: $(du -sh /Users/alexford/ingame/ml-models /Users/alexford/ingame/data /Users/alexford/ingame/runtime/artifacts/kills/window_model 2>/dev/null | tr "\n" " ")"
df -h / | tail -1'
echo "[$(ts)] ЗАЛИВКА ЗАВЕРШЕНА"

#!/bin/zsh
set -euo pipefail
cd /Users/alex/Documents/ingame
source venv_catboost/bin/activate
export BOOKMAKER_PREFETCH_ENABLED=0
export TEST_DISABLE_ADD_URL=0
exec /Users/alex/Documents/ingame/venv_catboost/bin/python -u /Users/alex/Documents/ingame/base/cyberscore_try.py --no-odds

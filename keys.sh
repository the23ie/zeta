#!/usr/bin/env bash
# ./keys.sh 7 12   -> print store keys (and the admin key) signed with .zeta_secret
cd "$(dirname "$0")"
if [ -f .zeta_secret ]; then export ZETA_SECRET=$(cat .zeta_secret); fi
exec .venv/bin/python -m zeta.guard "$@"

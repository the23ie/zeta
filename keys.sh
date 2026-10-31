#!/usr/bin/env bash
# Store API keys (signed with .zeta_secret). Run by the company owner, who hands each store its key.
#   ./keys.sh 7 12          keys for stores 7 and 12 (plus the owner and admin keys)
#   ./keys.sh --all t10m    every store's key (stores found in the t10m_* databases)
#   ./keys.sh --rotate 7    new key for store 7; its old key stops working immediately (also on a running server)
# A store owner opens  http://<host>:8001/store?key=<their key>
cd "$(dirname "$0")"
source ./env.sh
exec .venv/bin/python -m zeta.guard "$@"

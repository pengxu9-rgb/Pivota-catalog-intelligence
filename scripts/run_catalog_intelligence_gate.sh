#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SERVER_DIR="${REPO_ROOT}/server"
VENV_DIR="${REPO_ROOT}/.venv-readiness"

if [[ ! -d "${SERVER_DIR}" ]]; then
  echo "server directory not found: ${SERVER_DIR}" >&2
  exit 2
fi

if [[ ! -x "${SERVER_DIR}/node_modules/.bin/tsx" ]]; then
  (
    cd "${SERVER_DIR}"
    npm ci
  )
fi

if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  python3 -m venv "${VENV_DIR}"
  (
    . "${VENV_DIR}/bin/activate"
    pip install -r "${REPO_ROOT}/ingredient-harvester/requirements.txt"
  )
fi

(
  cd "${SERVER_DIR}"
  npm run build
  npm test
)

(
  cd "${REPO_ROOT}"
  . "${VENV_DIR}/bin/activate"
  export PYTHONPATH="${REPO_ROOT}/ingredient-harvester"
  python -m py_compile services/*.py ingredient-harvester/app/*.py ingredient-harvester/app/harvester/*.py
  python -m pytest ingredient-harvester/tests -q
)

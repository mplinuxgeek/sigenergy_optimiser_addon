#!/bin/sh
set -e

CONFIG_DIR=/config/sigenergy_optimiser
CONFIG_PATH="${CONFIG_PATH:-${CONFIG_DIR}/config.yaml}"

mkdir -p "${CONFIG_DIR}"

if [ ! -f "${CONFIG_PATH}" ]; then
    cp /app/config.example.yaml "${CONFIG_PATH}"
    echo "Created starter config at ${CONFIG_PATH} — edit it before restarting the add-on."
fi

export CONFIG_PATH
export LOG_LEVEL="${LOG_LEVEL:-INFO}"
export STATE_DB_PATH="${STATE_DB_PATH:-${CONFIG_DIR}/optimizer_state.db}"

exec uvicorn optimizer.web:app --host 0.0.0.0 --port 8000

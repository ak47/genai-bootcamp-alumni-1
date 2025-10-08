#!/bin/bash

export PATH="${PATH}:${LAMBDA_TASK_ROOT}/bin"
export PYTHONPATH="${PYTHONPATH}:/opt/python:${LAMBDA_RUNTIME_DIR}"

if command -v python3 >/dev/null 2>&1; then
    python_cmd="python3"
elif command -v python3.13 >/dev/null 2>&1; then
    python_cmd="python3.13"
elif command -v python >/dev/null 2>&1; then
    python_cmd="python"
else
    echo "Unable to locate a Python interpreter on PATH" >&2
    exit 127
fi

exec "${python_cmd}" -m uvicorn --port="${PORT}" main:app

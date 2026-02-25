#!/usr/bin/env bash
set -e

PROJECT_DIR=$(dirname "$(readlink -f "$0")")
AIRFLOW_HOME=$PROJECT_DIR/airflow_home
DAGS_FOLDER=$PROJECT_DIR/dags
PYTHON_VERSION=3.11

export AIRFLOW_HOME=$AIRFLOW_HOME
export AIRFLOW__CORE__DAGS_FOLDER=$DAGS_FOLDER
export AIRFLOW__CORE__LOAD_EXAMPLES=False

cd $PROJECT_DIR

if ! pyenv versions | grep -q "${PYTHON_VERSION}"; then
    echo "Установка Python ${PYTHON_VERSION} через pyenv..."
    pyenv install ${PYTHON_VERSION}
fi

pyenv local ${PYTHON_VERSION}

if ! command -v poetry &> /dev/null; then
    echo "Установка Poetry..."
    curl -sSL https://install.python-poetry.org | python3 -
    export PATH="$HOME/.local/bin:$PATH"
fi

poetry env use "$(pyenv which python)"

poetry install --no-root

echo "Запуск Airflow Standalone..."
poetry run airflow standalone
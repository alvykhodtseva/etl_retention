#!/usr/bin/env bash

# --------------------------------------------- SETUP -----------------------------------------------------------------
set -e

NETWORK="db"
APP_NAME="onetime_etl_retention"

LOGGING_DIR_PATH=$HOME/logging
CONFIG_DIR_PATH=$HOME/configs/etl_retention
CODEBASE_DIR_PATH=$HOME/etl_retention/src

# --------------------------------------------- WORKFLOW --------------------------------------------------------------

# if container exists, delete it
if [[ "$(docker container ls -a | grep -q ${APP_NAME})" ]]; then
  docker container rm ${APP_NAME}
fi

cd ./onetime

docker build -t ${APP_NAME} --rm .

docker run \
    -v ${CONFIG_DIR_PATH}:/configs \
    -v ${LOGGING_DIR_PATH}:/logging \
    -v ${CODEBASE_DIR_PATH}:/cmd/src \
    --network=${NETWORK} \
    --name=${APP_NAME} \
    --rm \
    ${APP_NAME}

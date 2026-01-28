#!/bin/bash

export DATASOURCE_DB_DATABASE=$postgresDatabase; \
export DATASOURCE_DB_PORT=$postgresPort; \
export DATASOURCE_DB_SCHEMA=$postgresSchema; \
export DATASOURCE_DB_HOST=$postgresServer; \
export DATASOURCE_DB_USERNAME=$postgresUsername; \
export DATASOURCE_DB_PASSWORD=$postgresPassword; \
bunny $@
#!/bin/bash

KAFKA_HOME=/home/steven/kafka_2.13-2.7.0

export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:./config/connect-log4j.properties"

${KAFKA_HOME}/bin/connect-standalone.sh ./config/standalone_connect.properties ./config/OracleSourceConnector.properties

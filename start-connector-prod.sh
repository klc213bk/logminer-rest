#!/bin/bash

KAFKA_HOME=/home/kafka/kafka_2.13-2.7.0

export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:./config/connect-log4j.properties"

${KAFKA_HOME}/bin/connect-standalone.sh ./config/standalone_connect-uat.properties ./config/OracleSourceConnector.properties

#!/bin/bash

curl -X POST http://localhost:8083/connectors/oracle-logminer-connector/restart?includeTasks=true&onlyFailed=false

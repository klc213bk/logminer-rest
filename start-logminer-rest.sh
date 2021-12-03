#!/bin/bash

java -jar -Dspring.profiles.active=logminer target/logminer-rest-1.0.jar --spring.config.location=file:config/ 

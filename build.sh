#!/usr/bin/env bash
mvn clean package -DskipTests \
-Pfast,skip-webui-build

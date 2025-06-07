#!/bin/sh
echo 'Run maven build'
mvn clean package

echo 'Build docker'
docker build -t seasonal-mortality-spark .

echo 'Run docker'
docker run --rm -v "$PWD/output":/tmp/output seasonal-mortality-spark

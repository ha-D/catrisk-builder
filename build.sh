#!/bin/bash
set -e
IMAGE=131364435557.dkr.ecr.eu-west-1.amazonaws.com/worker/catrisk/catrisks_worker:2.3.10-01

docker build -t $IMAGE .
cp aws/* ../.aws/
aws ecr get-login-password | docker login --username AWS --password-stdin 131364435557.dkr.ecr.eu-west-1.amazonaws.com
docker push $IMAGE

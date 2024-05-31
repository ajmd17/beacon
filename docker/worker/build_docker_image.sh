#!/bin/bash

docker buildx build -t beacon-worker -f ./docker/worker/Dockerfile . --platform linux/amd64
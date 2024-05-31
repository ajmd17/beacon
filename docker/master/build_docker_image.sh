#!/bin/bash

docker buildx build -t beacon-master -f ./docker/master/Dockerfile . --platform linux/amd64
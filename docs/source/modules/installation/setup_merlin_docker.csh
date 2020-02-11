#!/bin/csh

command -v docker>/dev/null 2>&1 || { echo >&2 "This script requires docker but it's not installed.  Aborting."; exit 1; }

# pull the containers
docker pull llnl/merlin
docker pull redis
# optional
docker pull rabbitmq

# start redis
docker run -d --name my-redis -p 6379:6379 redis

# start merlin
docker -dt --name my-merlin --link my-redis -v "$HOME/merlinu":/home/merlinu llnl/merlin

# setup aliases
alias merlin "docker exec my-merlin merlin"
alias celery "docker exec my-merlin celery"
alias python3 "docker exec my-merlin python3"

# start rabbitmq
#docker run -d --hostname my-rabbit --name some-rabbit rabbitmq:3

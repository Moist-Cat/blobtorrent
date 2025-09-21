#!/bin/bash
# Deploy the stack
docker swarm init
docker stack deploy -c docker-compose.yaml blobtorrent

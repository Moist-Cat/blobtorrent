#!/bin/bash
set -e

echo "Initializing swarm..."
# Use the machine's actual IP that's accessible from machine B
docker swarm init --advertise-addr 192.168.43.130 --listen-addr 192.168.43.130:2377

echo "Creating network..."
docker network create -d overlay --attachable blobtorrent-net

echo "Building images..."
docker build -f Dockerfile.tracker -t blobtorrent-tracker:latest .
docker build -t blobtorrent:latest .

echo "Starting trackers..."
for i in {1..3}; do
  docker run -d \
    --name "tracker-$i" \
    --network blobtorrent-net \
    --network-alias tracker \
    -v $(pwd)/src:/app \
    --restart unless-stopped \
    blobtorrent-tracker:latest
done

echo "Starting nodes..."
for i in {1..5}; do
  docker run -d \
    --name "blobtorrent-node-$i" \
    --network blobtorrent-net \
    --network-alias blobtorrent-node \
    -v $(pwd)/torrent:/torrent:ro \
    -v $(pwd)/src:/app \
    --memory=256m \
    --memory-reservation=128m \
    --restart unless-stopped \
    blobtorment:latest python3 api.py
done

echo "Starting control node..."
docker run -d \
  --name control-node \
  --network blobtorrent-net \
  -v $(pwd)/torrent:/torrent:ro \
  -v $(pwd)/scripts:/scripts \
  blobtorrent:latest tail -f /dev/null

echo "Starting monitor..."
docker volume create portainer-data
docker run -d \
  --name monitor \
  --network blobtorrent-net \
  -p 9000:9000 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v portainer-data:/data \
  portainer/portainer-ce:latest -H unix:///var/run/docker.sock

echo "Setup complete!"
# join token
docker swarm join-token manager

#!/bin/bash
# Network manipulation script

ACTION=$1
NETWORK=$2
NODE=$3

case $ACTION in
    "disconnect")
        docker network disconnect $NETWORK $NODE
        ;;
    "connect")
        docker network connect $NETWORK $NODE
        ;;
    "partition")
        # Create a network partition by disconnecting all nodes from the main network
        for node in $(docker service ps blobtorrent_blobtorrent-node -q); do
            docker network disconnect blobtorrent_blobtorrent-net $node
        done
        ;;
    "heal")
        # Reconnect all nodes to the main network
        for node in $(docker service ps blobtorrent_blobtorrent-node -q); do
            docker network connect blobtorrent_blobtorrent-net $node
        done
        ;;
    "limit")
        # Limit bandwidth for a node (requires tc in the node)
        CONTAINER=$(docker ps --filter "name=$NODE" --format "{{.ID}}")
        docker exec $CONTAINER tc qdisc add dev eth0 root tbf rate 1mbit burst 32kbit latency 400ms
        ;;
    "reset-limit")
        CONTAINER=$(docker ps --filter "name=$NODE" --format "{{.ID}}")
        docker exec $CONTAINER tc qdisc del dev eth0 root
        ;;
    *)
        echo "Usage: $0 {disconnect|connect|partition|heal|limit|reset-limit}"
        exit 1
        ;;
esac

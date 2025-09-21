#!/bin/bash
# Control script for managing nodes

ACTION=$1
NODE=$2
TORRENT=$3

case $ACTION in
    "start")
        docker service update --args="run-client --output-dir /out/ /torrent/$TORRENT" blobtorrent_blobtorrent-node
        ;;
    "stop")
        docker service update --args="tail -f /dev/null" blobtorrent_blobtorrent-node
        ;;
    "restart")
        docker service scale blobtorrent_blobtorrent-node=0
        sleep 2
        docker service scale blobtorrent_blobtorrent-node=5
        ;;
    "scale")
        docker service scale blobtorrent_blobtorrent-node=$NODE
        ;;
    "logs")
        docker service logs blobtorrent_blobtorrent-node -f
        ;;
    "status")
        docker service ps blobtorrent_blobtorrent-node
        ;;
    "exec")
        CONTAINER=$(docker ps --filter "name=blobtorrent" --format "{{.Names}}" | head -1)
        docker exec -it $CONTAINER /bin/bash
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|scale|logs|status|exec}"
        exit 1
        ;;
esac

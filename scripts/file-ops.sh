#!/bin/bash
# File operations script

ACTION=$1
NODE=$2
FILE=$3

CONTAINER=$(docker ps --filter "name=$NODE" --format "{{.Names}}")

case $ACTION in
    "delete")
        docker exec $CONTAINER rm -f "/out/$FILE"
        echo "Deleted $FILE from $NODE"
        ;;
    "restore")
        # Restore from original torrent content
        docker exec $CONTAINER cp "/out/original/$FILE" "/out/$FILE"
        echo "Restored $FILE on $NODE"
        ;;
    "list")
        docker exec $CONTAINER ls -la "/out/"
        ;;
    "check")
        docker exec $CONTAINER sha1sum "/out/$FILE"
        ;;
    *)
        echo "Usage: $0 {delete|restore|list|check}"
        exit 1
        ;;
esac

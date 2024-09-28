#!/bin/bash

CONTAINER_NAME=memcached

# Check if the container is running
if ! docker ps --format '{{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
    echo "Error: Container '$CONTAINER_NAME' is not running or doesn't exist."
    exit 1
fi

# Confirm before flushing memcached data
read -p "Are you sure you want to flush all data in memcached (y/n)? " confirmation
if [[ ! $confirmation =~ ^[Yy]$ ]]; then
    echo "Aborted by user."
    exit 0
fi

# Flush memcached using telnet
docker exec -it $CONTAINER_NAME bash -c "echo -e 'flush_all\r\nquit\r\n' | telnet localhost 11211"

# Check if the command was successful
if [ $? -eq 0 ]; then
    echo "Memcached has been successfully refreshed in container '$CONTAINER_NAME'."
else
    echo "Error: Failed to refresh memcached in container '$CONTAINER_NAME'."
    exit 1
fi

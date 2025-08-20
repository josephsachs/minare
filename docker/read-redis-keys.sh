#!/bin/bash

docker exec -i redis sh -c 'redis-cli --scan | while read k; do echo "Key: $k"; redis-cli JSON.GET "$k"; echo; done'
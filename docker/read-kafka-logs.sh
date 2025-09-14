#!/bin/bash

docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic minare.operations --from-beginning
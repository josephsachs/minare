#!/bin/bash

grep -h "$1" logs/COORDINATOR-app-coordinator-runtime.log logs/WORKER-*-runtime.log | tee /dev/stderr | wc -l

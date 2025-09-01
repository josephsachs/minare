#!/bin/bash

grep -h "$1" COORDINATOR-app-coordinator-runtime.log WORKER-*-runtime.log | tee /dev/stderr | wc -l

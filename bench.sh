#!/bin/bash

export GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS=TRUE
export GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS_FOR_RW=TRUE
export GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS=TRUE
mvn clean compile exec:java   -Dexec.mainClass="org.example.ThroughputRunner"   -Dexec.args="--project span-cloud-testing --instance sailesh-largeinstance-mr --database wmt-demo --mode findOrCreateEdge --numRuns 10000000 --numThreads 386"

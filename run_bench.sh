#!/bin/bash

export GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS=TRUE
export GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS_FOR_RW=TRUE
export GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS=TRUE

# The "$@" will be replaced by all the arguments you pass to this script.
mvn clean compile exec:java \
  -Dexec.mainClass="org.example.ThroughputRunner" \
  -Dexec.args="$*"

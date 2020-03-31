#!/bin/bash

set -e

if [[ "$WAIT_FOR_ENV_SCRIPT" == "true" ]]; then
  while [[ ! -s ./env.sh ]]; do sleep 0.1; done;
fi

. ./env.sh

export CLASSPATH="${CLASSPATH:-/opt/kafka/plugins/*}"

if [[ -z "$KAFKA_BROKER_ID" ]]; then
    export KAFKA_BROKER_ID=0
fi

if [[ -z "$KAFKA_LOG_DIRS" ]]; then
    export KAFKA_LOG_DIRS="/tmp/kafka-logs"
fi

if [[ -z "$KAFKA_ZOOKEEPER_CONNECT" ]]; then
    export KAFKA_ZOOKEEPER_CONNECT="0.0.0.0:2181"
fi

if [[ -z "$KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT_MS" ]]; then
    export KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT_MS="18000"
fi

if [[ -z "$KAFKA_KAFKA_METRICS_REPORTERS" ]]; then
    export KAFKA_KAFKA_METRICS_REPORTERS="nl.techop.kafka.KafkaHttpMetricsReporter"
fi

if [[ -z "$KAFKA_METRIC_REPORTERS" && "${CLASSPATH}" == *"/opt/kafka/plugins/*"* ]] && ls /opt/kafka/plugins/cruise-control-metrics-reporter-*.jar 1> /dev/null 2>&1; then
    export KAFKA_METRIC_REPORTERS="com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter"
fi

if [[ -z "$KAFKA_KAFKA_HTTP_METRICS_HOST" ]]; then
    export KAFKA_KAFKA_HTTP_METRICS_HOST="0.0.0.0"
fi

if [[ -z "$KAFKA_KAFKA_HTTP_METRICS_PORT" ]]; then
    export KAFKA_KAFKA_HTTP_METRICS_PORT=24042
fi

if [[ -z "$ADVERTISED_PORT" ]]; then
    ADVERTISED_PORT=9092
fi
if [[ -z "$HOST_NAME" ]]; then
    HOST_NAME=$(ip addr | grep 'BROADCAST' -A2 | tail -n1 | awk '{print $2}' | cut -f1  -d'/')
fi

: ${PORT:=9092}
: ${ADVERTISED_PORT:=9092}

: ${ADVERTISED_PORT:=${PORT}}
: ${ADVERTISED_HOST_NAME:=${HOST_NAME}}

: ${KAFKA_ADVERTISED_PORT:=${ADVERTISED_PORT}}
: ${KAFKA_ADVERTISED_HOST_NAME:=${ADVERTISED_HOST_NAME}}

: ${KAFKA_PORT:=${PORT}}
: ${KAFKA_HOST_NAME:=${HOST_NAME}}

: ${KAFKA_LISTENERS:=PLAINTEXT://$KAFKA_HOST_NAME:$KAFKA_PORT}
: ${KAFKA_ADVERTISED_LISTENERS:=PLAINTEXT://$KAFKA_ADVERTISED_HOST_NAME:$KAFKA_ADVERTISED_PORT}

export KAFKA_LISTENERS KAFKA_ADVERTISED_LISTENERS
unset HOST_NAME ADVERTISED_HOST_NAME KAFKA_HOST_NAME KAFKA_ADVERTISED_HOST_NAME PORT ADVERTISED_PORT KAFKA_PORT KAFKA_ADVERTISED_PORT

echo "" >> "server.properties"

#
# Process all environment variables that start with 'KAFKA_' (but not 'KAFKA_HOME' or 'KAFKA_VERSION'):
#
env -0 | while IFS='=' read -r -d '' env_var VALUE; do
  echo "processing env variable $env_var with value: $VALUE"
  if [[ $env_var =~ ^KAFKA_ && $env_var != "KAFKA_VERSION" && $env_var != "KAFKA_HOME" && $env_var != "KAFKA_OPTS" && $env_var != "KAFKA_LOG4J_OPTS" && $env_var != "KAFKA_JMX_OPTS" ]]; then
    prop_name=`echo "$env_var" | sed -r "s/^KAFKA_(.*)/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ .`
    if egrep -q "(^|^#)$prop_name=" server.properties; then
        #note that no config names or values may contain an '@' char
        sed -r -i "s@(^|^#)($prop_name)=(.*)@\2=${!env_var}@g" server.properties
    else
        #echo "Adding property $prop_name=${!env_var}"
        echo "$prop_name=${!env_var}" >> server.properties
    fi
  fi
done

if [[ -n "${KAFKA_CLUSTER_ID}" ]]; then
  kafka-storage.sh format --cluster-id "${KAFKA_CLUSTER_ID}" --config server.properties
fi
kafka-server-start.sh server.properties
#!/bin/bash

set -e

if [[ -z "$CONNECT_GROUP_ID" ]]; then
    export CONNECT_GROUP_ID="connect-cluster"
fi

if [[ -z "$CONNECT_KEY_CONVERTER" ]]; then
    export CONNECT_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter"
fi

if [[ -z "$CONNECT_VALUE_CONVERTER" ]]; then
    export CONNECT_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter"
fi

if [[ -z "$CONNECT_REST_PORT" ]]; then
    export CONNECT_REST_PORT=28083
fi

if [[ -z "$CONNECT_LISTENERS" ]]; then
    export CONNECT_LISTENERS="http://0.0.0.0:$CONNECT_REST_PORT"
fi

if [[ -z "$CONNECT_PLUGIN_PATH" ]]; then
    export CONNECT_PLUGIN_PATH="/opt/connect/plugin"
fi

if [[ -z "$CONNECT_OFFSET_STORAGE_TOPIC" ]]; then
    export CONNECT_OFFSET_STORAGE_TOPIC="connect-offsets"
fi

if [[ -z "$CONNECT_CONFIG_STORAGE_TOPIC" ]]; then
    export CONNECT_CONFIG_STORAGE_TOPIC="connect-configs"
fi

if [[ -z "$CONNECT_STATUS_STORAGE_TOPIC" ]]; then
    export CONNECT_STATUS_STORAGE_TOPIC="connect-status"
fi

if [[ -z "$CONNECT_OFFSET_FLUSH_INTERVAL_MS" ]]; then
    export CONNECT_OFFSET_FLUSH_INTERVAL_MS=10000
fi

if [[ -z "$CONNECT_CONNECT_PROMETHEUS_METRICS_PORT" ]]; then
    export CONNECT_CONNECT_PROMETHEUS_METRICS_PORT=28086
fi


echo "" >> "connect-distributed.properties"

#
# Process all environment variables that start with 'CONNECT_':
#
for VAR in `env`
do
  echo "processing env variable $VAR"
  env_var=`echo "$VAR" | sed -r "s/(.*)=.*/\1/g"`
  if [[ $env_var =~ ^CONNECT_ ]]; then
    prop_name=`echo "$VAR" | sed -r "s/^CONNECT_(.*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ .`
    if egrep -q "(^|^#)$prop_name=" connect-distributed.properties; then
        #note that no config names or values may contain an '@' char
        sed -r -i "s@(^|^#)($prop_name)=(.*)@\2=${!env_var}@g" connect-distributed.properties
    else
        #echo "Adding property $prop_name=${!env_var}"
        echo "$prop_name=${!env_var}" >> connect-distributed.properties
    fi
  fi
done

connect-distributed.sh connect-distributed.properties
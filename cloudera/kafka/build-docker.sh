#!/bin/bash

set -ex

: ${KAFKA_TAR_LOCATION:=__KAFKA_TAR_LOCATION__}
: ${CRUISE_CONTROL_TAR_LOCATION:=__CRUISE_CONTROL_TAR_LOCATION__}

rm -rf docker-tmp
mkdir docker-tmp

cp "${KAFKA_TAR_LOCATION}" docker-tmp/kafka.tgz
cp cloudera/kafka/docker-entrypoint.sh docker-tmp/docker-entrypoint.sh
cp cloudera/kafka/env.sh docker-tmp/env.sh

mkdir docker-tmp/plugins
if [[ -f "${CRUISE_CONTROL_TAR_LOCATION}" ]]; then
  tar xzf "${CRUISE_CONTROL_TAR_LOCATION}" -C docker-tmp/plugins --strip-components=2 --wildcards '**/cruise-control-metrics-reporter-*'
fi

docker_file="cloudera/kafka/Dockerfile"
image_name="kafka"

: ${REGISTRY:="docker-private.infra.cloudera.com/cloudera"}
: ${TAGS:="latest"}
: ${base_registry="docker-private.infra.cloudera.com/cloudera_base/"}

# we need to build every tag separately as build system wraps 'docker' command and transforms it to multi-arch building
for tag in ${TAGS}; do
  docker build --build-arg base_registry="${base_registry}" -f "${docker_file}" -t "${REGISTRY}/${image_name}:${tag}" ./docker-tmp
done

rm -rf docker-tmp

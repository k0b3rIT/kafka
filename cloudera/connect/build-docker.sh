#!/bin/bash

set -ex

KAFKA_TAR_LOCATION="${KAFKA_TAR_LOCATION:-__KAFKA_TAR_LOCATION__}"

rm -rf docker-tmp
mkdir docker-tmp

cp "${KAFKA_TAR_LOCATION}" docker-tmp/kafka.tgz
cp cloudera/connect/docker-entrypoint.sh docker-tmp/docker-entrypoint.sh

docker_file="cloudera/connect/Dockerfile"
image_name="kafka-connect-base"

: ${REGISTRY:="docker-private.infra.cloudera.com/cloudera"}
: ${TAGS:="latest"}
: ${base_registry="docker-private.infra.cloudera.com/cloudera_base/"}

# we need to build every tag separately as build system wraps 'docker' command and transforms it to multi-arch building
for tag in ${TAGS}; do
  docker build --build-arg base_registry="${base_registry}" -f "${docker_file}" -t "${REGISTRY}/${image_name}:${tag}" ./docker-tmp
done

rm -rf docker-tmp

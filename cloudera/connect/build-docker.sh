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

tagging=""
for tag in ${TAGS}; do
  tagging="${tagging} -t ${REGISTRY}/${image_name}:${tag}"
done

docker build --build-arg base_registry="${base_registry}" -f "${docker_file}" ${tagging} ./docker-tmp

rm -rf docker-tmp

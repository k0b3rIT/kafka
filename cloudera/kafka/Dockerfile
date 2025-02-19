ARG base_registry=docker-private.infra.cloudera.com/cloudera_base/

FROM ${base_registry}hardened/cloudera-openjdk:jdk-21-runtime-nofips

USER root

RUN apk update && apk add iproute2

ENV KAFKA_KAFKA_HTTP_METRICS_PORT=24042 \
    KAFKA_PORT=9092

RUN mkdir -p /opt/kafka
COPY kafka.tgz /tmp/kafka.tgz

RUN tar xfz /tmp/kafka.tgz -C /opt/kafka --strip-components=1
RUN rm -f /tmp/kafka.tgz

RUN apk update && apk add \
    krb5 \
    vim \
    bash \
    openssl \
    netcat-openbsd

ENV PATH=$PATH:/opt/kafka/bin
WORKDIR /opt/kafka

EXPOSE $KAFKA_PORT
EXPOSE $KAFKA_KAFKA_HTTP_METRICS_PORT

ADD env.sh /opt/kafka/
ADD docker-entrypoint.sh /opt/kafka/
ADD plugins /opt/kafka/plugins
ENTRYPOINT ["/opt/kafka/docker-entrypoint.sh"]
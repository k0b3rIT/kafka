ARG base_registry=docker-private.infra.cloudera.com/cloudera_base/

FROM ${base_registry}hardened/cloudera-openjdk:jdk-21-runtime-nofips

ENV CONNECT_REST_PORT=28083 \
    CONNECT_CONNECT_PROMETHEUS_METRICS_PORT=28086

USER root

RUN mkdir -p /opt/kafka
RUN mkdir -p /opt/connect/plugin
COPY kafka.tgz /tmp/kafka.tgz

RUN tar xfz /tmp/kafka.tgz -C /opt/kafka --strip-components=1
RUN rm -f /tmp/kafka.tgz

RUN apk update && apk add \
    krb5 \
    vim \
    bash \
    openssl

ENV PATH=$PATH:/opt/kafka/bin
WORKDIR /opt/kafka

EXPOSE $CONNECT_REST_PORT
EXPOSE $CONNECT_CONNECT_PROMETHEUS_METRICS_PORT

ADD docker-entrypoint.sh /opt/kafka
ENTRYPOINT ["/opt/kafka/docker-entrypoint.sh"]
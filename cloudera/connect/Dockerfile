ARG base_registry=docker-private.infra.cloudera.com/cloudera_base/

FROM ${base_registry}openjdk:11.0.13-jdk-slim-buster

ENV CONNECT_REST_PORT=28083 \
    CONNECT_CONNECT_PROMETHEUS_METRICS_PORT=28086


RUN mkdir -p /opt/kafka
RUN mkdir -p /opt/connect/plugin
COPY kafka.tgz /tmp/kafka.tgz

RUN tar xfz /tmp/kafka.tgz -C /opt/kafka --strip-components=1
RUN rm -f /tmp/kafka.tgz

RUN apt update && apt install -y \
    krb5-user \
    vim \
    bash \
    openssl

ENV PATH=$PATH:/opt/kafka/bin
WORKDIR /opt/kafka

EXPOSE $CONNECT_REST_PORT
EXPOSE $CONNECT_CONNECT_PROMETHEUS_METRICS_PORT

ADD docker-entrypoint.sh /opt/kafka
ENTRYPOINT ["/opt/kafka/docker-entrypoint.sh"]
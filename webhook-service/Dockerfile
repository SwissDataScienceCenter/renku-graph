# This is a multi-stage build, see reference:
# https://docs.docker.com/develop/develop-images/multistage-build/
FROM openjdk:8-jre-alpine3.9 as builder

# Getting SBT
RUN echo "http://dl-4.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
    apk add --no-cache sbt curl

WORKDIR /work

# Building microservice
COPY project project
COPY build.sbt .
COPY webhook-service/project webhook-service/project
COPY webhook-service/build.sbt webhook-service/
RUN sbt "project webhook-service" "stage"

FROM openjdk:8-jre-alpine3.9

WORKDIR /opt/webhook-service

# Add artifacts from builder
COPY --from=builder /work/webhook-service/target/universal/stage .

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

RUN chown -R daemon:daemon .

ENTRYPOINT ["bin/webhook-service"]
CMD []

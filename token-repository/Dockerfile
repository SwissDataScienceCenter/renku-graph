# This is a multi-stage build, see reference:
# https://docs.docker.com/develop/develop-images/multistage-build/
FROM openjdk:8-jre-alpine3.9 as builder

# Getting SBT
RUN echo "http://dl-4.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
    apk add --no-cache sbt

WORKDIR /work

# Building microservice
COPY . .
RUN sbt "project token-repository" stage

FROM openjdk:8-jre-alpine3.9

WORKDIR /opt/token-repository

# Add artifacts from builder
COPY --from=builder /work/token-repository/target/universal/stage .

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

RUN apk add --no-cache curl bash && \
    chown -R daemon:daemon .

ENTRYPOINT ["bin/token-repository"]
CMD []

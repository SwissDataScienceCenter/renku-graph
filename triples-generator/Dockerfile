# This is a multi-stage build, see reference:
# https://docs.docker.com/develop/develop-images/multistage-build/
FROM openjdk:8-jre-alpine3.9 as builder

# Getting SBT
RUN echo "http://dl-4.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
    apk add --no-cache sbt

WORKDIR /work

# Building microservice
COPY . .
RUN sbt "project triples-generator" stage

FROM openjdk:8-jre-alpine3.9

WORKDIR /opt/triples-generator

# Add artifacts from builder
COPY --from=builder /work/triples-generator/target/universal/stage .

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

# Installing Renku
RUN apk add --no-cache python3 git curl bash && \
    apk add --no-cache --virtual .renku-dependencies linux-headers gcc python3-dev libxml2-dev libxslt-dev libc-dev && \
    pip3 install --pre renku && \
    apk del .renku-dependencies && \
    chown -R daemon:daemon .

ENTRYPOINT ["bin/triples-generator"]
CMD []

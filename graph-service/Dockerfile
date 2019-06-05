# This is a multi-stage build, see reference:
# https://docs.docker.com/develop/develop-images/multistage-build/

FROM openjdk:8-jre-alpine3.9 as builder

WORKDIR /work

COPY . .

RUN echo "http://dl-4.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
    apk add --no-cache --virtual .build-dependencies sbt && \
    sbt "project graph-service" stage && \
    apk del .build-dependencies

FROM openjdk:8-jre-alpine3.9

WORKDIR /opt/graph-service

# Add artifacts from builder
COPY --from=builder /work/graph-service/target/universal/stage .

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

RUN apk add --no-cache curl bash && \
    chown -R daemon:daemon .

ENTRYPOINT ["bin/graph-service"]
CMD []

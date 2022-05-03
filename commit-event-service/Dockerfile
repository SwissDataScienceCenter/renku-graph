# This is a multi-stage build, see reference:
# https://docs.docker.com/develop/develop-images/multistage-build/

FROM openjdk:18-jdk-alpine as builder

WORKDIR /work

COPY . .

RUN echo "http://dl-4.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
    export PATH="/usr/local/sbt/bin:$PATH" && \
    apk update && apk add --no-cache --virtual .build-dependencies bash wget tar git && \
    mkdir -p "/usr/local/sbt" && \
    wget -qO  - "https://github.com/sbt/sbt/releases/download/v1.6.2/sbt-1.6.2.tgz" | tar xz -C /usr/local/sbt --strip-components=1 && \
    sbt writeVersionToVersionConf && \
    sbt "project commit-event-service" stage  && \
    apk del .build-dependencies

FROM openjdk:18-jdk-alpine

WORKDIR /opt/commit-event-service

# Add artifacts from builder
COPY --from=builder /work/commit-event-service/target/universal/stage .

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV TZ UTC

RUN apk add --no-cache tzdata curl bash tini && \
    chown -R daemon:daemon .

ENV GID=1000
RUN adduser --disabled-password -g "$GID" -D -u 1000 cesuser && \
    chmod 555 -R /opt/commit-event-service

USER cesuser

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["bin/commit-event-service"]

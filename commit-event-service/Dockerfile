# This is a multi-stage build, see reference:
# https://docs.docker.com/develop/develop-images/multistage-build/

FROM openjdk:15-jdk-alpine as builder

WORKDIR /work

COPY . .

RUN echo "http://dl-4.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
    apk add --no-cache --virtual .build-dependencies sbt && \
    sbt "project commit-event-service" stage  && \
    apk del .build-dependencies

FROM openjdk:15-jdk-alpine

WORKDIR /opt/commit-event-service

# Add artifacts from builder
COPY --from=builder /work/commit-event-service/target/universal/stage .

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV TZ UTC

RUN apk add --no-cache tzdata curl bash && \
    chown -R daemon:daemon .

ENTRYPOINT ["bin/commit-event-service"]
CMD []

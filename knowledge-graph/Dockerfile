# This is a multi-stage build, see reference:
# https://docs.docker.com/develop/develop-images/multistage-build/

FROM openjdk:14-jdk-alpine as builder

WORKDIR /work

COPY . .

RUN echo "http://dl-4.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
    export PATH="/usr/local/sbt/bin:$PATH" && \
    apk update && apk add --no-cache --virtual .build-dependencies bash wget tar && \
    mkdir -p "/usr/local/sbt" && \
    wget -qO - --no-check-certificate "https://github.com/sbt/sbt/releases/download/v1.5.6/sbt-1.5.6.tgz" | tar xz -C /usr/local/sbt --strip-components=1 && \
    sbt "project knowledge-graph" stage && \
    apk del .build-dependencies

FROM openjdk:14-jdk-alpine

WORKDIR /opt/knowledge-graph

# Add artifacts from builder
COPY --from=builder /work/knowledge-graph/target/universal/stage .

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV TZ UTC

RUN apk add --no-cache tzdata curl bash && \
    chown -R daemon:daemon .

ENTRYPOINT ["bin/knowledge-graph"]
CMD []

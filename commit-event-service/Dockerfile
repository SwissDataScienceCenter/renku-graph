# This is a multi-stage build, see reference:
# https://docs.docker.com/develop/develop-images/multistage-build/

FROM openjdk:19-slim-bullseye as builder

WORKDIR /work

COPY . .

RUN export PATH="/usr/local/sbt/bin:$PATH" && \
    apt-get update && apt-get install -y bash wget tar git && \
    mkdir -p "/usr/local/sbt" && \
    wget -qO  - "https://github.com/sbt/sbt/releases/download/v1.7.0/sbt-1.7.0.tgz" | tar xz -C /usr/local/sbt --strip-components=1 && \
    sbt writeVersionToVersionConf && \
    sbt "project commit-event-service" stage  && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives

FROM openjdk:19-slim-bullseye

WORKDIR /opt/commit-event-service

# Add artifacts from builder
COPY --from=builder /work/commit-event-service/target/universal/stage .

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV TZ UTC

ENV GID=1000
RUN apt-get update && apt-get install -y tzdata curl bash tini && \
    chown -R daemon:daemon . && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives && \
    addgroup --gid "$GID" cesuser && \
    adduser --disabled-password --gid "$GID" --uid 1000 cesuser && \
    chmod 555 -R /opt/commit-event-service

USER cesuser

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["bin/commit-event-service"]

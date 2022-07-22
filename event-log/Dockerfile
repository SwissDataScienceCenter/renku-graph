# This is a multi-stage build, see reference:
# https://docs.docker.com/develop/develop-images/multistage-build/

FROM openjdk:19-slim-bullseye as builder

WORKDIR /work

COPY . .

RUN export PATH="/usr/local/sbt/bin:$PATH" && \
    apt-get update && apt-get install -y bash wget tar git && \
    mkdir -p "/usr/local/sbt" && \
    wget -qO - "https://github.com/sbt/sbt/releases/download/v1.7.0/sbt-1.7.0.tgz" | tar xz -C /usr/local/sbt --strip-components=1 && \
    sbt writeVersionToVersionConf && \
    sbt "project event-log" stage && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives

FROM openjdk:19-slim-bullseye

WORKDIR /opt/event-log

# Add artifacts from builder
COPY --from=builder /work/event-log/target/universal/stage .

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV TZ UTC

ENV GID=1000
RUN apt-get update && apt-get install -y tzdata curl bash tini && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives && \
    chown -R daemon:daemon . && \
    addgroup --gid "$GID" eluser && \
    adduser --disabled-password --gid "$GID" -uid 1000 eluser && \
    chmod 555 -R /opt/event-log

USER eluser

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["bin/event-log"]

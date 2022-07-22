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
    sbt "project knowledge-graph" stage && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives

FROM openjdk:19-slim-bullseye

WORKDIR /opt/knowledge-graph

# Add artifacts from builder
COPY --from=builder /work/knowledge-graph/target/universal/stage .

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV TZ UTC

RUN apt-get update && apt-get install -y tzdata curl bash tini && \
    chown -R daemon:daemon . && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives

ENV GID=1000
RUN addgroup --gid "$GID" ceuser && \
    adduser --disabled-password --gid "$GID" --uid 1000 kguser && \
    chmod 555 -R /opt/knowledge-graph

USER kguser

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["bin/knowledge-graph"]

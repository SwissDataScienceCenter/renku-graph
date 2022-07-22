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
    sbt "project triples-generator" stage && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives

FROM openjdk:19-slim-bullseye

WORKDIR /opt/triples-generator

# Add artifacts from builder
COPY --from=builder /work/triples-generator/target/universal/stage .

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV TZ UTC

# Installing Renku and other dependencies
RUN apt-get update && apt-get install -y tzdata git git-lfs curl bash python3-dev py3-pip py3-wheel openssl-dev libffi-dev linux-headers gcc g++ make libxml2-dev libxslt-dev libc-dev yaml-dev 'rust>1.41.0' cargo tini && \
    python3 -m pip install --ignore-installed packaging && \
    python3 -m pip install --upgrade 'pip==22.0.4' && \
    python3 -m pip install jinja2 && \
    python3 -m pip install 'renku==1.5.0' 'sentry-sdk==1.5.5'  && \
    chown -R daemon:daemon . && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives

COPY triples-generator/entrypoint.sh /entrypoint.sh

ENV GID=1000
RUN addgroup --gid "$GID" tguser && \
    adduser --disabled-password --gid "$GID" --uid 1000 tguser && \
    chmod 555 -R /opt/triples-generator && \
    chmod 555 -R /entrypoint.sh

USER tguser

RUN git config --global user.name 'renku'  && \
    git config --global user.email 'renku@renkulab.io'  && \
    git config --global filter.lfs.smudge "git-lfs smudge --skip %f"  && \
    git config --global filter.lfs.process "git-lfs filter-process --skip"

ENTRYPOINT ["/sbin/tini", "--", "/entrypoint.sh"]
CMD ["bin/triples-generator"]

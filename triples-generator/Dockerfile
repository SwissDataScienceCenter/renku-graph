# This is a multi-stage build, see reference:
# https://docs.docker.com/develop/develop-images/multistage-build/

FROM openjdk:18-jdk-alpine as builder

WORKDIR /work

COPY . .

RUN echo "http://dl-4.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
    export PATH="/usr/local/sbt/bin:$PATH" && \
    apk update && apk add --no-cache --virtual .build-dependencies bash wget tar git && \
    mkdir -p "/usr/local/sbt" && \
    wget -qO - "https://github.com/sbt/sbt/releases/download/v1.5.7/sbt-1.5.7.tgz" | tar xz -C /usr/local/sbt --strip-components=1 && \
    sbt writeVersionToVersionConf && \
    sbt "project triples-generator" stage && \
    apk del .build-dependencies

FROM openjdk:18-jdk-alpine

WORKDIR /opt/triples-generator

# Add artifacts from builder
COPY --from=builder /work/triples-generator/target/universal/stage .

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV TZ UTC

# Installing Renku and other dependencies
RUN apk update && apk add --no-cache tzdata git git-lfs curl bash python3-dev py3-pip py3-wheel openssl-dev libffi-dev linux-headers gcc g++ make libxml2-dev libxslt-dev libc-dev yaml-dev 'rust>1.41.0' cargo tini && \
    python3 -m pip install --ignore-installed packaging && \
    python3 -m pip install --upgrade 'pip==22.0.4' && \
    python3 -m pip install jinja2 && \
    python3 -m pip install 'renku==1.1.4' 'sentry-sdk==1.5.5'  && \
    chown -R daemon:daemon .

COPY triples-generator/entrypoint.sh /entrypoint.sh

ENV GID=90020
RUN addgroup -g "$GID" -S tggroup && \
    adduser --disabled-password -g "$GID" -D -u 9002 tguser && \
    chmod 555 -R /opt/triples-generator && \
    chmod 555 -R /entrypoint.sh

USER tguser

RUN git config --global user.name 'renku'  && \
    git config --global user.email 'renku@renkulab.io'  && \
    git config --global filter.lfs.smudge "git-lfs smudge --skip %f"  && \
    git config --global filter.lfs.process "git-lfs filter-process --skip"

ENTRYPOINT ["/sbin/tini", "--", "/entrypoint.sh"]
CMD ["bin/triples-generator"]

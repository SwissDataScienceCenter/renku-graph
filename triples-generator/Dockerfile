# This is a multi-stage build, see reference:
# https://docs.docker.com/develop/develop-images/multistage-build/

FROM eclipse-temurin:21-jre-alpine as builder

WORKDIR /work

COPY . .

RUN export PATH="/usr/local/sbt/bin:$PATH" && \
    apk update && apk add --no-cache --virtual .build-dependencies bash wget tar git && \
    mkdir -p "/usr/local/sbt" && \
    wget -qO - "https://github.com/sbt/sbt/releases/download/v1.9.8/sbt-1.9.8.tgz" | tar xz -C /usr/local/sbt --strip-components=1 && \
    sbt writeVersionToVersionSbt && \
    sbt writeVersionToVersionConf && \
    sbt "project triples-generator" stage && \
    apk del .build-dependencies

FROM eclipse-temurin:21-jre-alpine

WORKDIR /opt/triples-generator

# Add artifacts from builder
COPY --from=builder /work/triples-generator/target/universal/stage .

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV TZ UTC

# Installing Renku dependencies other tools
RUN apk update && apk add --no-cache tzdata git git-lfs curl bash python3-dev py3-pip py3-wheel pipx openssl-dev libffi-dev linux-headers gcc g++ make libxml2-dev libxslt-dev libc-dev yaml-dev tini && \
    chown -R daemon:daemon .

COPY triples-generator/entrypoint.sh /entrypoint.sh

ENV GID=1000
RUN adduser --disabled-password -g "$GID" -D -u 1000 tguser && \
    chmod 555 -R /opt/triples-generator && \
    chmod 555 -R /entrypoint.sh

USER tguser

ENV PATH=$PATH:/home/tguser/.local/bin

# Installing Renku
RUN pipx install 'renku==2.9.2' && \
    pipx inject renku sentry-sdk==1.43.0

RUN git config --global user.name 'renku'  && \
    git config --global user.email 'renku@renkulab.io'  && \
    git config --global filter.lfs.smudge "git-lfs smudge --skip %f"  && \
    git config --global filter.lfs.process "git-lfs filter-process --skip"

ENTRYPOINT ["/sbin/tini", "--", "/entrypoint.sh"]
CMD ["bin/triples-generator"]

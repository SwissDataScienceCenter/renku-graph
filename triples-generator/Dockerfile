# This is a multi-stage build, see reference:
# https://docs.docker.com/develop/develop-images/multistage-build/

FROM openjdk:8-jre-alpine3.9 as builder

WORKDIR /work

COPY . .

RUN echo "http://dl-4.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
    apk add --no-cache --virtual .build-dependencies sbt && \
    sbt "project triples-generator" stage && \
    apk del .build-dependencies

FROM openjdk:8-jre-alpine3.9

WORKDIR /opt/triples-generator

# Add artifacts from builder
COPY --from=builder /work/triples-generator/target/universal/stage .

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV TZ UTC

# Installing Renku and other dependencies
RUN apk add --no-cache tzdata git git-lfs curl bash python3-dev openssl-dev libffi-dev linux-headers gcc libxml2-dev libxslt-dev libc-dev && \
    python3 -m pip install 'renku==0.10.5' sentry-sdk && \
    chown -R daemon:daemon . && \
    git config --global filter.lfs.smudge "git-lfs smudge --skip %f"  && \
    git config --global filter.lfs.process "git-lfs filter-process --skip"

COPY triples-generator/entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["bin/triples-generator"]

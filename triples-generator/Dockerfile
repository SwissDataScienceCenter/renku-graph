# This is a multi-stage build, see reference:
# https://docs.docker.com/develop/develop-images/multistage-build/

FROM openjdk:15-jdk-alpine as builder

WORKDIR /work

COPY . .

RUN echo "http://dl-4.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
    apk add --no-cache --virtual .build-dependencies sbt && \
    sbt "project triples-generator" stage && \
    apk del .build-dependencies

FROM openjdk:15-jdk-alpine

WORKDIR /opt/triples-generator

# Add artifacts from builder
COPY --from=builder /work/triples-generator/target/universal/stage .

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV TZ UTC

# Installing Renku and other dependencies
RUN apk update && apk add --no-cache tzdata git git-lfs curl bash python3-dev py3-pip py3-wheel openssl-dev libffi-dev linux-headers gcc libxml2-dev libxslt-dev libc-dev yaml-dev 'rust>1.41.0' cargo && \
    python3 -m pip install 'git+https://github.com/SwissDataScienceCenter/renku-python.git@728343d64956f2441ad5ec2b203125d2fa14bc74' 'sentry-sdk==0.18.0'  && \
    chown -R daemon:daemon . && \
    git config --global user.name 'renku'  && \
    git config --global user.email 'renku@renkulab.io'  && \
    git config --global filter.lfs.smudge "git-lfs smudge --skip %f"  && \
    git config --global filter.lfs.process "git-lfs filter-process --skip"

COPY triples-generator/entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["bin/triples-generator"]

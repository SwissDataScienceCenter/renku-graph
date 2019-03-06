# This is a multi-stage build, see reference:
# https://docs.docker.com/develop/develop-images/multistage-build/
FROM openjdk:8-jre-alpine3.9 as builder

# Getting SBT
RUN echo "http://dl-4.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
    apk add --no-cache sbt curl

WORKDIR /work

# Building microservice
COPY project project
COPY build.sbt .
COPY token-repository/project token-repository/project
COPY token-repository/build.sbt token-repository/
RUN sbt "project token-repository" "stage"

FROM openjdk:8-jre-alpine3.9

WORKDIR /opt/token-repository

# Add artifacts from builder
COPY --from=builder /work/token-repository/target/universal/stage .

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

RUN chown -R daemon:daemon .

ENTRYPOINT ["bin/token-repository"]
CMD []

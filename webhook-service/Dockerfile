# This is a multi-stage build, see reference:
# https://docs.docker.com/develop/develop-images/multistage-build/
FROM openjdk:8-jre as builder

# Get SBT
RUN set -e \
  ; echo "deb http://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list \
  ; apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 \
  ; apt-get update \
  ; apt-get install -y sbt \
  ;

WORKDIR /work

# Get dependencies first, rebuilds will reuse these layers
COPY project project
COPY build.sbt .
COPY webhook-service/project webhook-service/project
COPY webhook-service/build.sbt webhook-service/
RUN set -e \
  ; sbt  "project webhook-service" "stage" \
  ;

# Stage artifacts
COPY . .
RUN set -e \
  ; sbt  "project webhook-service" "stage" \
  ;

FROM openjdk:8-jre

WORKDIR /opt/webhook-service

# Add artifacts from builder
COPY --from=builder /work/webhook-service/target/universal/stage .

# Adding initial tooling
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

RUN apt-get update --fix-missing && \
    apt-get install -y wget bzip2 ca-certificates curl git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN set -e \
  ; chown -R daemon:daemon . \
  ;

ENTRYPOINT ["bin/webhook-service"]
CMD []

# This is a multi-stage build, see reference:
# https://docs.docker.com/develop/develop-images/multistage-build/
FROM openjdk:8 as builder

# Get SBT
RUN set -e \
  ; echo "deb http://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list \
  ; apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 \
  ; apt-get update \
  ; apt-get install sbt \
  ;

WORKDIR /work

# Get dependencies first, rebuilds will reuse these layers
COPY project project
COPY build.sbt .
RUN set -e \
  ; sbt "stage" \
  ;

# Stage artifacts
COPY . .
RUN set -e \
  ; sbt "stage" \
  ;

FROM openjdk:8-jre

WORKDIR /opt/triples-generator

# Add artifacts from builder
COPY --from=builder /work/target/universal/stage .

# Adding initial tooling
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH /opt/conda/bin:$PATH

RUN apt-get update --fix-missing && \
    apt-get install -y wget bzip2 ca-certificates curl git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Installing Python - copied from https://hub.docker.com/r/continuumio/miniconda3/~/dockerfile/
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-4.5.11-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh && \
    /opt/conda/bin/conda clean -tipsy && \
    ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate base" >> ~/.bashrc

# Installing Renku
RUN apt-get update --fix-missing && \
    apt-get install -y gcc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --pre renku

RUN set -e \
  ; chown -R daemon:daemon . \
  ;

ENTRYPOINT ["bin/triples-generator"]
CMD []

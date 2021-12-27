# Define custom function directory
ARG FUNCTION_DIR="/opt/tika/"
ARG TIKA_VERSION_ARG=2.2.1

FROM python:buster as build-image

# Include global arg in this stage of the build
ARG FUNCTION_DIR
ARG TIKA_VERSION_ARG

# Install aws-lambda-cpp build dependencies
RUN apt clean && apt-get update && \
  apt-get install -y \
  g++ \
  make \
  cmake \
  unzip \
  wget \
  libcurl4-openssl-dev

# Copy function code
RUN mkdir -p ${FUNCTION_DIR}
RUN mkdir -p ${FUNCTION_DIR}lib
COPY app/* ${FUNCTION_DIR}

# Download Tika Libraries
RUN wget -P ${FUNCTION_DIR} "https://dlcdn.apache.org/tika/${TIKA_VERSION_ARG}/tika-server-standard-${TIKA_VERSION_ARG}.jar"
RUN wget -P ${FUNCTION_DIR} "https://dlcdn.apache.org/tika/${TIKA_VERSION_ARG}/tika-server-standard-${TIKA_VERSION_ARG}.jar.sha512"
RUN md5sum ${FUNCTION_DIR}tika-server-standard-${TIKA_VERSION_ARG}.jar > ${FUNCTION_DIR}tika-server-standard-${TIKA_VERSION_ARG}.jar.md5

# Install the function's dependencies
RUN pip install \
    --target ${FUNCTION_DIR}lib \
        -r ${FUNCTION_DIR}requirements.txt

FROM python:buster

# Include global arg in this stage of the build
ARG FUNCTION_DIR
ARG TIKA_VERSION_ARG
# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}

RUN apt-get update && \
      apt-get install -y software-properties-common
RUN apt-add-repository 'deb http://security.debian.org/debian-security stretch/updates main'
RUN apt-get update && \
  apt-get install -y openjdk-8-jdk \
  ant \
  ca-certificates-java
RUN apt-get clean;

# Fix certificate issues
RUN update-ca-certificates -f;

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

# Copy in the built dependencies
COPY --from=build-image ${FUNCTION_DIR} ${FUNCTION_DIR}
ENV TIKA_VERSION ${TIKA_VERSION_ARG}
RUN chmod 644 app.py tika-server-standard-${TIKA_VERSION_ARG}.jar tika-server-standard-${TIKA_VERSION_ARG}.jar.md5 \
    tika-server-standard-${TIKA_VERSION_ARG}.jar.sha512 requirements.txt
RUN chmod 755 $(find . -type d)
ENV TIKA_SERVER_JAR file://${FUNCTION_DIR}tika-server-standard-${TIKA_VERSION_ARG}.jar
ENV PYTHONPATH "${PYTHONPATH}:${FUNCTION_DIR}lib"
ENV TIKA_STARTUP_MAX_RETRY 20

ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
CMD [ "app.handler" ]
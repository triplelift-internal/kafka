##############################################################################################################
##############################################################################################################
#
# Kafka connect compatibility matrix - https://docs.confluent.io/platform/current/installation/versions-interoperability.html
# Redpanda compatibility matrix - https://docs.redpanda.com/docs/develop/kafka-clients/
#
##############################################################################################################
##############################################################################################################

# Stage 1: Download custom Connect and kafka lib JARs from S3
FROM amazon/aws-cli:latest AS downloader
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_SESSION_TOKEN
ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
ENV AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN
WORKDIR /tmp/jars
RUN aws s3 sync s3://ops.triplelift.net/public/kafka/3.2.6-tl/ .


# Stage 2: Build the final image
ARG TARGETARCH
FROM confluentinc/cp-kafka-connect-base:7.6.7 as base

USER root
RUN microdnf update -y && \
    microdnf install -y zstd curl tar gzip && \
    microdnf clean all

# Download and install Amazon Corretto 17
ARG TARGETARCH
RUN ARCH="${TARGETARCH:-$(uname -m)}" && \
    case "${ARCH}" in \
        amd64|x86_64) CORRETTO_ARCH="x64" ;; \
        arm64|aarch64) CORRETTO_ARCH="aarch64" ;; \
        *) echo "Unsupported architecture: ${ARCH}" && exit 1 ;; \
    esac && \
    echo "Building for architecture: ${ARCH}, Corretto arch: ${CORRETTO_ARCH}" && \
    curl -L "https://corretto.aws/downloads/latest/amazon-corretto-17-${CORRETTO_ARCH}-linux-jdk.tar.gz" -o /tmp/corretto-17.tar.gz && \
    mkdir -p /usr/lib/jvm && \
    tar -xzf /tmp/corretto-17.tar.gz -C /usr/lib/jvm && \
    mv /usr/lib/jvm/amazon-corretto-17.* /usr/lib/jvm/java-17-amazon-corretto && \
    rm /tmp/corretto-17.tar.gz

# Set Java 17 as the default
ENV JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
ENV PATH="$JAVA_HOME/bin:$PATH"

# Verify Java installation
RUN java -version && javac -version


# ===============================
# Plugin installation stage
# ===============================
FROM base as plugins
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.13


# ===============================
# Final image with plugins and runtime
# ===============================
FROM base

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

# Ensure Java 17 is available in final image
ENV JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
ENV PATH="$JAVA_HOME/bin:$PATH"

# Copy only the installed plugin from previous layer
COPY --from=plugins /usr/share/confluent-hub-components /usr/share/confluent-hub-components

# Copy runtime native libraries
ARG TARGETARCH
#COPY ./runtime-libraries/${TARGETARCH}/hadoop-annotations-3.4.0.jar /usr/share/confluent-hub-components/confluentinc-kafka-connect-s3/lib/
#COPY ./runtime-libraries/${TARGETARCH}/hadoop-auth-3.4.0.jar /usr/share/confluent-hub-components/confluentinc-kafka-connect-s3/lib/
#COPY ./runtime-libraries/${TARGETARCH}/hadoop-common-3.4.0.jar /usr/share/confluent-hub-components/confluentinc-kafka-connect-s3/lib/
#COPY ./runtime-libraries/${TARGETARCH}/libhadoop.so /usr/lib64/
#COPY ./runtime-libraries/${TARGETARCH}/libhadoop.so.1.0.0 /usr/lib64/
#COPY ./runtime-libraries/${TARGETARCH}/libnativetask.so /usr/lib64/
#COPY ./runtime-libraries/${TARGETARCH}/libnativetask.so.1.0.0 /usr/lib64/

# Auth and config
#COPY ./runtime-libraries/rest-api-auth.conf /etc/kafka-connect/
#COPY ./runtime-libraries/connect-api.password /etc/kafka-connect/
#COPY ./runtime-libraries/run /etc/confluent/docker/run

# Install JMX Prometheus agent
ARG JMX_PROMETHEUS_JAVA_AGENT_VERSION=1.0.1
RUN curl -fsSL https://repo.maven.apache.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_PROMETHEUS_JAVA_AGENT_VERSION}/jmx_prometheus_javaagent-${JMX_PROMETHEUS_JAVA_AGENT_VERSION}.jar \
    -o /etc/kafka-connect/jmx_prometheus_javaagent.jar

#COPY ./prometheus/prometheus.yaml /etc/kafka-connect/prometheus.yaml

# Copy Opensearch sink connector jars
#COPY ./runtime-libraries/opensearch-connector-libs/ /etc/kafka-connect/jars/

# Replace connect-runtime jar with custom build which has global task balance support
# Remove all the old jar files in the kafka package. 
# While we only need connect-runtime.jar for the balance support change, 
# confluent packages a custom build of kakfa which is incompatible with what we built.
# To avoid any potential compatibility issues, we are replacing all the jars with our own build.
RUN rm -rf /usr/share/java/kafka/*
COPY --from=downloader /tmp/jars/*.jar /usr/share/java/kafka/

# Entrypoint
ENTRYPOINT ["/usr/bin/bash", "-l", "-c"]
CMD ["-c", "(export CONNECT_REST_ADVERTISED_HOST_NAME=$(wget -qO- http://169.254.169.254/latest/meta-data/local-ipv4); \
    export CONNECT_CLIENT_ID=$(wget -qO- http://169.254.169.254/latest/meta-data/instance-id); \
    export CONNECT_CLIENT_RACK=$(wget -qO- http://169.254.169.254/latest/meta-data/placement/availability-zone); \
    export CONNECT_PRODUCER_CLIENT_RACK=$(wget -qO- http://169.254.169.254/latest/meta-data/placement/availability-zone); \
    export CONNECT_ADMIN_CLIENT_RACK=$(wget -qO- http://169.254.169.254/latest/meta-data/placement/availability-zone); \
    export CONNECT_CONSUMER_CLIENT_RACK=$(wget -qO- http://169.254.169.254/latest/meta-data/placement/availability-zone); \
    export CONNECT_CONSUMER_OVERRIDE_CLIENT_RACK=$(wget -qO- http://169.254.169.254/latest/meta-data/placement/availability-zone); \
    export CONNECT_ADMIN_OVERRIDE_CLIENT_RACK=$(wget -qO- http://169.254.169.254/latest/meta-data/placement/availability-zone); \
    export CLIENT_RACK=$(wget -qO- http://169.254.169.254/latest/meta-data/placement/availability-zone); \
    export CONNECT_GROUP_INSTANCE_ID=$(wget -qO- http://169.254.169.254/latest/meta-data/instance-id); \
    /etc/confluent/docker/run)" \
    ]

HEALTHCHECK NONE

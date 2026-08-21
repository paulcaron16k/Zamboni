# A Spark Connect server that can drive Iceberg on this stack's MinIO.
#
# Built rather than pulled because the stock apache/spark image has neither the
# Iceberg runtime nor an S3A filesystem, and the alternative -- `--packages` at
# container start -- resolves from Maven on every boot. That turns a stack
# start into a network operation and a CI job into a flake.
#
# Versions are pinned here for the same reason .env pins the others: a rebuild
# has to produce the same thing.
ARG SPARK_VERSION=4.0.4
FROM apache/spark:${SPARK_VERSION}-scala2.13-java17-ubuntu

ARG ICEBERG_VERSION=1.11.0
# hadoop-aws must match the Hadoop the Spark image ships, and the AWS SDK must
# match what that hadoop-aws declares -- 3.4.1 names software.amazon.awssdk:bundle
# 2.24.6 in hadoop-project's pom. Mismatch here fails at class-load time with a
# NoSuchMethod deep inside the S3A client, which is a poor way to learn it.
ARG HADOOP_AWS_VERSION=3.4.1
ARG AWS_SDK_VERSION=2.24.6

USER root
RUN set -eux; \
    repo=https://repo1.maven.org/maven2; \
    curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime.jar \
      "$repo/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.0_2.13-${ICEBERG_VERSION}.jar"; \
    curl -fsSL -o /opt/spark/jars/hadoop-aws.jar \
      "$repo/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fsSL -o /opt/spark/jars/aws-sdk-bundle.jar \
      "$repo/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar"
USER spark

# SPARK_NO_DAEMONIZE keeps the server in the foreground. Without it
# start-connect-server.sh backgrounds itself through spark-daemon.sh and exits
# 0, so the container stops immediately and looks like a clean shutdown.
ENV SPARK_NO_DAEMONIZE=1
EXPOSE 15002

# Spark image shared by spark-master, spark-worker and (later) the streaming app.
# The Kafka connector and the PostgreSQL JDBC driver are baked in at /opt/extra-jars
# so no job ever downloads jars at runtime (see spark/conf/spark-defaults.conf).
ARG SPARK_IMAGE=apache/spark:3.5.5-scala2.12-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

USER root

ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar /opt/extra-jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar /opt/extra-jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/extra-jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/extra-jars/
ADD --chmod=644 ${MAVEN}/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/extra-jars/

COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

ENV PYTHONPATH=/opt/project \
    SPARK_CONF_DIR=/opt/project/spark/conf \
    PYSPARK_PYTHON=python3

# The base entrypoint drops from root to the 'spark' user (uid 185) before running the command.
WORKDIR /opt/project

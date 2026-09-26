# Airflow image that can spark-submit to the standalone cluster in client mode.
# Python 3.10 matches the Spark image, because Spark refuses to run when the
# driver (here) and the executors (spark-worker) use different Python minor versions.
ARG AIRFLOW_IMAGE=apache/airflow:2.10.5-python3.10
FROM ${AIRFLOW_IMAGE}

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/java-17 \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17

# Same extra jars as the Spark image: the driver runs in this container.
ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar /opt/extra-jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar /opt/extra-jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/extra-jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/extra-jars/
ADD --chmod=644 ${MAVEN}/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/extra-jars/

USER airflow
# Providers are installed against Airflow's published constraints; pyspark is then
# pinned separately so it matches the cluster exactly.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
      apache-airflow-providers-apache-spark \
      apache-airflow-providers-postgres \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt"
COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt

ENV PYTHONPATH=/opt/project \
    SPARK_CONF_DIR=/opt/project/spark/conf \
    PYSPARK_PYTHON=python3

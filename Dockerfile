FROM nedbank-de-challenge/base:1.0

# Fix incorrect SPARK_HOME from base image
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download Delta JARs at BUILD time so they're baked into the image.
# The scoring system runs with --network=none — JARs must already be present.
# We download exactly the versions pinned in the base image (delta-spark 3.1.0,
# PySpark 3.5.0 = Scala 2.12).
RUN mkdir -p /opt/delta-jars && \
    curl -L -o /opt/delta-jars/delta-spark_2.12-3.1.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -L -o /opt/delta-jars/delta-storage-3.1.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar

COPY pipeline/ pipeline/
COPY config/ config/

CMD ["python", "-m", "pipeline.run_all"]
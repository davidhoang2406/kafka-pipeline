FROM bitnami/spark:4

USER root

# Project Python dependencies (pyspark is already in the bitnami image)
RUN pip install \
    fastavro>=1.9 \
    "pyarrow>=16.0" \
    "minio>=7.2" \
    python-dotenv==1.2.2

# Pre-bake S3A JARs into Spark's classpath — no internet access needed at job runtime
RUN curl -fL -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
    && curl -fL -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

USER 1001

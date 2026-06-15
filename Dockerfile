# ============================================================
# Runtime image — lean image with only what's needed
# ============================================================
FROM docker.io/library/amazonlinux:2023

RUN yum install -y --allowerasing java-17-amazon-corretto wget tar gzip && \
    yum clean all

# Spark runtime (needed for Spark JARs)
RUN wget -q https://dlcdn.apache.org/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3-scala2.13.tgz && \
    tar -xzf spark-3.5.8-bin-hadoop3-scala2.13.tgz && \
    rm spark-3.5.8-bin-hadoop3-scala2.13.tgz && \
    mv spark-3.5.8-bin-hadoop3-scala2.13 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=${PATH}:${SPARK_HOME}/bin
ENV JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
ENV PATH=${PATH}:${JAVA_HOME}/bin
ENV SPARK_LOCAL_HOSTNAME=localhost

# Copy the staged application files (jars + libs folder)
COPY target/stage/ /app/

# Copy logback config
COPY logback.xml /app/logback.xml

# Default entrypoint: java with Spark jars + staged libraries
ENTRYPOINT ["java", \
  "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED", \
  "--add-exports", "java.base/java.nio=ALL-UNNAMED", \
  "--add-opens", "java.base/sun.util.calendar=ALL-UNNAMED", \
  "--add-opens", "java.base/java.nio=ALL-UNNAMED", \
  "-Dlogback.configurationFile=file:/app/logback.xml", \
  "-Dfile.encoding=UTF-8", \
  "-cp", "/app/etl-tool.jar:/app/schema-validator.jar:/app/libs/*:/opt/spark/jars/*", \
  "pro.datawiki.sparkLoader.sparkRun"]

CMD []

# ============================================================
# Stage 1: Builder — compile schema-validator + etl-tool
# ============================================================
FROM amazonlinux:2023 AS builder

RUN yum install -y --allowerasing wget tar java-17-amazon-corretto curl gzip && \
    yum clean all

# Spark (needed at compile time by schema-validator)
RUN wget -q https://dlcdn.apache.org/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3-scala2.13.tgz && \
    tar -xzf spark-3.5.8-bin-hadoop3-scala2.13.tgz && \
    rm spark-3.5.8-bin-hadoop3-scala2.13.tgz && \
    mv spark-3.5.8-bin-hadoop3-scala2.13 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=${PATH}:${SPARK_HOME}/bin
ENV JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
ENV PATH=${PATH}:${JAVA_HOME}/bin

# SBT
RUN curl -L https://www.scala-sbt.org/sbt-rpm.repo > /etc/yum.repos.d/sbt-rpm.repo && \
    yum -y install sbt && \
    yum clean all

WORKDIR /build/etl-tool

# Copy everything (schema-validator is in libs/schema-validator)
COPY . /build/etl-tool/

# Build fat JAR (schema-validator is a subproject, built automatically)
ENV SBT_OPTS="-Xmx4G -Xss4M"
RUN sbt assembly

# ============================================================
# Stage 2: Runtime image — lean image with only what's needed
# ============================================================
FROM amazonlinux:2023

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

# Copy the assembled fat JAR from builder stage
RUN mkdir -p /app
COPY --from=builder /build/etl-tool/target/scala-3.4.2/etl-tool.jar /app/etl-tool.jar

# Copy logback config
COPY logback.xml /app/logback.xml

# Default entrypoint: java with Spark jars + etl-tool jar
ENTRYPOINT ["java", \
  "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED", \
  "--add-exports", "java.base/java.nio=ALL-UNNAMED", \
  "--add-opens", "java.base/sun.util.calendar=ALL-UNNAMED", \
  "--add-opens", "java.base/java.nio=ALL-UNNAMED", \
  "-Dlogback.configurationFile=file:/app/logback.xml", \
  "-Dfile.encoding=UTF-8", \
  "-cp", "/app/etl-tool.jar:/opt/spark/jars/*", \
  "pro.datawiki.sparkLoader.sparkRun"]

CMD []

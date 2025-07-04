FROM amazonlinux:2023

RUN yum install -y wget passwd tar java-17-amazon-corretto
RUN yum update -y
## Support
RUN yum install -y mc vim file htop
## Spark
RUN wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3-scala2.13.tgz
RUN tar -xvzf spark-3.5.5-bin-hadoop3-scala2.13.tgz
RUN rm spark-3.5.5-bin-hadoop3-scala2.13.tgz
RUN mv spark-3.5.5-bin-hadoop3-scala2.13 /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=${PATH}:$SPARK_HOME/bin
## SBT
RUN rm -f /etc/yum.repos.d/bintray-rpm.repo || true
RUN curl -L https://www.scala-sbt.org/sbt-rpm.repo > /etc/yum.repos.d/sbt-rpm.repo
RUN yum -y install sbt
## SCALA
RUN wget https://github.com/scala/scala3/releases/download/3.4.2/scala3-3.4.2.tar.gz
RUN tar -xvzf scala3-3.4.2.tar.gz
RUN rm scala3-3.4.2.tar.gz
RUN mv scala3-3.4.2 /opt/scala
ENV SCALA_HOME=/opt/scala
ENV PATH=${PATH}:$SCALA_HOME/bin
ENV SPARK_LOCAL_HOSTNAME=localhost
ENV JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto/
ENV PATH=${PATH}:$JAVA_HOME/bin
## SSH connect
RUN yum install -y openssh-server
RUN echo "PermitRootLogin yes" >> /etc/ssh/sshd_config
RUN sed -i -e 's/PasswordAuthentication no/PasswordAuthentication yes/g' /etc/ssh/sshd_config

RUN echo 'Docker!' | passwd --stdin root
## Google chrome
COPY docker/google.repo /etc/yum.repos.d/google-chrome.repo
RUN yum install -y google-chrome



RUN yum install procps-ng
CMD ["/usr/sbin/init"]

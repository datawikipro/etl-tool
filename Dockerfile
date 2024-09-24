FROM dokken/centos-stream-9
ENV container docker
ARG PASSWORD=racoiaws
EXPOSE 22
ARG USERNAME=root

RUN yum -y install epel-release
RUN yum install -y wget passwd
RUN yum update -y
#support
RUN yum install -y mc vim file htop

# init ssh
RUN yum install -y openssh-server openssh-clients initscripts
RUN ssh-keygen -f /etc/ssh/ssh_host_rsa_key -N '' -b 4096 -t rsa
RUN ssh-keygen -f /etc/ssh/ssh_host_ed25519_key -N '' -t ed25519
RUN ssh-keygen -f /etc/ssh/ssh_host_ecdsa_key -N '' -t ecdsa
RUN /usr/bin/ssh-keygen -A
RUN echo $PASSWORD | passwd --stdin root

RUN wget https://dlcdn.apache.org/spark/spark-3.4.3/spark-3.4.3-bin-hadoop3-scala2.13.tgz
RUN tar -xvzf spark-3.4.3-bin-hadoop3-scala2.13.tgz
RUN rm spark-3.4.3-bin-hadoop3-scala2.13.tgz
RUN mv spark-3.4.3-bin-hadoop3-scala2.13 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=${PATH}:$SPARK_HOME/bin

# SBT
RUN rm -f /etc/yum.repos.d/bintray-rpm.repo || true
RUN curl -L https://www.scala-sbt.org/sbt-rpm.repo > sbt-rpm.repo
RUN mv sbt-rpm.repo /etc/yum.repos.d/
RUN yum -y install sbt

# SCALA
RUN yum install -y java
RUN wget https://github.com/scala/scala3/releases/download/3.4.2/scala3-3.4.2.tar.gz
RUN tar -xvzf scala3-3.4.2.tar.gz
RUN rm scala3-3.4.2.tar.gz
RUN mv scala3-3.4.2 /opt/scala

ENV SCALA_HOME=/opt/scala
ENV PATH=${PATH}:$SCALA_HOME/bin

#support
RUN yum install -y mc file htop

ENV SPARK_LOCAL_HOSTNAME=localhost
#ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-11.0.23.0.9-2.el7_9.x86_64
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-11.0.20.1.1-2.el9.aarch64
ENV PATH=${PATH}:$JAVA_HOME/bin

# Airflow
RUN yum install -y pip
RUN pip install apache-airflow[celery,psycopg2]==2.10.1
RUN pip install psycopg2-binary

CMD ["/usr/sbin/init"]

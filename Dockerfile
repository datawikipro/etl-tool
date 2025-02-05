FROM dokken/centos-stream-9
#ARG USERNAME=root

RUN yum -y install epel-release
RUN yum install -y wget passwd
RUN yum update -y
#support
RUN yum install -y mc vim file htop

RUN wget https://dlcdn.apache.org/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3-scala2.13.tgz

RUN tar -xvzf spark-3.4.4-bin-hadoop3-scala2.13.tgz
RUN rm spark-3.4.4-bin-hadoop3-scala2.13.tgz
RUN mv spark-3.4.4-bin-hadoop3-scala2.13 /opt/spark
#
ENV SPARK_HOME=/opt/spark
ENV PATH=${PATH}:$SPARK_HOME/bin
#
## SBT
RUN rm -f /etc/yum.repos.d/bintray-rpm.repo || true
RUN curl -L https://www.scala-sbt.org/sbt-rpm.repo > sbt-rpm.repo
RUN mv sbt-rpm.repo /etc/yum.repos.d/
RUN yum -y install sbt
#
## SCALA
RUN yum install -y java
RUN wget https://github.com/scala/scala3/releases/download/3.4.2/scala3-3.4.2.tar.gz
RUN tar -xvzf scala3-3.4.2.tar.gz
RUN rm scala3-3.4.2.tar.gz
RUN mv scala3-3.4.2 /opt/scala
#
ENV SCALA_HOME=/opt/scala
ENV PATH=${PATH}:$SCALA_HOME/bin
#
#support
RUN yum install -y mc file htop

COPY docker/google.repo /etc/yum.repos.d/google-chrome.repo
RUN yum install -y google-chrome

ENV SCALA_HOME=/opt/scala
ENV PATH=${PATH}:$SCALA_HOME/bin
ENV SPARK_LOCAL_HOSTNAME=localhost
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-17.0.13.0.11-4.el9.x86_64/
ENV PATH=${PATH}:$JAVA_HOME/bin
ENV JAVA_OPTS='--add-exports java.base/sun.nio.ch=ALL-UNNAMED'
#ENV SBT_OPTS='-Xmx4G'

RUN yum install -y openssh-server
RUN echo "PermitRootLogin yes" >> /etc/ssh/sshd_config
RUN echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config

RUN echo 'Docker!' | passwd --stdin root

#ENTRYPOINT [ "sh", "-c"]
CMD ["/usr/sbin/init"]

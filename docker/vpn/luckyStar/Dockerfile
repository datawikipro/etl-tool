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
RUN ssh-keygen -f /root/.ssh/id_rsa -N '' -b 4096 -t rsa
RUN cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

RUN /usr/bin/ssh-keygen -A
RUN echo $PASSWORD | passwd --stdin root

# OpenVpn
RUN yum install -y https://packages.openvpn.net/openvpn-openvpn3-epel-repo-1-1.noarch.rpm
RUN yum install -y openvpn3-client

RUN echo 'PasswordAuthentication no' >> /etc/ssh/sshd_config
RUN echo 'HostKey /root/.ssh/id_rsa' >> /etc/ssh/sshd_config
RUN echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config
RUN echo 'PubkeyAuthentication yes' >> /etc/ssh/sshd_config
RUN echo 'RSAAuthentication yes' >> /etc/ssh/sshd_config
RUN echo 'KexAlgorithms curve25519-sha256@libssh.org,ecdh-sha2-nistp256,ecdh-sha2-nistp384,ecdh-sha2-nistp521,diffie-hellman-group-exchange-sha256,diffie-hellman-group14-sha1,diffie-hellman-group-exchange-sha1,diffie-hellman-group1-sha1' >> /etc/ssh/sshd_config


RUN echo '#!/bin/bash'>> /root/vpn.sh
RUN echo 'openvpn3 config-import --config /root/.vpn/LackyStar.1win.pro.ovpn --name vpn;'>> /root/vpn.sh
RUN echo 'openvpn3 config-manage --config vpn --allow-compression yes;'>> /root/vpn.sh
RUN echo 'printf "achernousov\nO~7JF[HN7l7M1i*k\nj2LievR0zn0IjD4NmmbFBwBG9Dl1cMxx\n" | openvpn3 session-start --config vpn' >> /root/vpn.sh


RUN echo '[Unit]' >> /etc/systemd/system/vpn_network.service
RUN echo 'Description=Service description' >> /etc/systemd/system/vpn_network.service
RUN echo '[Service]' >> /etc/systemd/system/vpn_network.service
RUN echo 'ExecStart=/root/vpn.sh' >> /etc/systemd/system/vpn_network.service
RUN echo '[Install]' >> /etc/systemd/system/vpn_network.service
RUN echo 'WantedBy=default.target' >> /etc/systemd/system/vpn_network.service
RUN chmod 777 /root/vpn.sh
RUN systemctl enable vpn_network

CMD ["/usr/sbin/init"]

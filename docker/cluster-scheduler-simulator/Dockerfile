FROM ubuntu:14.04

MAINTAINER Francesco Pace <francesco.pace@eurecom.fr>

# Install Software properties
RUN apt-get update && apt-get install -y --force-yes \
					software-properties-common \
					python-software-properties \
					python3-software-properties
# Install Java7
RUN apt-add-repository -y ppa:webupd8team/java
RUN /bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
RUN apt-get update && apt-get -y --force-yes install \
				oracle-java7-installer \
				oracle-java7-set-default curl
ENV JAVA_HOME /usr/lib/jvm/java-7-oracle/

# Install common packages
RUN apt-get update && apt-get install -y --force-yes \
						build-essential \
						wget \
						curl \
						zip

# Install SCALA
ENV SCALA_VERSION="2.10.4"
RUN apt-get update && apt-get install -y --force-yes libjansi-java
RUN curl -sSO http://downloads.typesafe.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.deb && \
                       dpkg -i scala-${SCALA_VERSION}.deb && \
                            rm scala-${SCALA_VERSION}.deb

# Install SBT
ENV SBT_VERSION="0.13.7"
RUN echo "deb http://dl.bintray.com/sbt/debian/ /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN apt-get update && apt-get install -y --force-yes sbt=${SBT_VERSION}
# Download the standard libraries required by sbt
RUN sbt

## Install Python3 and some packages
#RUN apt-get update && apt-get install -y --force-yes \
#					python3-dev \
#					python3-setuptools \
#					python3-pip \
#					python3-matplotlib
## Install PIP and some packages
#RUN pip3 install \
#    protobuf==3.0.0-b3

# Install Python2 and some packages
RUN apt-get update && apt-get install -y --force-yes \
					python-dev \
					python-setuptools \
					python-pip \
					python-matplotlib
# Install PIP and some packages
RUN pip install \
    protobuf==2.6.1

# Install Postfix.
# pre config
RUN echo mail > /etc/hostname; \
    echo "postfix postfix/main_mailer_type string Internet site" > preseed.txt; \
    echo "postfix postfix/mailname string mail.example.com" >> preseed.txt
# load pre config for apt
RUN debconf-set-selections preseed.txt
# install
RUN apt-get update; apt-get install -y \
    postfix \
    mailutils
# configure
RUN postconf -e smtpd_banner="\$myhostname ESMTP"; \
    postconf -e mail_spool_directory="/var/spool/mail/"; \
    postconf -e mailbox_command=""
# ports
EXPOSE 25

# Set Default CMD
CMD ["bash", "/mnt/workspace/cluster-scheduler-simulator/bin/launch.sh"]


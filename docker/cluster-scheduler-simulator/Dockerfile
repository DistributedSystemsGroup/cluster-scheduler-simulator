FROM ubuntu:14.04

MAINTAINER Francesco Pace <francesco.pace@eurecom.fr>

# Install Software properties
RUN apt-get update && apt-get install -y --force-yes \
					software-properties-common \
					python-software-properties
# Install Java7
RUN apt-add-repository -y ppa:webupd8team/java
RUN /bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
RUN apt-get update && apt-get -y install \
				oracle-java7-installer \
				oracle-java7-set-default curl
ENV JAVA_HOME /usr/lib/jvm/java-7-oracle/

# Install common packages
RUN apt-get update && apt-get install -y --force-yes \
						build-essential \
						wget \
						curl

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

# Install Python and some packages
RUN apt-get update && apt-get install -y --force-yes \
					python-dev \
					python-setuptools \
					python-matplotlib
# Install PIP and some packages
RUN easy_install pip
RUN pip install \
    protobuf==2.6.0

# Set Default CMD
CMD ["/bin/bash"]


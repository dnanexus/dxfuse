# Testing docker image
FROM ubuntu:18.04
ENV DEBIAN_FRONTEND noninteractive

# install python3
#RUN apt-get update && \
#    apt-get install -y libffi-dev python3 python3-pip python3-venv
RUN apt-get update && \
    apt-get install -y git sudo python3 python3-pip

# Various setting to get python3 to work reliably, especially,
# with the dx-toolkit.

# Set the locale to UTF-8
RUN apt-get install -y locales && \
    locale-gen "en_US.UTF-8"

# Language settings
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV LC_CTYPE=en_US.UTF-8

# Set python3 as default python implementation
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

# Install printing with colors, and dx-toolkit
RUN pip3 install termcolor dxpy

# Set up a directory with all the scripts we want to run
CMD mkdir -p /dxfs2_workdir
ENV DIRPATH  /dxfs2_workdir

COPY ./dxfs2 $DIRPATH/
COPY ./streaming_benchmark.py $DIRPATH/
COPY ./correctness_test.py $DIRPATH/


# Environment variables so that dx-toolkit and dxfs2 can work
CMD mkdir -p /home/dnanexus
CMD mkdir -p /home/dnanexus/.dnanexus_config
COPY ./cfg.json /home/dnanexus/.dnanexus_config/environment.json
# Testing docker image
FROM ubuntu:18.04
ENV DEBIAN_FRONTEND noninteractive

CMD mkdir -p /dxfs2_workdir
ENV DIRPATH  /dxfs2_workdir

COPY ./dxfs2 $DIRPATH/
COPY ./*.py $DIRPATH/
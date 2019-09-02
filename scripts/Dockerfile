# Testing docker image
FROM ubuntu:18.04
SHELL ["/bin/bash", "-c"]
ENV DEBIAN_FRONTEND noninteractive
ENTRYPOINT ["/bin/bash"]

CMD mkdir -p /dxfs2_workdir
ENV DIRPATH  /dxfs2_workdir

COPY /go/bin/dxfs2 $DIRPATH/
COPY ./*.py $DIRPATH/
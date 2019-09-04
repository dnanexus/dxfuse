DXWDL_VERSION=1.20a

all : clean dxfs2 dx_testbox_image wdl_test

dxfs2 :
	go build -o dxfs2 /go/src/github.com/dnanexus/dxfs2/cmd/main.go

local_docker_image :
	sudo docker build -t dxfs2_testbox .

dx_testbox_image: local_docker_image
	sudo docker image save dxfs2_testbox --output dxfs2_testbox.tar
	sudo chmod 644 dxfs2_testbox.tar
	dx upload dxfs2_testbox.tar --destination dxfs2_test_data:/images/

dxWDL-${DXWDL_VERSION}.jar :
	wget https://github.com/dnanexus/dxWDL/releases/download/${DXWDL_VERSION}/dxWDL-${DXWDL_VERSION}.jar

wdl_test: dxWDL-${DXWDL_VERSION}.jar
	java -jar dxWDL-${DXWDL_VERSION}.jar compile test.wdl --extras extras.json --project dxfs2_test_data --folder /applets --force

clean :
	sudo docker rmi dxfs2_testbox || true
	dx rm --force dxfs2_test_data:/images/dxfs2_testbox.tar || true

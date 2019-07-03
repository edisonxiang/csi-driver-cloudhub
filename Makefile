.PHONY: all build csi-driver-cloudhub docker clean

all:build

build:csi-driver-cloudhub

csi-driver-cloudhub:
	go build -o ./cmd/csi-driver-cloudhub/csi-driver-cloudhub ./cmd/csi-driver-cloudhub

docker:csi-driver-cloudhub
	docker build cmd/csi-driver-cloudhub -t xiangxinyong/csi-driver-cloudhub:latest

clean:
	rm -rf ./cmd/csi-driver-cloudhub/csi-driver-cloudhub

# Based on centos
FROM centos:7.6.1810
LABEL maintainers="Kubernetes Authors"
LABEL description="CloudHub CSI Driver"

# Copy from build directory
COPY csi-driver-cloudhub /csi-driver-cloudhub

# Update
RUN yum -y update

# Define default command
ENTRYPOINT ["/csi-driver-cloudhub"]

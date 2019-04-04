#!/bin/bash

# Fail on any error
set -e

ETCD_VERSION=v3.3.12

INSTALL_DIR="${1:-$HOME/etcd}"

# Download and install etcd
cd /tmp
wget -q https://storage.googleapis.com/etcd/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-amd64.tar.gz
mkdir -p ${INSTALL_DIR}
tar xzf etcd-${ETCD_VERSION}-linux-amd64.tar.gz -C ${INSTALL_DIR} --strip-components=1
rm -rf etcd*.gz
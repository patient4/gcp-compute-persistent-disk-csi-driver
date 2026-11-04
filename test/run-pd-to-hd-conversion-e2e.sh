#!/bin/bash

# Runs against an existing Kubernetes cluster with the PD CSI driver deployed.
# Application Default Credentials must be able to create and delete disks and
# instant snapshots in GCP_PROJECT.
set -o nounset
set -o errexit

: "${KUBECONFIG:?KUBECONFIG must point to a cluster with the PD CSI driver deployed}"
: "${GCP_PROJECT:=${GOOGLE_CLOUD_PROJECT:-}}"
: "${GCP_PROJECT:?GCP_PROJECT or GOOGLE_CLOUD_PROJECT must identify the disk project}"
export GCP_PROJECT

readonly PKGDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PKGDIR}"
go test -v -count=1 -timeout=12h -run '^TestDiskConversionE2E$' ./test/e2e/tests

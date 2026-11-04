#!/bin/bash

set -o nounset
set -o errexit
set -x

echo Using GOPATH ${GOPATH:-}

readonly PKGDIR=sigs.k8s.io/gcp-compute-persistent-disk-csi-driver

# This requires application default credentials to be set up, eg by
# `gcloud auth application-default login`
# Set RUN_PD_TO_HD_CONVERSION_E2E=true to run conversion specs against an
# existing Kubernetes cluster instead of provisioning the VM-based test setup.

CLOUDTOP_HOST=
if [ -z "${CLOUDTOP_HOST}" ] && hostname | grep -q c.googlers.com ; then
  CLOUDTOP_HOST=--cloudtop-host
fi

if [ "${RUN_PD_TO_HD_CONVERSION_E2E:-false}" = "true" ]; then
  : "${KUBECONFIG:?KUBECONFIG must point to a Kubernetes cluster with the PD CSI driver deployed}"
  : "${GCP_PROJECT:=${GOOGLE_CLOUD_PROJECT:-}}"
  : "${GCP_PROJECT:?GCP_PROJECT or GOOGLE_CLOUD_PROJECT must identify the disk project}"
  export GCP_PROJECT

  ginkgo --v --focus="PD to Hyperdisk disk type conversion" "./test/e2e/tests" -- --v=6 --logtostderr "$@"
else
  ginkgo --v "./test/e2e/tests" -- --project "${PROJECT}" --service-account "${IAM_NAME}" "${CLOUDTOP_HOST}" --v=6 --logtostderr "$@"
fi

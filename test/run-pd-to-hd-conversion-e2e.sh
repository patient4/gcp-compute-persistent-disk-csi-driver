#!/bin/bash

# Runs against an existing Kubernetes cluster with the PD CSI driver deployed.
# Application Default Credentials must be able to create and delete disks and
# instant snapshots in GCP_PROJECT.
set -o nounset
set -o errexit

readonly TESTDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd "${TESTDIR}/.." && pwd)"
cd "${REPO_ROOT}"
RUN_PD_TO_HD_CONVERSION_E2E=true "${TESTDIR}/run-e2e-local.sh" "$@"

#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

# The root of the build/dist directory
HYPER_ROOT=$(readlink -f $(dirname "${BASH_SOURCE}")/../..)

HYPER_OUTPUT_BINPATH="${HYPER_ROOT}"

source "${HYPER_ROOT}/hack/lib/util.sh"
source "${HYPER_ROOT}/hack/lib/logging.sh"

hyper::log::install_errexit

source "${HYPER_ROOT}/hack/lib/version.sh"
source "${HYPER_ROOT}/hack/lib/test.sh"
source "${HYPER_ROOT}/hack/lib/hyperstart.sh"

HYPER_OUTPUT_HOSTBIN="${HYPER_OUTPUT_BINPATH}"

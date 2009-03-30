#!/bin/bash

vp_dir=$(dirname $0)

${vp_dir}/run-groovy.sh src/main/groovy/vpork/VPork.groovy $@

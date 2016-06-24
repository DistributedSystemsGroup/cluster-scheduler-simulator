#!/usr/bin/env bash

############# Script Variables ###############################
script_dir=$(readlink -f $(dirname ${0}))
project_dir=$(cd "${script_dir}/.."; pwd)
project_name=$(basename ${project_dir})
##############################################################

### Functions ###

# trap ctrl-c and call ctrl_c()
trap ctrl_c INT

function ctrl_c() {
    exit 1
}


# This function run the simulation script inside a container
function run_simulation(){
    cd ${project_dir};
#    rm -rf experiment_results

    set -e
    bash bin/sbt "run"
    bash bin/generate-graphs.sh "$(ls -d -1 -t $PWD/experiment_results/** | head -1)"
    set +e
}
#################

### Script ######
START_TIME=$(date +%s)

run_simulation

echo "It took $(($(date +%s) - ${START_TIME})) seconds to complete"

#################
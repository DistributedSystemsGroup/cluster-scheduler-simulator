#!/usr/bin/env bash

############# Script Variables ###############################
script_dir=$(readlink -f $(dirname ${0}))
project_dir=$(cd "${script_dir}/.."; pwd)
project_name=$(basename ${project_dir})
TO_MAIL="francesco.pace@eurecom.fr"
##############################################################

### Functions ###

# trap ctrl-c and call ctrl_c()
#trap ctrl_c INT
#
#function ctrl_c() {
#    exit 1
#}


# This function run the simulation script inside a container
function run_simulation(){
    pushd ${project_dir}
#    rm -rf experiment_results

    set -e
    mkdir logs
    date=$(date +%Y-%m-%d-%H-%M-%S)
    bash bin/sbt "run" | tee "logs/output_${date}.log"
    bash bin/generate-graphs.sh "$(ls -d -1 -t $PWD/experiment_results/** | head -1)" | tee "logs/output_${date}.log"
    exit_code=$?
    set +e
    popd
    return ${exit_code}
}

# This function send a notification mail to a recipient when the simulation ends.
# We attach the results as well
function send_mail(){
    success=${1}

    pushd ${project_dir}
    if [ ${success} -eq 0 ]; then
        zips_folder="experiment_zips"
        if [ ! -d "${zips_folder}" ]; then
          mkdir -p ${zips_folder}
        fi
        last_created_dir="$(ls -d -1 -t experiment_results/** | head -1)"
        zip_name="${zips_folder}/$(basename ${last_created_dir}).zip"
        zip -r ${zip_name} ${last_created_dir}
        attachment="-A ${zip_name}"
        actualsize=$(du -k "${zip_name}" | cut -f 1)
        if [ ${actualsize} -lt 5242880 ];then
            echo -e \
                "Cluster Scheduler Simulation ended with Success.\n" \
                "You can find the results in the attachment(s).\n" \
                "Cheers :)\n" \
                | mail -s "Cluster-Scheduler-Simulator" ${attachment} "${TO_MAIL}"
        else
            echo -e \
                    "Cluster Scheduler Simulation ended with Success.\n" \
                    "The zip file for the results was too big for an attachment.\n" \
                    "Please log in the machine and download it (${zip_name}).\n" \
                    "Cheers :)\n" \
                    | mail -s "Cluster-Scheduler-Simulator" "${TO_MAIL}"
        fi
    else
        echo -e \
            "Cluster Scheduler Simulation ended with an Error.\n" \
            "Sorry :(\n" \
            | mail -s "Cluster-Scheduler-Simulator" "${TO_MAIL}"
    fi
    popd
}
#################

### Script ######
START_TIME=$(date +%s)

original_project_dir="${project_dir}"
cp -r ${project_dir} ${HOME}
project_dir="${HOME}/${project_name}"

run_simulation
ret=$?
send_mail ${ret}

mkdir "${original_project_dir}/../logs"
cp -r ${project_dir}/experiment_* "${original_project_dir}/../logs"

echo "It took $(($(date +%s) - ${START_TIME})) seconds to complete"

#################
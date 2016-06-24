#!/usr/bin/env bash

############# User Variables #################################
SWARM_IP="192.168.45.252:2380"
CONTAINER_NAME="cluster-scheduler-simulator"
CONTAINER_IMAGE="192.168.45.252:5000/pacerepo/cluster-scheduler-simulator"
TO_MAIL="francesco.pace@eurecom.fr"
##############################################################

############# Script Variables ###############################
script_dir=$(readlink -f $(dirname ${0}))
project_dir=$(cd "${script_dir}/.."; pwd)
project_name=$(basename ${project_dir})
remote_dir="/tmp/${project_name}"
docker="sudo docker -H ${SWARM_IP}"
##############################################################

### Functions ###

# trap ctrl-c and call ctrl_c()
trap ctrl_c INT

function ctrl_c() {
    ${docker} kill ${CONTAINER_NAME}
    exit 1
}

# This function setup the container and execute the script to start the simulation
# When the container exits, we copy the files back to the host
function run_container(){
    task="run-container"

    echo "# User Settings:"
    echo "#     SWARM_IP: ${SWARM_IP}"
    echo "#     CONTAINER_NAME: ${CONTAINER_NAME}"
    echo "#     CONTAINER_IMAGE: ${CONTAINER_IMAGE}"
    echo ""
    echo "# Script Variables:"
    echo "#     script_dir: ${script_dir}"
    echo "#     project_dir: ${project_dir}"
    echo "#     project_name: ${project_name}"
    echo "#     remote_dir: ${remote_dir}"
    echo ""

    if $(${docker} inspect ${CONTAINER_NAME} > /dev/null 2>&1); then
        ${docker} rm -f ${CONTAINER_NAME}
    fi
    container_command="bash ${remote_dir}/bin/launch_on_container.sh run-simulation"
    ${docker} create --name ${CONTAINER_NAME} -it ${CONTAINER_IMAGE} ${container_command}
    ${docker} cp "${project_dir}" "${CONTAINER_NAME}:${remote_dir}"
    ${docker} start ${CONTAINER_NAME}
    ${docker} logs -f ${CONTAINER_NAME}
    container_exist_code=$(${docker} inspect -f '{{.State.ExitCode}}' ${CONTAINER_NAME})

    if [ "${container_exist_code}" == "0" ];then
        ${docker} cp "${CONTAINER_NAME}:${remote_dir}/experiment_results" "${project_dir}/"
        sudo chown -R ${USER}:${USER} "${project_dir}/experiment_results"
        ${docker} rm -f ${CONTAINER_NAME}
    fi
    return ${container_exist_code}
}

# This function run the simulation script inside a container
function run_simulation(){
    task="run-simulation"

    cd ${remote_dir};
    rm -rf experiment_results

    # For reason that I do not known (perhaps I lack knowledge on Docker)
    # the error propagation does not work with a simple return 1
    # but only by using set -e
    set -e
    bash bin/sbt "run"
    bash bin/generate-graphs.sh "$(ls -d -1 -t $PWD/experiment_results/** | head -1)"
    set +e
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
        echo -e \
            "Cluster Scheduler Simulation ended with Success.\n" \
            "You can find the results in the attachment(s).\n" \
            "Cheers :)\n" \
            | mail -s "Cluster-Scheduler-Simulator" ${attachment} "${TO_MAIL}"
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
if [ ! -z "${1}" ] && [ "${1}" == "run-simulation" ]; then
    run_simulation
else
    run_container
    ret=$?
    send_mail ${ret}
fi
echo "It took $(($(date +%s) - ${START_TIME})) seconds to complete task ${task}."

#################
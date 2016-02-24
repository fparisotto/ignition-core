#!/bin/bash

# We suppose we are in a subdirectory of the root project
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

JOB_NAME="${1:?Please give a Job Name}"
JOB_DATE="${2?Please give the Job Date}"
JOB_TAG="${3?Please give the Job Tag}"
JOB_USER="${4?Please give the Job User}"
CONTROL_DIR="${5?Please give the Control Directory}"
SPARK_MEM_PARAM="${6?Please give the Job Memory Size to use}"
USE_YARN="${7?Please tell if we should use YARN (yes/no)}"
NOTIFY_ON_ERRORS="${8?Please tell if we will notify on errors (yes/no)}"
DRIVER_HEAP_SIZE="${9?Please tell driver heap size to use}"

JOB_WITH_TAG=${JOB_NAME}.${JOB_TAG}
JOB_CONTROL_DIR="${CONTROL_DIR}/${JOB_WITH_TAG}"
MY_USER=$(whoami)

# Avoids problems when another user created our control dir
sudo mkdir -p "${JOB_CONTROL_DIR}"
sudo chown $MY_USER "${JOB_CONTROL_DIR}"


RUNNING_FILE="${JOB_CONTROL_DIR}/RUNNING"
echo $$ > "${RUNNING_FILE}"

notify_error_and_exit() {
    description="${1}"
    echo "Exiting because: ${description}"
    echo "${description}" > "${JOB_CONTROL_DIR}/FAILURE"
    # Do not notify if NOTIFY_ON_ERRORS is different from yes
    [[ "${NOTIFY_ON_ERRORS}" != 'yes' ]] && exit 1
    # TODO: create some generic method to notify on errors
    exit 1
}

get_first_present() {
    for dir in "$@"; do
        if [[ -e "$dir" ]]; then
            echo $dir
            break
        fi
    done
}

on_trap_exit() {
    rm -f "${JOB_CONTROL_DIR}"/*.jar
    rm -f "${RUNNING_FILE}"
}

install_and_run_zeppelin() {
    if [[ ! -d "zeppelin" ]]; then
        wget "http://www.us.apache.org/dist/incubator/zeppelin/0.5.6-incubating/zeppelin-0.5.6-incubating-bin-all.tgz" -O zeppelin.tar.gz
        mkdir zeppelin
        tar xvzf zeppelin.tar.gz -C zeppelin --strip-components 1 > /tmp/zeppelin_install.log
    fi
    if [[ -f "zeppelin/bin/zeppelin.sh" ]]; then
        export MASTER="${JOB_MASTER}"
        export ZEPPELIN_PORT="8081"
        export SPARK_HOME="/root/spark"
        export SPARK_SUBMIT_OPTIONS="--jars ${JAR_PATH} --runner-executor-memory ${SPARK_MEM_PARAM}"
        sudo -E zeppelin/bin/zeppelin.sh
    else
        notify_error_and_exit "Zepellin installation not found"
    fi
}


trap "on_trap_exit" EXIT

SPARK_HOME=$(get_first_present /root/spark /opt/spark ~/spark*/)
source "${SPARK_HOME}/conf/spark-env.sh"

MAIN_CLASS="ignition.jobs.Runner"

cd "${DIR}" || notify_error_and_exit "Internal script error for job ${JOB_WITH_TAG}"

JAR_PATH_SRC=$(echo "${DIR}"/*assembly*.jar)
JAR_PATH="${JOB_CONTROL_DIR}/Ignition.jar"

cp ${JAR_PATH_SRC} ${JAR_PATH}

export JOB_MASTER=${MASTER}

if [[ "${USE_YARN}" == "yes" ]]; then
    export YARN_MODE=true
    export JOB_MASTER="yarn-client"
    export SPARK_JAR=$(echo ${SPARK_HOME}/assembly/target/scala-*/spark-assembly-*.jar)
    export SPARK_YARN_APP_JAR=${JAR_PATH}
    export SPARK_WORKER_MEMORY=${SPARK_MEM_PARAM}
fi

if [[ "${JOB_NAME}" == "shell" ]]; then
    export ADD_JARS=${JAR_PATH}
    sudo -E ${SPARK_HOME}/bin/spark-shell || notify_error_and_exit "Execution failed for shell"
elif [[ "${JOB_NAME}" == "zeppelin" ]]; then
    install_and_run_zeppelin
else
    JOB_OUTPUT="${JOB_CONTROL_DIR}/output.log"
    tail -F "${JOB_OUTPUT}" &
    sudo -E "${SPARK_HOME}/bin/spark-submit" --master "${JOB_MASTER}" --driver-memory "${DRIVER_HEAP_SIZE}" --driver-java-options "-Djava.io.tmpdir=/mnt -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps" --class "${MAIN_CLASS}" ${JAR_PATH} "${JOB_NAME}" --runner-date "${JOB_DATE}" --runner-tag "${JOB_TAG}" --runner-user "${JOB_USER}" --runner-master "${JOB_MASTER}" --runner-executor-memory "${SPARK_MEM_PARAM}" >& "${JOB_OUTPUT}" || notify_error_and_exit "Execution failed for job ${JOB_WITH_TAG}"
fi

touch "${JOB_CONTROL_DIR}/SUCCESS"

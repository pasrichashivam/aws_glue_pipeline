#!/bin/bash -e

# File: create_sw_emr_artifacts.sh
# Description: create tfvars artifacts for deployment

SCRIPT_LOG_STAMP="SCRIPT ---create sw_emr_artifacts.sh --- ";
declare -a ENV_VAR_LIS;
RED="\033[1;31m";
GREEN="\033[1;32m";
NOCOLOR="\033[0m" ;
ENV_VAR_LIS=('REPOSITORY_NAME');

echo $(date) ${SCRIPT_LOG_STAMP} "START";

if [ -z "${SHELL_SCRIPTS_PATH}" ]; then
    echo -e ${RED} $(date) ${SCRIPT_LOG_STAMP} "environment variable SHELL_SCRIPTS_PATH is not set";
    echo -e ${NOCOLOR};
    exit 1;
fi;

create_artifacts_error=false;
zip_size=0;
print_zip_size=0;
new_zip_file_name="";

env_file_name="venv-${REPOSITORY_NAME}.tar.gz"
src_file_name="pyfiles.zip"
workspace_emr_source_path="${WORKSPACE}/src"
VENV_PATH="${WORKSPACE}/venv"

if [ -d "${workspace_emr_source_path}" ]; then
    cd "${WORKSPACE}"
    mkdir -p artifact
    cd artifact
    echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "Creating the environment...";
    python3 -m venv ${VENV_PATH}
    echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "activating the environment...";
    source ${VENV_PATH}/bin/activate || create_artifacts_error=true;
    echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "installing the ${REPOSITORY_NAME} dependencies...";
    pip3 install -q --upgrade pip || create_artifacts_error=true;
    pip3 install -q -U -r "${workspace_emr_source_path}/requirements.txt" || create_artifacts_error=true;
    echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "creating venv-pack ${env_file_name}...";

    venv-pack -q -f -p ${VENV_PATH} -o "${env_file_name}" --python-prefix /home/hadoop/environment || create_artifacts_error=true;

    echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "disabling the environment ${VENV_PATH}...";
    deactivate || create_artifacts_error=true;

    echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "move main.py file to root";
    mv "${workspace_emr_source_path}/main.py" "${WORKSPACE}/artifact" || create_artifacts_error=true;

    echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "creating zip from src ...";
    cd "${workspace_emr_source_path}"
    zip -u -q -r "${workspace_emr_source_path}/${src_file_name}" . --include '*.py' \
        -x main.py -x 'requirements.txt' || create_artifacts_error=true;

    if [ -d "${WORKSPACE}/config" ]; then
        cd "${WORKSPACE}"
        echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "creating zip from config/ ...";
        zip -u -q -r "${workspace_emr_source_path}/${src_file_name}" config/ \
            --include '*.yaml' || create_artifacts_error=true;
    fi;

    if [ -d "${WORKSPACE}/query" ]; then
        cd "${WORKSPACE}"
        echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "creating zip from query/ ...";
        zip -u -q -r "${workspace_emr_source_path}/${src_file_name}" query/ \
            --include '*.sql' || create_artifacts_error=true;
    fi;

    echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "move pyfiles.zip to root...";
    mv  "${workspace_emr_source_path}/pyfiles.zip" "${WORKSPACE}/artifact" || create_artifacts_error=true;

else
    echo -e ${RED} $(date) ${SCRIPT_LOG_STAMP} "${workspace_emr_source_path} PATH not found";
    echo -e ${NOCOLOR};
    create_artifacts_error=true;
fi;

if [ "${create_artifacts_error}" = "true" ]; then
    echo -e ${RED} $(date) ${SCRIPT_LOG_STAMP} "Build completed failed";
    exit 1;
else
    echo -e ${GREEN} $(date) ${SCRIPT_LOG_STAMP} "Build completed successfuly";
    exit 0;
fi;

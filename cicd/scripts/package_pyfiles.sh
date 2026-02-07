#!/bin/bash -e

# Description: create tfvars artifacts for deployment

SCRIPT_LOG_STAMP="SCRIPT ---create package_pyfiles.sh --- ";
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

src_file_name="pyfiles.zip"
workspace_source_path="${WORKSPACE}/src"

if [ -d "${workspace_source_path}" ]; then
    cd "${WORKSPACE}"
    mkdir -p artifact
    mkdir -p wheelhouse
    cd artifact

    echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "downloading the ${REPOSITORY_NAME} dependencies...";
    pip3.11 install -q --upgrade pip || create_artifacts_error=true;
    pip3.11 wheel --wheel-dir=wheelhouse -r "${workspace_source_path}/requirements.txt" || create_artifacts_error=true;

    echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "move requirements.txt file to root";
    mv "${workspace_source_path}/requirements.txt" "${WORKSPACE}/artifact" || create_artifacts_error=true;

    echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "move main.py file to root";
    mv "${workspace_source_path}/main.py" "${WORKSPACE}/artifact" || create_artifacts_error=true;

    echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "Creating zip of whl files";
    zip -r glue_job_whl_packages.gluewheels.zip wheelhouse/ || create_artifacts_error=true;

    echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "creating zip from src ...";
    cd "${workspace_source_path}"
    zip -u -q -r "${workspace_source_path}/${src_file_name}" . --include '*.py' \
        -x main.py -x 'requirements.txt' || create_artifacts_error=true;

    if [ -d "${WORKSPACE}/config" ]; then
        cd "${WORKSPACE}"
        echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "creating zip from config/ ...";
        zip -u -q -r "${workspace_source_path}/${src_file_name}" config/ \
            --include '*.yaml' || create_artifacts_error=true;
    fi;

    if [ -d "${WORKSPACE}/query" ]; then
        cd "${WORKSPACE}"
        echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "creating zip from query/ ...";
        zip -u -q -r "${workspace_source_path}/${src_file_name}" query/ \
            --include '*.sql' || create_artifacts_error=true;
    fi;

    echo -e ${NOCOLOR} $(date) ${SCRIPT_LOG_STAMP} "move pyfiles.zip to root...";
    mv  "${workspace_source_path}/pyfiles.zip" "${WORKSPACE}/artifact" || create_artifacts_error=true;

else
    echo -e ${RED} $(date) ${SCRIPT_LOG_STAMP} "${workspace_source_path} PATH not found";
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

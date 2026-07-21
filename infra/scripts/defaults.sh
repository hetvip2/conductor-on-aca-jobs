#!/usr/bin/env sh
set -eu
: "${ACA_NAME_PREFIX:=conaca}"
: "${ACA_JOB_IMAGE:=mcr.microsoft.com/k8se/quickstart-jobs:latest}"
azd env set ACA_NAME_PREFIX "$ACA_NAME_PREFIX"
azd env set ACA_JOB_IMAGE "$ACA_JOB_IMAGE"
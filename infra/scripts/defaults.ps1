$ErrorActionPreference = 'Stop'
$prefix = if ($env:ACA_NAME_PREFIX) { $env:ACA_NAME_PREFIX } else { 'conaca' }
$image = if ($env:ACA_JOB_IMAGE) { $env:ACA_JOB_IMAGE } else { 'mcr.microsoft.com/k8se/quickstart-jobs:latest' }
azd env set ACA_NAME_PREFIX $prefix
azd env set ACA_JOB_IMAGE $image
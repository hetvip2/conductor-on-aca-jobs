@description('Azure location for all resources.')
param location string = resourceGroup().location
@description('Prefix used for generated resource names.')
@minLength(3)
param namePrefix string = 'conaca'
@description('Container image used by the sample manual ACA Job.')
param acaJobImage string = 'mcr.microsoft.com/k8se/quickstart-jobs:latest'
var suffix = uniqueString(subscription().subscriptionId, resourceGroup().id)
resource logs 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: '${namePrefix}-law-${suffix}'
  location: location
  properties: { sku: { name: 'PerGB2018' }, retentionInDays: 30 }
}
resource environment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: '${namePrefix}-env-${suffix}'
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: { customerId: logs.properties.customerId, sharedKey: logs.listKeys().primarySharedKey }
    }
  }
}
resource job 'Microsoft.App/jobs@2024-03-01' = {
  name: '${namePrefix}-job-${suffix}'
  location: location
  properties: {
    environmentId: environment.id
    configuration: { triggerType: 'Manual', replicaRetryLimit: 1, replicaTimeout: 1800 }
    template: {
      containers: [{
        name: 'worker'
        image: acaJobImage
        resources: { cpu: json('0.5'), memory: '1Gi' }
      }]
    }
  }
}
output acaJobResourceId string = job.id
output acaJobName string = job.name
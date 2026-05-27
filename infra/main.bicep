targetScope = 'resourceGroup'

@description('Azure region for all resources.')
param location string = resourceGroup().location

@description('Azure region for Azure AI Search. May differ from location when the primary region lacks Search capacity.')
param searchLocation string = location

@description('Short environment name such as dev, test, or prod.')
param environmentName string

@description('Base application name used for Azure resource names.')
param appName string

@description('Fully qualified container image to run. Required to avoid accidental placeholder deployments.')
param containerImage string

@description('Container port exposed by the FastAPI service.')
param containerPort int = 8000

@minValue(0)
@description('Minimum Container App replicas. Use 1 for first real deployments until readiness checks are wired.')
param minReplicas int = 0

@minValue(1)
@description('Maximum Container App replicas.')
param maxReplicas int = 1

@description('Tags applied to all supported resources.')
param tags object = {}

@allowed([
  'Enabled'
  'Disabled'
])
@description('Public network access setting for dev Cosmos account. Set to Disabled when private endpoints are added.')
param cosmosPublicNetworkAccess string = 'Enabled'

@allowed([
  'enabled'
  'disabled'
])
@description('Public network access setting for dev Azure AI Search service. Set to disabled when private endpoints are added.')
param searchPublicNetworkAccess string = 'enabled'

@allowed([
  'Enabled'
  'Disabled'
])
@description('Public network access setting for dev Storage account. Set to Disabled when private endpoints are added.')
param storagePublicNetworkAccess string = 'Enabled'

@allowed([
  'Enabled'
  'Disabled'
])
@description('Public network access setting for dev Service Bus namespace. Premium is required before private endpoints are added.')
param serviceBusPublicNetworkAccess string = 'Enabled'

@description('Azure AI Search index name reserved for tree/root recall.')
param searchRecallIndexName string = 'memforest-recall'

@description('Service Bus queue name for ordered write jobs.')
param serviceBusWritesQueueName string = 'memforest-writes'

var normalizedName = toLower(replace('${appName}-${environmentName}', '_', '-'))
var acrName = replace(take('${replace(normalizedName, '-', '')}acr', 50), '_', '')
var cosmosName = take('${replace(normalizedName, '-', '')}cosmos', 44)
var searchName = take('${replace(normalizedName, '-', '')}search', 60)
var storageName = '${take(replace(normalizedName, '-', ''), 9)}st${uniqueString(resourceGroup().id)}'
var serviceBusName = take('${replace(normalizedName, '-', '')}sb', 50)
var acrPullRoleDefinitionId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
var cosmosDataContributorRoleDefinitionId = '${cosmosAccount.id}/sqlRoleDefinitions/00000000-0000-0000-0000-000000000002'
var searchServiceContributorRoleDefinitionId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7ca78c08-252a-4471-8644-bb5ff32d4ba0')
var searchIndexDataContributorRoleDefinitionId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '8ebe5a00-799e-43f5-93ac-243d3dce84a7')
var storageBlobDataContributorRoleDefinitionId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
var serviceBusDataSenderRoleDefinitionId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '69a216fc-b8fb-44d8-bc22-1f3c2cd27a39')
var serviceBusDataReceiverRoleDefinitionId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4f6d3b9b-027b-4f4c-9142-0e5a2a2247e0')

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: '${normalizedName}-law'
  location: location
  tags: tags
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
  }
}

resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: '${normalizedName}-appi'
  location: location
  tags: tags
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: logAnalytics.id
  }
}

resource containerRegistry 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: acrName
  location: location
  tags: tags
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: false
    publicNetworkAccess: 'Enabled'
  }
}

resource appIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${normalizedName}-id'
  location: location
  tags: tags
}

resource cosmosAccount 'Microsoft.DocumentDB/databaseAccounts@2024-05-15' = {
  name: cosmosName
  location: location
  tags: tags
  kind: 'GlobalDocumentDB'
  properties: {
    databaseAccountOfferType: 'Standard'
    consistencyPolicy: {
      defaultConsistencyLevel: 'Session'
    }
    locations: [
      {
        locationName: location
        failoverPriority: 0
        isZoneRedundant: false
      }
    ]
    publicNetworkAccess: cosmosPublicNetworkAccess
    disableLocalAuth: true
    capabilities: [
      {
        name: 'EnableServerless'
      }
    ]
  }
}

resource cosmosDatabase 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2024-05-15' = {
  name: 'memforest'
  parent: cosmosAccount
  properties: {
    resource: {
      id: 'memforest'
    }
  }
}

var userScopedCosmosContainers = [
  'sessions'
  'turns'
  'facts'
  'tree_nodes'
  'fact_locations'
]

var tenantScopedCosmosContainers = [
  'manifests'
  'deletions'
]

resource userScopedCosmosContainersResource 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2024-05-15' = [for containerName in userScopedCosmosContainers: {
  name: containerName
  parent: cosmosDatabase
  properties: {
    resource: {
      id: containerName
      partitionKey: {
        paths: [
          '/tenant_user_key'
        ]
        kind: 'Hash'
      }
    }
  }
}]

resource tenantScopedCosmosContainersResource 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2024-05-15' = [for containerName in tenantScopedCosmosContainers: {
  name: containerName
  parent: cosmosDatabase
  properties: {
    resource: {
      id: containerName
      partitionKey: {
        paths: [
          '/tenant_id'
        ]
        kind: 'Hash'
      }
    }
  }
}]

resource jobsCosmosContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2024-05-15' = {
  name: 'jobs'
  parent: cosmosDatabase
  properties: {
    resource: {
      id: 'jobs'
      defaultTtl: 2592000
      partitionKey: {
        paths: [
          '/tenant_id'
        ]
        kind: 'Hash'
      }
    }
  }
}

resource queryTracesCosmosContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2024-05-15' = {
  name: 'query_traces'
  parent: cosmosDatabase
  properties: {
    resource: {
      id: 'query_traces'
      defaultTtl: 604800
      partitionKey: {
        paths: [
          '/tenant_id'
        ]
        kind: 'Hash'
      }
      indexingPolicy: {
        indexingMode: 'consistent'
        automatic: true
        includedPaths: [
          {
            path: '/tenant_id/?'
          }
          {
            path: '/user_id/?'
          }
          {
            path: '/query_id/?'
          }
          {
            path: '/timestamp_unix/?'
          }
        ]
        excludedPaths: [
          {
            path: '/*'
          }
        ]
      }
    }
  }
}

resource cosmosDataContributorAssignment 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2024-05-15' = {
  name: guid(cosmosAccount.id, appIdentity.id, 'cosmos-data-contributor')
  parent: cosmosAccount
  properties: {
    roleDefinitionId: cosmosDataContributorRoleDefinitionId
    principalId: appIdentity.properties.principalId
    scope: cosmosAccount.id
  }
}

resource searchService 'Microsoft.Search/searchServices@2024-06-01-preview' = {
  name: searchName
  location: searchLocation
  tags: tags
  sku: {
    name: 'basic'
  }
  properties: {
    replicaCount: 1
    partitionCount: 1
    publicNetworkAccess: searchPublicNetworkAccess
    disableLocalAuth: true
  }
}

resource searchServiceContributorAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(searchService.id, appIdentity.id, 'search-service-contributor')
  scope: searchService
  properties: {
    roleDefinitionId: searchServiceContributorRoleDefinitionId
    principalId: appIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource searchIndexDataContributorAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(searchService.id, appIdentity.id, 'search-index-data-contributor')
  scope: searchService
  properties: {
    roleDefinitionId: searchIndexDataContributorRoleDefinitionId
    principalId: appIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageName
  location: location
  tags: tags
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    accessTier: 'Hot'
    allowBlobPublicAccess: false
    allowCrossTenantReplication: false
    allowSharedKeyAccess: false
    defaultToOAuthAuthentication: true
    minimumTlsVersion: 'TLS1_2'
    publicNetworkAccess: storagePublicNetworkAccess
    supportsHttpsTrafficOnly: true
    networkAcls: {
      bypass: 'AzureServices'
      defaultAction: 'Allow'
    }
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  name: 'default'
  parent: storageAccount
  properties: {
    deleteRetentionPolicy: {
      enabled: true
      days: 7
    }
    containerDeleteRetentionPolicy: {
      enabled: true
      days: 7
    }
  }
}

resource snapshotsContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  name: 'snapshots'
  parent: blobService
  properties: {
    publicAccess: 'None'
  }
}

resource storageBlobDataContributorAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, appIdentity.id, 'storage-blob-data-contributor')
  scope: storageAccount
  properties: {
    roleDefinitionId: storageBlobDataContributorRoleDefinitionId
    principalId: appIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource serviceBusNamespace 'Microsoft.ServiceBus/namespaces@2024-01-01' = {
  name: serviceBusName
  location: location
  tags: tags
  sku: {
    name: 'Standard'
    tier: 'Standard'
  }
  properties: {
    disableLocalAuth: true
    minimumTlsVersion: '1.2'
    publicNetworkAccess: serviceBusPublicNetworkAccess
  }
}

resource writesQueue 'Microsoft.ServiceBus/namespaces/queues@2024-01-01' = {
  name: serviceBusWritesQueueName
  parent: serviceBusNamespace
  properties: {
    requiresSession: true
    lockDuration: 'PT2M'
    maxDeliveryCount: 5
    defaultMessageTimeToLive: 'P14D'
    deadLetteringOnMessageExpiration: true
    enablePartitioning: false
  }
}

resource serviceBusSenderAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(serviceBusNamespace.id, appIdentity.id, 'servicebus-data-sender')
  scope: serviceBusNamespace
  properties: {
    roleDefinitionId: serviceBusDataSenderRoleDefinitionId
    principalId: appIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource serviceBusReceiverAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(serviceBusNamespace.id, appIdentity.id, 'servicebus-data-receiver')
  scope: serviceBusNamespace
  properties: {
    roleDefinitionId: serviceBusDataReceiverRoleDefinitionId
    principalId: appIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource acrPullAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(containerRegistry.id, appIdentity.id, 'acr-pull')
  scope: containerRegistry
  properties: {
    roleDefinitionId: acrPullRoleDefinitionId
    principalId: appIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource containerAppEnvironment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: '${normalizedName}-cae'
  location: location
  tags: tags
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalytics.properties.customerId
        sharedKey: logAnalytics.listKeys().primarySharedKey
      }
    }
  }
}

resource containerApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: '${normalizedName}-api'
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${appIdentity.id}': {}
    }
  }
  properties: {
    managedEnvironmentId: containerAppEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        external: true
        targetPort: containerPort
        transport: 'auto'
      }
      registries: [
        {
          server: containerRegistry.properties.loginServer
          identity: appIdentity.id
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'api'
          image: containerImage
          env: [
            {
              name: 'AZURE_MEMFOREST_ENV'
              value: environmentName
            }
            {
              name: 'APPLICATIONINSIGHTS_CONNECTION_STRING'
              value: appInsights.properties.ConnectionString
            }
            {
              name: 'AZURE_CLIENT_ID'
              value: appIdentity.properties.clientId
            }
            {
              name: 'AZURE_COSMOS_ENDPOINT'
              value: cosmosAccount.properties.documentEndpoint
            }
            {
              name: 'AZURE_COSMOS_DATABASE_NAME'
              value: cosmosDatabase.properties.resource.id
            }
            {
              name: 'AZURE_SEARCH_ENDPOINT'
              value: 'https://${searchService.name}.search.windows.net'
            }
            {
              name: 'AZURE_SEARCH_SERVICE_NAME'
              value: searchService.name
            }
            {
              name: 'AZURE_SEARCH_RECALL_INDEX_NAME'
              value: searchRecallIndexName
            }
            {
              name: 'AZURE_STORAGE_ACCOUNT_NAME'
              value: storageAccount.name
            }
            {
              name: 'AZURE_STORAGE_BLOB_ENDPOINT'
              value: storageAccount.properties.primaryEndpoints.blob
            }
            {
              name: 'AZURE_STORAGE_SNAPSHOT_CONTAINER'
              value: snapshotsContainer.name
            }
            {
              name: 'AZURE_SERVICEBUS_FULLY_QUALIFIED_NAMESPACE'
              value: '${serviceBusNamespace.name}.servicebus.windows.net'
            }
            {
              name: 'AZURE_SERVICEBUS_WRITES_QUEUE_NAME'
              value: writesQueue.name
            }
          ]
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
        }
      ]
      scale: {
        minReplicas: minReplicas
        maxReplicas: maxReplicas
      }
    }
  }
  dependsOn: [
    acrPullAssignment
    cosmosDataContributorAssignment
    searchServiceContributorAssignment
    searchIndexDataContributorAssignment
    storageBlobDataContributorAssignment
    serviceBusSenderAssignment
    serviceBusReceiverAssignment
  ]
}

output containerAppName string = containerApp.name
output containerAppFqdn string = containerApp.properties.configuration.ingress.fqdn
output containerRegistryLoginServer string = containerRegistry.properties.loginServer
output managedIdentityClientId string = appIdentity.properties.clientId
output applicationInsightsName string = appInsights.name
output cosmosAccountName string = cosmosAccount.name
output cosmosEndpoint string = cosmosAccount.properties.documentEndpoint
output cosmosDatabaseName string = cosmosDatabase.properties.resource.id
output searchServiceName string = searchService.name
output searchEndpoint string = 'https://${searchService.name}.search.windows.net'
output searchRecallIndexName string = searchRecallIndexName
output storageAccountName string = storageAccount.name
output storageBlobEndpoint string = storageAccount.properties.primaryEndpoints.blob
output storageSnapshotContainerName string = snapshotsContainer.name
output serviceBusNamespaceName string = serviceBusNamespace.name
output serviceBusFullyQualifiedNamespace string = '${serviceBusNamespace.name}.servicebus.windows.net'
output serviceBusWritesQueueName string = writesQueue.name

@secure()
output applicationInsightsConnectionString string = appInsights.properties.ConnectionString

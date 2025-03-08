# Azure Service Bus Deployment Commands

Below are the Azure CLI commands to create and configure an Azure Service Bus for message queuing.

## Create Resource Group

```bash
# Create a new resource group (if needed)
az group create --name poco-api-rg --location eastus
```

## Create Service Bus Namespace

```bash
# Create a Service Bus namespace
az servicebus namespace create \
  --resource-group poco-api-rg \
  --name poco-serv-bus \
  --location eastus \
  --sku Standard
```

## Create Queue

```bash
# Create a queue named "whatsapp-messages"
az servicebus queue create \
  --resource-group poco-api-rg \
  --namespace-name poco-serv-bus \
  --name whatsapp-messages \
  --max-size 1024 \
  --default-message-time-to-live P14D \
  --lock-duration PT2M \
  --enable-dead-lettering-on-message-expiration true
```

## Create Access Policy

```bash
# Create a single policy with both Send and Listen rights
az servicebus namespace authorization-rule create \
  --resource-group poco-api-rg \
  --namespace-name poco-serv-bus \
  --name ServiceBusPolicy \
  --rights Send Listen
```

## Get Connection String

```bash
# Get connection string for both producer and consumer
az servicebus namespace authorization-rule keys list \
  --resource-group poco-api-rg \
  --namespace-name poco-serv-bus \
  --name ServiceBusPolicy \
  --query primaryConnectionString \
  --output tsv
```

## Environment Variables Setup

Add the connection string to your `.env` file:

```
AZURE_SERVICEBUS_CONNECTION_STRING=your-connection-string
AZURE_SERVICEBUS_QUEUE_NAME=whatsapp-messages
```

## Using Azure Portal Instead

If you prefer using the Azure Portal instead of CLI:

1. Go to [Azure Portal](https://portal.azure.com)
2. Create a new resource → Integration → Service Bus
3. Create a namespace with Standard tier
4. Navigate to the namespace and create a queue named "whatsapp-messages"
5. Go to "Shared access policies" to create policies and get connection strings

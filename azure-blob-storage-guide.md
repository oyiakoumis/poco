# Guide to Setting Up Azure Blob Storage for WhatsApp Media

This guide provides step-by-step instructions for setting up Azure Blob Storage to handle media files (images) from WhatsApp messages in your FastAPI application.

## Prerequisites

- Azure account with an active subscription
- Azure CLI installed and logged in
- Access to your application's configuration

## Step 1: Create an Azure Storage Account

```bash
# Set variables
RESOURCE_GROUP="poco-api-rg" 
LOCATION="eastus"
STORAGE_ACCOUNT_NAME="pocomediastorage"
SKU="Standard_LRS"

# Create the storage account
az storage account create \
  --name $STORAGE_ACCOUNT_NAME \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku $SKU \
  --kind StorageV2 \
  --access-tier Hot \
  --https-only true
```

## Step 2: Create a Blob Container

```bash
# Set variables
CONTAINER_NAME="whatsapp-media"  

# Get the storage account key
STORAGE_ACCOUNT_KEY=$(az storage account keys list \
  --resource-group $RESOURCE_GROUP \
  --account-name $STORAGE_ACCOUNT_NAME \
  --query "[0].value" -o tsv)

# Create the container
az storage container create \
  --name $CONTAINER_NAME \
  --account-name $STORAGE_ACCOUNT_NAME \
  --account-key $STORAGE_ACCOUNT_KEY \
  --public-access off
```

## Step 3: Get the Connection String

```bash
# Get the connection string
CONNECTION_STRING=$(az storage account show-connection-string \
  --name $STORAGE_ACCOUNT_NAME \
  --resource-group $RESOURCE_GROUP \
  --query connectionString \
  --output tsv)

echo "Connection String: $CONNECTION_STRING"
echo "Storage Account Key: $STORAGE_ACCOUNT_KEY"
echo "Container Name: $CONTAINER_NAME"
```

## Step 4: Update Application Configuration

Add the following environment variables to your application:

```bash
# Set variables
RESOURCE_GROUP="poco-api-rg"
APP_NAME="poco-api"  # Your Container App name
WORKER_APP_NAME="poco-worker"  # Your Worker Container App name

# Update the API Container App
az containerapp update \
  --name $APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --set-env-vars "AZURE_STORAGE_CONNECTION_STRING=$CONNECTION_STRING" \
                 "AZURE_BLOB_CONTAINER_NAME=$CONTAINER_NAME" \
                 "AZURE_STORAGE_ACCOUNT_KEY=$STORAGE_ACCOUNT_KEY"

# Update the Worker Container App (if separate)
az containerapp update \
  --name $WORKER_APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --set-env-vars "AZURE_STORAGE_CONNECTION_STRING=$CONNECTION_STRING" \
                 "AZURE_BLOB_CONTAINER_NAME=$CONTAINER_NAME" \
                 "AZURE_STORAGE_ACCOUNT_KEY=$STORAGE_ACCOUNT_KEY"
```

If you're running locally, add these variables to your `.env` file:

```
AZURE_STORAGE_CONNECTION_STRING=your_connection_string_here
AZURE_BLOB_CONTAINER_NAME=whatsapp-media
AZURE_STORAGE_ACCOUNT_KEY=your_account_key_here
```

## Step 5: Verify the Configuration

To verify that your application can connect to Azure Blob Storage, you can run a simple test:

```bash
# Create a test file
echo "Test content" > test.txt

# Upload the test file to the container
az storage blob upload \
  --account-name $STORAGE_ACCOUNT_NAME \
  --account-key $STORAGE_ACCOUNT_KEY \
  --container-name $CONTAINER_NAME \
  --name test.txt \
  --file test.txt

# List blobs in the container
az storage blob list \
  --account-name $STORAGE_ACCOUNT_NAME \
  --account-key $STORAGE_ACCOUNT_KEY \
  --container-name $CONTAINER_NAME \
  --output table

# Clean up the test file
az storage blob delete \
  --account-name $STORAGE_ACCOUNT_NAME \
  --account-key $STORAGE_ACCOUNT_KEY \
  --container-name $CONTAINER_NAME \
  --name test.txt
```

# Guide to Update FastAPI Application on Azure Container Apps

This guide provides step-by-step instructions for updating your FastAPI application that's deployed on Azure Container Apps.

## Prerequisites

- Azure CLI installed and logged in
- Docker installed locally
- Access to your Azure resources (Container Registry and Container App)
- Access to your MongoDB Atlas database

## Step 1: Update Your Application Code

1. Make the necessary changes to your application code
2. Test your changes locally:

```bash
# Run the application locally
python -m src.run
```

## Step 2: Update Environment Variables (If Needed)

If you've added new environment variables to your application, you'll need to update them in Azure:

```bash
# Set variables
RESOURCE_GROUP="poco-api-rg"
APP_NAME="poco-api"

# Add or update environment variables
az containerapp update \
  --name $APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --set-env-vars "NEW_ENV_VAR=value" "UPDATED_ENV_VAR=new_value"
```

## Step 3: Rebuild and Push Docker Image

Since you're using a Mac with Apple Silicon, remember to build for the AMD64 architecture:

```bash
# Set variables
ACR_NAME="pocoacr"  # Your Azure Container Registry name
VERSION=$(date +%Y%m%d%H%M%S)  # Use timestamp as version

# Login to ACR
az acr login --name $ACR_NAME

# Build the Docker image for AMD64 architecture
docker buildx create --use
docker buildx build --platform linux/amd64 -t poco-api:$VERSION --load .

# Tag the image for ACR
docker tag poco-api:$VERSION $ACR_NAME.azurecr.io/poco-api:$VERSION
docker tag poco-api:$VERSION $ACR_NAME.azurecr.io/poco-api:latest

# Push the image to ACR
docker push $ACR_NAME.azurecr.io/poco-api:$VERSION
docker push $ACR_NAME.azurecr.io/poco-api:latest
```

Alternatively, use the multi-architecture approach:

```bash
# Create and use a buildx builder
docker buildx create --name mybuilder --use

# Build and push multi-architecture image directly to ACR
docker buildx build --platform linux/amd64,linux/arm64 \
  -t $ACR_NAME.azurecr.io/poco-api:$VERSION \
  -t $ACR_NAME.azurecr.io/poco-api:latest \
  --push .
```

## Step 4: Update the Container App

```bash
# Set variables
RESOURCE_GROUP="poco-api-rg"
APP_NAME="poco-api"
ACR_NAME="pocoacr"
VERSION=$(date +%Y%m%d%H%M%S)  # Same version used in Step 3

# Update the Container App with the new image
az containerapp update \
  --name $APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --image $ACR_NAME.azurecr.io/poco-api:$VERSION
```

## Step 5: Handle Database Migrations (If Needed)

If your update includes database schema changes, you'll need to run migrations. There are several approaches:

### Option 1: Run Migrations as Part of Container Startup

Update your Dockerfile to include a script that runs migrations on startup:

```dockerfile
# Add to your Dockerfile
COPY ./scripts/start.sh /app/start.sh
RUN chmod +x /app/start.sh
CMD ["/app/start.sh"]
```

Create a start.sh script:

```bash
#!/bin/bash
# Run migrations
python -m src.db.migrations

# Start the application
gunicorn -k uvicorn.workers.UvicornWorker -w 4 -b 0.0.0.0:8000 src.api.main:app
```

### Option 2: Run Migrations Manually

For more control, you can run migrations manually:

```bash
# Create a temporary container to run migrations
az containerapp exec \
  --name $APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --command "python -m src.db.migrations"
```

## Step 6: Verify the Update

```bash
# Get the Container App URL
CONTAINER_APP_URL=$(az containerapp show --name $APP_NAME --resource-group $RESOURCE_GROUP --query properties.configuration.ingress.fqdn -o tsv)
echo "Container App URL: https://$CONTAINER_APP_URL"

# Test the API
curl https://$CONTAINER_APP_URL

# Check the logs
az containerapp logs show --name $APP_NAME --resource-group $RESOURCE_GROUP
```

## Step 7: Rollback (If Needed)

If something goes wrong, you can roll back to the previous version:

```bash
# Get the previous revision
PREVIOUS_REVISION=$(az containerapp revision list \
  --name $APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --query "[1].name" -o tsv)

# Activate the previous revision
az containerapp revision activate \
  --name $APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --revision $PREVIOUS_REVISION
```

## Advanced Update Strategies

### Blue-Green Deployment

Azure Container Apps supports traffic splitting between revisions, enabling blue-green deployments:

```bash
# Create a new revision with 0% traffic
az containerapp update \
  --name $APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --image $ACR_NAME.azurecr.io/poco-api:$VERSION \
  --revision-suffix "v$VERSION"

# Get the new revision name
NEW_REVISION=$(az containerapp revision list \
  --name $APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --query "[0].name" -o tsv)

# Get the active revision name
ACTIVE_REVISION=$(az containerapp revision list \
  --name $APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --query "[1].name" -o tsv)

# Split traffic: 10% to new, 90% to current
az containerapp traffic split set \
  --name $APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --revision-weight $NEW_REVISION=10 $ACTIVE_REVISION=90

# After testing, shift all traffic to new revision
az containerapp traffic split set \
  --name $APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --revision-weight $NEW_REVISION=100
```

### Continuous Deployment with GitHub Actions

For automated updates, set up a GitHub Actions workflow:

```yaml
name: Update Azure Container App

on:
  push:
    branches: [ main ]
  workflow_dispatch:

jobs:
  build-and-deploy:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Log in to Azure
      uses: azure/login@v1
      with:
        creds: ${{ secrets.AZURE_CREDENTIALS }}
    
    - name: Set up Docker Buildx
      uses: docker/setup-buildx-action@v1
    
    - name: Log in to ACR
      uses: azure/docker-login@v1
      with:
        login-server: ${{ secrets.ACR_NAME }}.azurecr.io
        username: ${{ secrets.ACR_USERNAME }}
        password: ${{ secrets.ACR_PASSWORD }}
    
    - name: Build and push image to ACR
      uses: docker/build-push-action@v2
      with:
        context: .
        push: true
        platforms: linux/amd64
        tags: |
          ${{ secrets.ACR_NAME }}.azurecr.io/poco-api:${{ github.sha }}
          ${{ secrets.ACR_NAME }}.azurecr.io/poco-api:latest
    
    - name: Update Container App
      run: |
        az containerapp update \
          --name poco-api \
          --resource-group poco-api-rg \
          --image ${{ secrets.ACR_NAME }}.azurecr.io/poco-api:${{ github.sha }}
```

## Monitoring Updates

Monitor your application after updates to ensure everything is working correctly:

```bash
# View Container App logs
az containerapp logs show --name $APP_NAME --resource-group $RESOURCE_GROUP

# View Container App metrics in the Azure Portal
# Navigate to your Container App > Monitoring > Metrics
```

## Troubleshooting Updates

If you encounter issues after an update:

1. Check the application logs:
   ```bash
   az containerapp logs show --name $APP_NAME --resource-group $RESOURCE_GROUP
   ```

2. Verify the container is running:
   ```bash
   az containerapp show --name $APP_NAME --resource-group $RESOURCE_GROUP --query "properties.latestRevisionName" -o tsv
   ```

3. Check if the environment variables are set correctly:
   ```bash
   az containerapp show --name $APP_NAME --resource-group $RESOURCE_GROUP --query "properties.configuration.secrets" -o table
   ```

4. If all else fails, roll back to the previous version as described in Step 7.

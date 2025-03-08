# Guide to Setting Up Redis for Distributed Locking on Azure

This guide provides step-by-step instructions for setting up Redis on Azure to implement distributed locking using the redlock-py library.

## What is Redis Distributed Locking?

Redis distributed locking (implemented using the Redlock algorithm) is a technique that allows distributed systems to coordinate access to shared resources. It's particularly useful in scenarios where:

- Multiple instances of your application are running concurrently
- You need to prevent race conditions when accessing shared resources
- You want to ensure that certain operations are performed exactly once
- You need to implement mutual exclusion across distributed systems

The Redlock algorithm, as implemented by the redlock-py library, provides a more robust distributed locking mechanism than simple Redis locks by using multiple Redis instances to achieve consensus.

## Prerequisites

- Azure CLI installed and logged in
- Access to your Azure resources
- Basic understanding of Redis and distributed locking concepts

## Step 1: Set Up Azure Cache for Redis

Azure Cache for Redis is a fully managed Redis implementation that provides high-performance, secure, and scalable caching.

### Create a Basic Redis Cache

```bash
# Set variables
RESOURCE_GROUP="poco-api-rg"
REDIS_NAME="poco-redis"
LOCATION="eastus" 
SKU="Standard"
REDIS_VERSION="6"

# Create Azure Cache for Redis
az redis create \
  --name $REDIS_NAME \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku $SKU \
  --vm-size C1 \
  --redis-version $REDIS_VERSION
```

For Redlock to be effective, it's recommended to have at least 3 independent Redis instances. You can create multiple Redis caches in different regions:

```bash
# Create additional Redis instances
az redis create \
  --name "${REDIS_NAME}-2" \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku $SKU \
  --vm-size C1 \
  --redis-version $REDIS_VERSION

az redis create \
  --name "${REDIS_NAME}-3" \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku $SKU \
  --vm-size C1 \
  --redis-version $REDIS_VERSION
```

### Security Considerations

By default, Azure Cache for Redis is created with SSL enabled and non-SSL port disabled. For production environments, consider these additional security measures:

```bash
# Enable VNet support (Premium tier only)
az redis update \
  --name $REDIS_NAME \
  --resource-group $RESOURCE_GROUP \
  --subnet-id "/subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/Microsoft.Network/virtualNetworks/{vnet-name}/subnets/{subnet-name}"

# Set firewall rules if not using VNet
az redis firewall-rules create \
  --name $REDIS_NAME \
  --resource-group $RESOURCE_GROUP \
  --rule-name "AllowAzureServices" \
  --start-ip "0.0.0.0" \
  --end-ip "0.0.0.0"
```

### Get Redis Connection Information

Retrieve connection information for all Redis instances:

```bash
# Function to get Redis connection info
get_redis_info() {
  local name=$1
  
  # Get Redis hostname
  local host=$(az redis show \
    --name $name \
    --resource-group $RESOURCE_GROUP \
    --query "hostName" -o tsv)

  # Get Redis access key
  local key=$(az redis list-keys \
    --name $name \
    --resource-group $RESOURCE_GROUP \
    --query "primaryKey" -o tsv)

  # Get Redis SSL port
  local port=$(az redis show \
    --name $name \
    --resource-group $RESOURCE_GROUP \
    --query "sslPort" -o tsv)

  echo "Redis Instance: $name"
  echo "  Host: $host"
  echo "  Port: $port"
  echo "  Key: $key"
  echo "  Connection String: rediss://$host:$port"
}

# Get info for all Redis instances
get_redis_info $REDIS_NAME
get_redis_info "${REDIS_NAME}-2"
get_redis_info "${REDIS_NAME}-3"
```

## Step 2: Configure Redis for Optimal Locking Performance

### Adjust Redis Settings

For optimal performance with distributed locking, you may want to adjust some Redis settings:

```bash
# Update Redis configuration
az redis update \
  --name $REDIS_NAME \
  --resource-group $RESOURCE_GROUP \
  --set redisConfiguration="{'maxmemory-policy':'allkeys-lru'}"
```

### Configure Redis Persistence

For critical locking scenarios, you might want to enable persistence to prevent lock data loss during Redis restarts:

```bash
# Enable AOF persistence (Premium tier only)
az redis update \
  --name $REDIS_NAME \
  --resource-group $RESOURCE_GROUP \
  --set redisConfiguration="{'aof-backup-enabled':'true','aof-storage-connection-string-0':'DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net'}"
```

## Step 3: Set Up Azure Container Apps to Use Redis

Update your Container App environment variables to include Redis connection information:

```bash
# Set variables
RESOURCE_GROUP="poco-api-rg"
APP_NAME="poco-api"
WORKER_APP_NAME="poco-worker"

# Get Redis connection information for all instances
REDIS1_HOST=$(az redis show --name $REDIS_NAME --resource-group $RESOURCE_GROUP --query "hostName" -o tsv)
REDIS1_KEY=$(az redis list-keys --name $REDIS_NAME --resource-group $RESOURCE_GROUP --query "primaryKey" -o tsv)
REDIS1_PORT=$(az redis show --name $REDIS_NAME --resource-group $RESOURCE_GROUP --query "sslPort" -o tsv)

REDIS2_HOST=$(az redis show --name "${REDIS_NAME}-2" --resource-group $RESOURCE_GROUP --query "hostName" -o tsv)
REDIS2_KEY=$(az redis list-keys --name "${REDIS_NAME}-2" --resource-group $RESOURCE_GROUP --query "primaryKey" -o tsv)
REDIS2_PORT=$(az redis show --name "${REDIS_NAME}-2" --resource-group $RESOURCE_GROUP --query "sslPort" -o tsv)

REDIS3_HOST=$(az redis show --name "${REDIS_NAME}-3" --resource-group $RESOURCE_GROUP --query "hostName" -o tsv)
REDIS3_KEY=$(az redis list-keys --name "${REDIS_NAME}-3" --resource-group $RESOURCE_GROUP --query "primaryKey" -o tsv)
REDIS3_PORT=$(az redis show --name "${REDIS_NAME}-3" --resource-group $RESOURCE_GROUP --query "sslPort" -o tsv)

# Create JSON strings for Redis instances
REDIS_INSTANCES="[{\"host\":\"$REDIS1_HOST\",\"port\":$REDIS1_PORT,\"password\":\"$REDIS1_KEY\",\"ssl\":true},{\"host\":\"$REDIS2_HOST\",\"port\":$REDIS2_PORT,\"password\":\"$REDIS2_KEY\",\"ssl\":true},{\"host\":\"$REDIS3_HOST\",\"port\":$REDIS3_PORT,\"password\":\"$REDIS3_KEY\",\"ssl\":true}]"

# Update API Container App
az containerapp update \
  --name $APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --set-env-vars "REDIS_INSTANCES=$REDIS_INSTANCES"

# Update Worker Container App
az containerapp update \
  --name $WORKER_APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --set-env-vars "REDIS_INSTANCES=$REDIS_INSTANCES"
```

## Step 4: Set Up Monitoring for Redis

### Enable Diagnostic Settings

```bash
# Enable diagnostic settings for all Redis instances
for instance in $REDIS_NAME "${REDIS_NAME}-2" "${REDIS_NAME}-3"; do
  az monitor diagnostic-settings create \
    --name "redis-diagnostics" \
    --resource-id $(az redis show --name $instance --resource-group $RESOURCE_GROUP --query id -o tsv) \
    --logs '[{"category": "ConnectedClientList", "enabled": true}, {"category": "Redis", "enabled": true}]' \
    --metrics '[{"category": "AllMetrics", "enabled": true}]' \
    --workspace $(az monitor log-analytics workspace show --resource-group $RESOURCE_GROUP --workspace-name "poco-logs" --query id -o tsv)
done
```

### Create Azure Monitor Alerts

```bash
# Create alerts for Redis metrics
for instance in $REDIS_NAME "${REDIS_NAME}-2" "${REDIS_NAME}-3"; do
  # Alert for high memory usage
  az monitor metrics alert create \
    --name "${instance}-HighMemory" \
    --resource-group $RESOURCE_GROUP \
    --scopes $(az redis show --name $instance --resource-group $RESOURCE_GROUP --query id -o tsv) \
    --condition "avg UsedMemoryPercentage > 80" \
    --window-size 5m \
    --evaluation-frequency 1m \
    --action $(az monitor action-group show --name "DevOpsTeam" --resource-group $RESOURCE_GROUP --query id -o tsv)
  
  # Alert for high CPU usage
  az monitor metrics alert create \
    --name "${instance}-HighCPU" \
    --resource-group $RESOURCE_GROUP \
    --scopes $(az redis show --name $instance --resource-group $RESOURCE_GROUP --query id -o tsv) \
    --condition "avg ServerLoad > 80" \
    --window-size 5m \
    --evaluation-frequency 1m \
    --action $(az monitor action-group show --name "DevOpsTeam" --resource-group $RESOURCE_GROUP --query id -o tsv)
  
  # Alert for high connection count
  az monitor metrics alert create \
    --name "${instance}-HighConnections" \
    --resource-group $RESOURCE_GROUP \
    --scopes $(az redis show --name $instance --resource-group $RESOURCE_GROUP --query id -o tsv) \
    --condition "avg connectedclients > 1000" \
    --window-size 5m \
    --evaluation-frequency 1m \
    --action $(az monitor action-group show --name "DevOpsTeam" --resource-group $RESOURCE_GROUP --query id -o tsv)
done
```

## Step 5: Create a Redis Dashboard

```bash
# Create a Redis monitoring dashboard
az portal dashboard create \
  --name "Redis-Monitoring" \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --input-path redis-dashboard-template.json
```

Example dashboard template (redis-dashboard-template.json):
```json
{
  "properties": {
    "lenses": {
      "0": {
        "order": 0,
        "parts": {
          "0": {
            "position": {
              "x": 0,
              "y": 0,
              "colSpan": 6,
              "rowSpan": 4
            },
            "metadata": {
              "inputs": [
                {
                  "name": "resourceId",
                  "value": "/subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/Microsoft.Cache/Redis/{redis-name}"
                }
              ],
              "type": "Extension/Microsoft_Azure_Cache/PartType/RedisMonitoringPart"
            }
          },
          "1": {
            "position": {
              "x": 6,
              "y": 0,
              "colSpan": 6,
              "rowSpan": 4
            },
            "metadata": {
              "inputs": [
                {
                  "name": "resourceId",
                  "value": "/subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/Microsoft.Cache/Redis/{redis-name}-2"
                }
              ],
              "type": "Extension/Microsoft_Azure_Cache/PartType/RedisMonitoringPart"
            }
          },
          "2": {
            "position": {
              "x": 0,
              "y": 4,
              "colSpan": 6,
              "rowSpan": 4
            },
            "metadata": {
              "inputs": [
                {
                  "name": "resourceId",
                  "value": "/subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/Microsoft.Cache/Redis/{redis-name}-3"
                }
              ],
              "type": "Extension/Microsoft_Azure_Cache/PartType/RedisMonitoringPart"
            }
          }
        }
      }
    }
  }
}
```

## Step 6: Scaling Considerations

### Scaling Redis Cache

As your application grows, you may need to scale your Redis instances:

```bash
# Scale up Redis cache
az redis update \
  --name $REDIS_NAME \
  --resource-group $RESOURCE_GROUP \
  --sku Standard \
  --vm-size C2
```

### Redis Clustering (Premium Tier)

For very high throughput scenarios, consider using Redis clustering:

```bash
# Create a Premium tier Redis with clustering
az redis create \
  --name "poco-redis-premium" \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Premium \
  --vm-size P1 \
  --shard-count 3 \
  --enable-non-ssl-port false
```

## Step 7: Disaster Recovery Planning

### Geo-Replication (Premium Tier)

For critical applications, set up geo-replication:

```bash
# Create a geo-replicated Redis cache (Premium tier only)
az redis create \
  --name "poco-redis-primary" \
  --resource-group $RESOURCE_GROUP \
  --location "eastus" \
  --sku Premium \
  --vm-size P1 \
  --enable-non-ssl-port false

# Create a linked cache
az redis linked-server create \
  --name "poco-redis-primary" \
  --resource-group $RESOURCE_GROUP \
  --linked-server-id $(az redis show --name "poco-redis-secondary" --resource-group $RESOURCE_GROUP --query id -o tsv) \
  --server-role "Secondary" \
  --geo-replication-location "westus"
```

### Backup and Restore (Premium Tier)

Set up regular backups for your Redis cache:

```bash
# Configure backup for Redis (Premium tier only)
az redis update \
  --name "poco-redis-primary" \
  --resource-group $RESOURCE_GROUP \
  --set redisConfiguration="{'rdb-backup-enabled':'true','rdb-backup-frequency':'60','rdb-backup-max-snapshot-count':'1','rdb-storage-connection-string':'DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net'}"
```

## Best Practices for Redis Infrastructure on Azure

1. **Use Multiple Redis Instances**: For true Redlock implementation, use at least 3 Redis instances, preferably in different regions or availability zones.

2. **Enable SSL**: Always use SSL for Redis connections to ensure data security.

3. **Implement Proper Monitoring**: Set up alerts for key Redis metrics like memory usage, CPU usage, and connection count.

4. **Consider Premium Tier for Production**: The Premium tier offers features like clustering, geo-replication, and persistence that are valuable for production environments.

5. **Network Security**: Use VNet injection (Premium tier) or firewall rules to restrict access to your Redis instances.

6. **Regular Backups**: For critical applications, enable regular backups of your Redis data.

7. **Load Testing**: Before deploying to production, perform load testing to ensure your Redis infrastructure can handle the expected load.

## Troubleshooting Redis on Azure

### Common Issues and Solutions

1. **Connection Issues**:
   - Check network security groups and firewall rules
   - Verify that you're using the correct port (6380 for SSL)
   - Ensure you're using the correct access key

2. **Performance Issues**:
   - Monitor Redis metrics in Azure Monitor
   - Consider scaling up or enabling clustering
   - Check for slow commands using Redis SLOWLOG

3. **High Memory Usage**:
   - Review your key expiration policies
   - Consider implementing key eviction policies
   - Scale up to a larger instance size

### Useful Redis Commands for Troubleshooting

You can connect to your Redis instance using redis-cli:

```bash
# Install redis-cli if needed
apt-get update && apt-get install -y redis-tools

# Connect to Redis using redis-cli
redis-cli -h $REDIS_HOST -p $REDIS_PORT -a $REDIS_KEY --tls

# Useful commands
INFO               # Get Redis server information
INFO memory        # Get memory usage information
CLIENT LIST        # List connected clients
SLOWLOG GET 10     # Get the 10 slowest commands
MONITOR            # Monitor Redis commands in real-time (use with caution in production)
```

## Conclusion

Setting up Redis on Azure for distributed locking provides a robust foundation for implementing the Redlock algorithm using the redlock-py library. By following this guide, you've created the necessary infrastructure to support distributed locking in your application.

Remember to monitor your Redis instances and adjust your configuration as your application's needs evolve. With proper setup and monitoring, Redis on Azure can provide reliable distributed locking for your application.

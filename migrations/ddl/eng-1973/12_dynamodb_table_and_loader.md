# ENG-1973: DynamoDB Table and Complete Data Pipeline

## Table Structure

**Table Name**: vector_emails_operational
**Primary Key**: vup_id (String) partition, hem (String) sort
**GSI**: hem-index with hem (String) partition, vup_id (String) sort, ALL projection
**Billing**: PAY_PER_REQUEST (on-demand)
**Encryption**: AWS-managed KMS (SSE enabled)
**Streams**: Enabled with NEW_AND_OLD_IMAGES
**Point-in-Time Recovery**: Enabled

## Unresolved HEM Handling - Sharded Sentinel Pattern

HEMs without VUP associations receive sharded sentinel values following pattern UNRESOLVED#{first_two_hex_chars} to prevent hot partition issues. This distributes unresolved records across 256 distinct partitions.

Examples:
- HEM a1b2c3... → vup_id UNRESOLVED#a1
- HEM 5f3e2d... → vup_id UNRESOLVED#5f

## Table Creation with GSI Activation Wait
```bash
# Create table
aws dynamodb create-table \
  --table-name vector_emails_operational \
  --attribute-definitions \
      AttributeName=vup_id,AttributeType=S \
      AttributeName=hem,AttributeType=S \
  --key-schema \
      AttributeName=vup_id,KeyType=HASH \
      AttributeName=hem,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES \
  --tags Key=Project,Value=VectorEmailsV2 Key=Ticket,Value=ENG-1973 \
  --region us-east-1

# Wait for table creation
aws dynamodb wait table-exists \
  --table-name vector_emails_operational \
  --region us-east-1

# Add GSI
aws dynamodb update-table \
  --table-name vector_emails_operational \
  --attribute-definitions \
      AttributeName=hem,AttributeType=S \
      AttributeName=vup_id,AttributeType=S \
  --global-secondary-index-updates '[{
    "Create": {
      "IndexName": "hem-index",
      "KeySchema": [
        {"AttributeName": "hem", "KeyType": "HASH"},
        {"AttributeName": "vup_id", "KeyType": "RANGE"}
      ],
      "Projection": {"ProjectionType": "ALL"}
    }
  }]' \
  --region us-east-1

# CRITICAL: Wait for GSI activation (standard wait does NOT check GSI status)
echo "Waiting for GSI activation..."
while true; do
  GSI_STATUS=$(aws dynamodb describe-table \
    --table-name vector_emails_operational \
    --region us-east-1 \
    --query 'Table.GlobalSecondaryIndexes[0].IndexStatus' \
    --output text)
  
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] GSI status: $GSI_STATUS"
  
  if [ "$GSI_STATUS" = "ACTIVE" ]; then
    echo "GSI activated - ready for data load"
    break
  elif [ "$GSI_STATUS" = "CREATING" ]; then
    sleep 30
  else
    echo "ERROR: Unexpected GSI status"
    exit 1
  fi
done

# Enable point-in-time recovery
aws dynamodb update-continuous-backups \
  --table-name vector_emails_operational \
  --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true \
  --region us-east-1
```

## Data Export with Timezone Conversion
```sql
UNLOAD (
  'SELECT 
     hem, 
     COALESCE(vup_id, '''') AS vup_id,
     COALESCE(email, '''') AS email,
     COALESCE(domain, '''') AS domain,
     COALESCE(TO_CHAR(CONVERT_TIMEZONE(''UTC'',''UTC'', last_verified), ''YYYY-MM-DD"T"HH24:MI:SS"Z"''), '''') AS last_verified,
     COALESCE(source, '''') AS source,
     TO_CHAR(CONVERT_TIMEZONE(''UTC'',''UTC'', updated_at), ''YYYY-MM-DD"T"HH24:MI:SS"Z"'') AS updated_at
   FROM derived.v_email_hem_best
   ORDER BY hem'
)
TO 's3://your-export-bucket/vector-emails/v2/hem_export_'
IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/RedshiftUnloadRole'
FORMAT AS CSV
HEADER
PARALLEL OFF
ALLOWOVERWRITE
GZIP;
```

The CONVERT_TIMEZONE function explicitly converts timestamps to UTC before formatting with Z suffix, preventing timezone corruption where exported timestamps claim UTC but contain cluster local time.

## Streaming Lambda Function
```python
import csv
import io
import gzip
import boto3
from datetime import datetime

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('vector_emails_operational')
cloudwatch = boto3.client('cloudwatch', region_name='us-east-1')
s3_client = boto3.client('s3')

# SECURITY: Never log cleartext emails or PII

def resolve_vup_id(raw_vup, hem):
    """Convert null vup_id to sharded sentinel."""
    if raw_vup and raw_vup.strip():
        return raw_vup.strip()
    shard = hem[:2] if len(hem) >= 2 else '00'
    return f"UNRESOLVED#{shard}"

def lambda_handler(event, context):
    """Stream CSV from S3 to DynamoDB using row-by-row processing."""
    
    record = event['Records'][0]['s3']
    bucket = record['bucket']['name']
    key = record['object']['key']
    print(f"Processing: s3://{bucket}/{key}")
    
    # Stream file with decompression
    response = s3_client.get_object(Bucket=bucket, Key=key)
    stream = response['Body']
    
    if key.endswith('.gz'):
        file_handle = gzip.GzipFile(fileobj=stream)
    else:
        file_handle = stream
    
    text_stream = io.TextIOWrapper(file_handle, encoding='utf-8')
    reader = csv.DictReader(text_stream)
    
    items_processed = 0
    items_failed = 0
    
    try:
        with table.batch_writer(overwrite_by_pkeys=['vup_id', 'hem']) as batch:
            for row_num, row in enumerate(reader, start=2):
                try:
                    if not row.get('hem') or not row.get('hem').strip():
                        items_failed += 1
                        continue
                    
                    hem = row['hem'].strip()
                    vup_id = resolve_vup_id(row.get('vup_id'), hem)
                    
                    item = {
                        'vup_id': vup_id,
                        'hem': hem,
                        'updated_at': row.get('updated_at', datetime.utcnow().isoformat() + 'Z')
                    }
                    
                    if row.get('email') and row['email'].strip():
                        item['email'] = row['email'].strip()
                    
                    if row.get('domain') and row['domain'].strip():
                        item['domain'] = row['domain'].strip()
                    
                    if row.get('last_verified') and row['last_verified'].strip():
                        item['last_verified'] = row['last_verified'].strip()
                    
                    if row.get('source') and row['source'].strip():
                        item['source'] = row['source'].strip()
                    
                    batch.put_item(Item=item)
                    items_processed += 1
                    
                    if items_processed % 10000 == 0:
                        print(f"Progress: {items_processed} items")
                    
                except Exception as e:
                    print(f"Row {row_num} error: {e}")
                    items_failed += 1
                    
    finally:
        text_stream.close()
        file_handle.close()
    
    print(f"Complete: {items_processed} processed, {items_failed} failed")
    
    # Publish CloudWatch metrics
    cloudwatch.put_metric_data(
        Namespace='ENG-1973/EmailIngestion',
        MetricData=[
            {
                'MetricName': 'ItemsProcessed',
                'Value': items_processed,
                'Unit': 'Count',
                'Dimensions': [{'Name': 'ExportFile', 'Value': key}]
            },
            {
                'MetricName': 'ItemsFailed',
                'Value': items_failed,
                'Unit': 'Count',
                'Dimensions': [{'Name': 'ExportFile', 'Value': key}]
            }
        ]
    )
    
    return {
        'statusCode': 200 if items_failed == 0 else 207,
        'body': {
            'items_processed': items_processed,
            'items_failed': items_failed
        }
    }
```

**Lambda Configuration**:
- Runtime: Python 3.11+
- Memory: 1024 MB
- Timeout: 300 seconds
- Reserved Concurrency: 1

**Required IAM Permissions**:
- s3:GetObject
- dynamodb:BatchWriteItem
- cloudwatch:PutMetricData
- logs:CreateLogGroup, logs:CreateLogStream, logs:PutLogEvents

## Access Pattern Validation

### VUP to All HEMs (Primary Key)
```bash
aws dynamodb query \
  --table-name vector_emails_operational \
  --key-condition-expression "vup_id = :v" \
  --expression-attribute-values '{":v": {"S": "abc123"}}' \
  --consistent-read \
  --region us-east-1
```

### HEM to VUP (GSI)
```bash
aws dynamodb query \
  --table-name vector_emails_operational \
  --index-name hem-index \
  --key-condition-expression "hem = :h" \
  --expression-attribute-values '{":h": {"S": "a1b2c3..."}}' \
  --region us-east-1
```

## Rollback Procedures

**Point-in-Time Recovery**:
```bash
aws dynamodb restore-table-to-point-in-time \
  --source-table-name vector_emails_operational \
  --target-table-name vector_emails_operational_restored \
  --restore-date-time 2025-01-15T14:30:00Z \
  --region us-east-1
```

**Re-export from Corrected Source**:
Preferred if issue stems from view logic or source data problems. Correct the view, delete DynamoDB items, re-run UNLOAD.

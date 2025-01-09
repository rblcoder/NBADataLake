import boto3
import json
import urllib.request
import os


# AWS configurations
region = "us-east-1"  # Replace with your preferred AWS region
bucket_name = "sports-analytics-data-lake"  # Change to a unique S3 bucket name
glue_database_name = "glue_nba_data_lake"

# Sportsdata.io configurations (loaded from .env)
api_key = os.getenv("NBA_API_KEY")  # Get API key from .env
nba_endpoint = os.getenv("NBA_ENDPOINT")  # Get NBA endpoint from .env
api_url = f"{nba_endpoint}?key={api_key}"

# Create AWS clients
s3_client = boto3.client("s3", region_name=region)

def lambda_handler(event, context):
    """AWS Lambda function to fetch and upload NBA data to S3."""
    nba_data = fetch_nba_data()
    if nba_data:  # Only proceed if data was fetched successfully
        upload_data_to_s3(nba_data)
    return {
        'statusCode': 200,
        'body': json.dumps('Data ingestion completed successfully.')
    }

def fetch_nba_data():
    """Fetch NBA player data from sportsdata.io."""
    try:
        with urllib.request.urlopen(api_url) as response:
            data = json.loads(response.read().decode())
        print("Fetched NBA data successfully.")
        return data  # Return JSON response
    except Exception as e:
        print(f"Error fetching NBA data: {e}")
        return []

def convert_to_line_delimited_json(data):
    """Convert data to line-delimited JSON format."""
    print("Converting data to line-delimited JSON format...")
    return "\n".join([json.dumps(record) for record in data])

def upload_data_to_s3(data):
    """Upload NBA data to the S3 bucket."""
    try:
        # Convert data to line-delimited JSON
        line_delimited_data = convert_to_line_delimited_json(data)

        # Define S3 object key
        file_key = "raw-data/nba_player_data.jsonl"

        # Upload JSON data to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=line_delimited_data
        )
        print(f"Uploaded data to S3: {file_key}")
    except Exception as e:
        print(f"Error uploading data to S3: {e}")



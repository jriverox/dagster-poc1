import os
from pymongo import MongoClient
import pandas as pd
from io import BytesIO
import boto3
from dagster import AssetExecutionContext, MetadataValue, asset, MaterializeResult
from datetime import datetime

MONGO_URI = os.getenv("MONGO_URI")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")

@asset
def fetch_data_from_mongodb(context: AssetExecutionContext):
    try:
        context.log.info(f"MONGO_URI {MONGO_URI}")
        client = MongoClient(MONGO_URI, tls=True, tlsAllowInvalidCertificates=True)
        db = client.get_database("demo")
        collection = db.products
        data = list(collection.find({"department": "Books"}))
        context.log.info(f"Fetched {len(data)} records from MongoDB")
        return data
    except Exception as e:
        context.log.error(f"Error fetching data from MongoDB: {str(e)}")
        raise

@asset
def create_excel_file(context: AssetExecutionContext, fetch_data_from_mongodb):
    data = fetch_data_from_mongodb

    if not data:
        raise ValueError("No data fetched from MongoDB")
    df = pd.DataFrame(data)
    excel_buffer = BytesIO()
    df.to_excel(excel_buffer, index=False)

    context.log.info("Excel file created successfully")
    
    return excel_buffer

@asset
def upload_to_s3(context: AssetExecutionContext, create_excel_file):
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        excel_buffer = create_excel_file
        excel_buffer.seek(0)
        file_name = 'data.xlsx'
        s3.upload_fileobj(excel_buffer, S3_BUCKET_NAME, file_name)
        #file_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{file_name}"
        file_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{file_name}"

        context.log.info(f"File uploaded to S3 successfully, {file_url}")
        
        return file_url
    except Exception as e:
        context.log.error(f"Error uploading file to S3: {str(e)}")
        raise

@asset
def publish_sns_message(context: AssetExecutionContext, upload_to_s3) -> MaterializeResult:
    try:
        s3_url = upload_to_s3
        context.log.info(f"Publishing SNS message with S3 URL: {s3_url}")
        sns_client = boto3.client(
            'sns',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        message = f"Reporte generado, haga clic en el siguiente link para descargarlo: {s3_url}"
        sns_client.publish(TopicArn=SNS_TOPIC_ARN, Message=message)
        context.log.info("SNS message published successfully")
    except Exception as e:
        context.log.error(f"Error publishing SNS message: {str(e)}")
        raise



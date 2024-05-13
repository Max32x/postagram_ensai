import boto3
import os
import logging
import json
from datetime import datetime
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

reckognition = boto3.client('rekognition')
s3_client = boto3.client('s3', config=boto3.session.Config(signature_version='s3v4'))

def lambda_handler(event, context):

    logger.info(json.dumps(event, indent=2))

    # Récupération du nom du bucket
    bucket = event["Records"][0]["s3"]["bucket"]["name"]

    # Récupération du nom de l'objet
    key = unquote_plus(event["Records"][0]["s3"]["object"]["key"])

    # extration de l'utilisateur et de l'id de la tâche
    user, post_id = key.split('/')[:2]


    label_data = reckognition.detect_labels(
        Image={
        "S3Object": {
            "Bucket": bucket,
            "Name": key
            }
        },
    MaxLabels=5,
    MinConfidence=0.75
    )
    logger.info(f"Labels data : {label_data}")

    # On extrait les labels du résultat
    labels = [label["Name"] for label in label_data["Labels"]]
    logger.info(f"Labels detected : {labels}")


    url = s3_client.generate_presigned_url(
        Params={
            "Bucket": bucket,
            "Key": key,
        },
    ClientMethod='get_object'
    )

    data = table.update_item(
        
        Key={'user': "USER#"+user,  
            'id': "POST#"+post_id},
        
        UpdateExpression="set #img = :img, #lbl = :lbl",

        ExpressionAttributeNames={
            "#img": "image",
            "#lbl": "labels"
        },
        ExpressionAttributeValues={
            ":img": url,
            ":lbl": labels
        },
        ReturnValues="UPDATED_NEW"
    )
    
    return data
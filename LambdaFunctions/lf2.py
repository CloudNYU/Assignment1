import json
import boto3
import random
import requests
from botocore.exceptions import BotoCoreError, NoCredentialsError
from requests_aws4auth import AWS4Auth

# Configuración de AWS
REGION = "us-east-1"

# SQS
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/904233095319/dining-concierge-queue-jclsn"
sqs = boto3.client('sqs', region_name=REGION)

# OpenSearch
OPENSEARCH_URL = "https://search-restaurants-opensearch-yl6nmx64chp4wr5mzklj2jhdge.aos.us-east-1.on.aws"
INDEX_NAME = "restaurants"
credentials = boto3.Session().get_credentials()
AUTH = AWS4Auth(credentials.access_key, credentials.secret_key, REGION, "es", session_token=credentials.token)

# DynamoDB
dynamodb = boto3.resource('dynamodb', region_name=REGION)
table = dynamodb.Table("yelp-restaurants")

# SES (Simple Email Service)
ses = boto3.client('ses', region_name=REGION)
SENDER_EMAIL = "sn4236@nyu.edu" 


def get_message_from_sqs():
    response = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10
    )
    
    messages = response.get("Messages", [])
    if not messages:
        print("No hay mensajes en la kokola")
        return None,None

    message = messages[0]
    receipt_handle = message["ReceiptHandle"]
    body = json.loads(message["Body"])
    
    return body, receipt_handle


def get_random_restaurant(cuisine):
    query = {
        "query": {
            "match": {
                "cuisine": cuisine
            }
        }
    }
    
    response = requests.post(
        f"{OPENSEARCH_URL}/{INDEX_NAME}/_search",
        auth=AUTH,
        headers = {"Content-Type": "application/json"},
        json=query
    )
    print(response.status_code,response.text)

    if response.status_code == 200:
        results = response.json()
        hits = results["hits"]["hits"]
        print(hits)
        if hits:
            chosen_restaurant = random.choice(hits)["_source"]
            return chosen_restaurant["restaurant_id"]
    
    return None


def get_restaurant_details(business_id):
    response = table.get_item(Key={"business_id": business_id})
    return response.get("Item", {})


def send_email(to_email, recommendations, cuisine, num_people, dining_date, dining_time):
    subject = "Your Restaurant Suggestions"
    body_text = f"Hello! Here are my {cuisine} restaurant suggestions for {num_people}, for {dining_date} at {dining_time}:\n\n"

    for idx, r in enumerate(recommendations, start=1):
        body_text += f"{idx}. {r['name']}, located at {r['address']} (Rating: {r['rating']})\n"

    body_text += "\nEnjoy your meal!"

    response = ses.send_email(
        Source=SENDER_EMAIL,
        Destination={"ToAddresses": [to_email]},
        Message={
            "Subject": {"Data": subject},
            "Body": {"Text": {"Data": body_text}}
        }
    )

    print(f"Email sent to {to_email}: {response['MessageId']}")


def lambda_handler(event, context):
    sqs_message, receipt_handle = get_message_from_sqs()
    if not sqs_message:
        print("No new messages in SQS.")
        return

    print(f"Processing request: {sqs_message}")

    cuisine = sqs_message["cuisine"]
    email = sqs_message["email"]
    num_people = sqs_message["num_people"]
    dining_date = sqs_message["dining_date"]
    dining_time = sqs_message["dining_time"]

    recommended_restaurants = []
    
    for _ in range(3):
        restaurant_id = get_random_restaurant(cuisine[0].upper()+cuisine[1:])
        print(restaurant_id, cuisine[0].upper()+cuisine[1:])
        if restaurant_id:
            details = get_restaurant_details(restaurant_id)
            recommended_restaurants.append(details)

    print("cantidad de restaurantes:",len(recommended_restaurants))
    if recommended_restaurants:
        send_email(email, recommended_restaurants, cuisine, num_people, dining_date, dining_time)
    

    sqs.delete_message(
        QueueUrl=SQS_QUEUE_URL,
        ReceiptHandle=receipt_handle
    )
    print("Processed and deleted SQS message.")

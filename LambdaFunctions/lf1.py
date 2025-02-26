import json
import boto3

sqs = boto3.client('sqs')
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/904233095319/dining-concierge-queue-jclsn"

def push_to_sqs(location, cuisine, dining_time, dining_date, num_people, phone_number, email):
    message = {
        "location": location,
        "cuisine": cuisine,
        "dining_time": dining_time,
        "dining_date": dining_date,
        "num_people": num_people,
        "phone_number": phone_number,
        "email": email
    }
    
    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message)
    )

def lambda_handler(event, context):

    print("Event: ", json.dumps(event))

    intent_name = event['sessionState']['intent']['name']

    if intent_name == "GreetingIntent":
        return generate_response("Hi there, how can I help?", intent_name)
    
    elif intent_name == "ThankYouIntent":
        return generate_response("You're welcome! Have a great day.", intent_name)
    
    elif intent_name == "DiningSuggestionsIntent":

        return handle_dining_suggestions(event)
    
    return generate_response("Sorry, I didn't understand that.",intent_name)

def generate_response(message, intent_name):
    return {
        "sessionState": {
            "dialogAction": {
                "type": "Close"
            },
            "intent": {
                "name": intent_name,
                "state": "Fulfilled"
            }
        },
        "messages": [
            {
                "contentType": "PlainText", 
                "content": message
                }
        ]
    }

def handle_dining_suggestions(event):
    intent_name = event['sessionState']['intent']['name']
    slots = event['sessionState']['intent']['slots']

    if not slots:
        return generate_response("I didn't understand your request. Could you repeat that?", intent_name)            
        
    
    location = slots.get("Location", {}).get("value", {}).get("interpretedValue", "unknown location")
    cuisine = slots.get("Cuisine", {}).get("value", {}).get("interpretedValue", "any cuisine")
    dining_time = slots.get("DiningTime", {}).get("value", {}).get("interpretedValue", "unspecified time")
    dining_date = slots.get("DiningDate", {}).get("value", {}).get("interpretedValue", "unspecified date")
    num_people = slots.get("NumPeople", {}).get("value", {}).get("interpretedValue", "some")
    phone_number = slots.get("PhoneNumber", {}).get("value", {}).get("interpretedValue", "no phone number provided")
    email = slots.get("Email", {}).get("value", {}).get("interpretedValue", "no email provided")

    response_msg = f"Got it! You want {cuisine} food in {location} on {dining_date} at {dining_time} for {num_people}. Expect my suggestions shortly! Have a good day."
    
    push_to_sqs(location, cuisine, dining_time, dining_date, num_people, phone_number, email)

    return generate_response(response_msg, intent_name)


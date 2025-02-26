import json
import boto3
import datetime
import re

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

    if event['invocationSource'] == 'DialogCodeHook':

        if not any(slots.values()):  
            return {
                "sessionState": {
                    "dialogAction": {
                        "slotToElicit": "Location",
                        "type": "ElicitSlot"
                    },
                    "intent": {
                        "name": intent_name,
                        "slots": slots
                    }
                },
                "messages": [{"contentType": "PlainText", "content": "What city are you looking to dine in?"}]
            }

   
        validation = validate_slots(slots)

        if not validation['isValid']:
            return {
                'sessionState': {
                    'dialogAction': {
                        'slotToElicit': validation['invalidSlot'],
                        'type': 'ElicitSlot'
                    },
                    'intent': {
                        'name': intent_name,
                        'slots': slots
                    },
                    'messages': [
                        {
                            'contentType': 'PlainText',
                            'content': validation['message']
                        }
                    ]
                    
                }
            }
        
        return {
            'sessionState': {
                'dialogAction': {
                    'type': 'Delegate'
                },
                'intent': {
                    'name': intent_name,
                    'slots': slots
                }
            }
            
        }

    if event['invocationSource'] == 'FulfillmentCodeHook':
        return handle_fulfillment(intent_name, slots)
    
    return generate_response("Sorry, I couldn't process your request", intent_name)


def validate_slots(slots):
    """Validate slot values before proceeding."""

    location = slots.get("Location", {}).get("value", {}).get("interpretedValue") if slots.get("Location") else None
    cuisine = slots.get("Cuisine", {}).get("value", {}).get("interpretedValue") if slots.get("Cuisine") else None
    dining_time = slots.get("DiningTime", {}).get("value", {}).get("interpretedValue") if slots.get("DiningTime") else None
    dining_date = slots.get("DiningDate", {}).get("value", {}).get("interpretedValue") if slots.get("DiningDate") else None
    num_people = slots.get("NumPeople", {}).get("value", {}).get("interpretedValue") if slots.get("NumPeople") else None
    phone_number = slots.get("PhoneNumber", {}).get("value", {}).get("interpretedValue") if slots.get("PhoneNumber") else None
    email = slots.get("Email", {}).get("value", {}).get("interpretedValue") if slots.get("Email") else None

    if not location:
        return {"isValid": False, "invalidSlot": "Location", "message": "Please specify a location."}

    if not cuisine:
        return {"isValid": False, "invalidSlot": "Cuisine", "message": "Please specify a cuisine type."}

    if num_people is None or int(num_people) <= 0:
        return {"isValid": False, "invalidSlot": "NumPeople", "message": "Please enter a valid number of people."}

    if not dining_time:
        return {"isValid": False, "invalidSlot": "DiningTime", "message": "Please specify a dining time."}

    if not dining_date:
        return {"isValid": False, "invalidSlot": "DiningDate", "message": "Please specify a dining date."}

 
    date_obj = datetime.datetime.strptime(dining_date, "%Y-%m-%d").date()
    if date_obj < datetime.date.today():
        return {"isValid": False, "invalidSlot": "DiningDate", "message": "The date must be in the future."}


    if phone_number and not re.match(r'^\d{10}$', phone_number):
        return {"isValid": False, "invalidSlot": "PhoneNumber", "message": "Invalid phone number. Use 10 digits."}

    if email and not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        return {"isValid": False, "invalidSlot": "Email", "message": "Invalid email format."}

    return {"isValid": True}

def handle_fulfillment(intent_name, slots):
    
    location = slots.get("Location", {}).get("value", {}).get("interpretedValue", "unknown location")
    cuisine = slots.get("Cuisine", {}).get("value", {}).get("interpretedValue", "any cuisine")
    dining_time = slots.get("DiningTime", {}).get("value", {}).get("interpretedValue", "unspecified time")
    dining_date = slots.get("DiningDate", {}).get("value", {}).get("interpretedValue", "unspecified date")
    num_people = slots.get("NumPeople", {}).get("value", {}).get("interpretedValue", "some")
    phone_number = slots.get("PhoneNumber", {}).get("value", {}).get("interpretedValue", "no phone number provided")
    email = slots.get("Email", {}).get("value", {}).get("interpretedValue", "no email provided")

    response_msg = f"Got it! You want {cuisine} food in {location} on {dining_date} at {dining_time} for {num_people} people. Expect my suggestions shortly!"

    push_to_sqs(location, cuisine, dining_time, dining_date, num_people, phone_number, email)

    return generate_response(response_msg, intent_name)
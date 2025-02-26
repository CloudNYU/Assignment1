import json
import datetime
import boto3

lex_client = boto3.client('lexv2-runtime', region_name='us-east-1')

BOT_ID = "7V7ZNYC8H6"
BOT_ALIAS_ID = "TSTALIASID"
LOCALE_ID = "en_US" 

def lambda_handler(event, context):
    user_message = event.get("messages", [{}])[0].get("unstructured", {}).get("text")

    if not user_message:
        return generate_response("I'm sorry, I didn't receive any message.")

    lex_response = call_lex(user_message, "user-123")
    dialog_state = lex_response.get("sessionState", {}).get("dialogAction", {}).get("type", "Close")

    messages = lex_response.get("messages", [])
    bot_responses = "\n".join([msg.get("content", "Sorry, I didn't understand that.") for msg in messages])

    return generate_response(bot_responses)

def call_lex(user_message, session_id):
    response = lex_client.recognize_text(
        botId=BOT_ID,
        botAliasId=BOT_ALIAS_ID,
        localeId=LOCALE_ID,
        sessionId=session_id,
        text=user_message
    )
    return response

def generate_response(message):
    response = {
        "messages": [
            {
                "type": "unstructured",
                "unstructured": {
                    "id": "001",
                    "text": message,
                    "timestamp": datetime.datetime.utcnow().isoformat()
                }
            }
        ]
    }

    return {
        'statusCode': 200,
        "headers": {"Content-Type": "application/json"},
        'body': json.dumps(response)
    }

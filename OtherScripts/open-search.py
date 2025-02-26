# import requests
from requests_aws4auth import AWS4Auth
# import boto3

# # Configuración de AWS y OpenSearch
REGION = "us-east-1"
# OPENSEARCH_URL = "https://search-restaurants-opensearch-yl6nmx64chp4wr5mzklj2jhdge.aos.us-east-1.on.aws"
# INDEX_NAME = "restaurants"

# # Obtener credenciales IAM automáticamente
# credentials = boto3.Session().get_credentials()
# aws_auth = AWS4Auth(credentials.access_key, credentials.secret_key, REGION, "es", session_token=credentials.token)

# def create_index():
#     """Crea el índice en OpenSearch con configuración correcta de shards y replicas"""
#     index_settings = {
#         "settings": {
#             "number_of_shards": 3,  # ✅ Ajustado a 3 shards
#             "number_of_replicas": 0,  # ✅ Ajustado a 3 réplicas para cumplir con AZ-awareness
#         },
#         "mappings": {
#             "properties": {
#                 "restaurant_id": {"type": "keyword"},
#                 "cuisine": {"type": "text"}
#             }
#         }
#     }
    
#     response = requests.put(f"{OPENSEARCH_URL}/{INDEX_NAME}", json=index_settings, auth=aws_auth)

#     if response.status_code == 200:
#         print(f"✅ Índice '{INDEX_NAME}' creado en OpenSearch.")
#     else:
#         print(f"❌ Error al crear índice: {response.text}")

# create_index()


import boto3
import requests
from requests.auth import HTTPBasicAuth

# Conexión con DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table("yelp-restaurants")

# Configurar OpenSearch
OPENSEARCH_URL = "https://search-restaurants-opensearch-yl6nmx64chp4wr5mzklj2jhdge.aos.us-east-1.on.aws"
INDEX_NAME = "restaurants"
# Obtener credenciales IAM automáticamente
credentials = boto3.Session().get_credentials()
aws_auth = AWS4Auth(credentials.access_key, credentials.secret_key, REGION, "es", session_token=credentials.token)

def load_data_to_opensearch():
    """Carga los datos desde DynamoDB a OpenSearch."""
    scan = table.scan()
    restaurants = scan.get('Items', [])

    while 'LastEvaluatedKey' in scan:
        scan = table.scan(ExclusiveStartKey=scan['LastEvaluatedKey'])
        restaurants.extend(scan.get('Items', []))

    for restaurant in restaurants:
        try:
            document = {
                "restaurant_id": restaurant["business_id"],
                "cuisine": restaurant["cuisine"]
            }
            
            response = requests.post(f"{OPENSEARCH_URL}/{INDEX_NAME}/_doc/{restaurant['business_id']}", 
                                     json=document, 
                                     auth=aws_auth )
            
            if response.status_code in [200, 201]:
                print(f"✅ Inserted: {restaurant['business_id']} - {restaurant['cuisine']}")
            else:
                print(f" Error inserting {restaurant['business_id']}: {response.text}")

        except Exception as e:
            print(f" Error: {e}")

load_data_to_opensearch()

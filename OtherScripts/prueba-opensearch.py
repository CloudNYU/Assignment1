import requests
from requests_aws4auth import AWS4Auth
import boto3

# Configuración de AWS y OpenSearch
REGION = "us-east-1"
OPENSEARCH_URL = "https://search-restaurants-opensearch-yl6nmx64chp4wr5mzklj2jhdge.aos.us-east-1.on.aws"
INDEX_NAME = "restaurants"

# Obtener credenciales IAM automáticamente
session = boto3.Session()
credentials = session.get_credentials()
auth = AWS4Auth(credentials.access_key, credentials.secret_key, REGION, "es", session_token=credentials.token)

def search_by_cuisine(cuisine_type):
    """Realiza una búsqueda en OpenSearch filtrando por cuisine"""
    search_query = {
        "query": {
            "match": {
                "cuisine": cuisine_type  # Filtra por el tipo de comida
            }
        }
    }
    
    url = f"{OPENSEARCH_URL}/{INDEX_NAME}/_search"
    headers = {"Content-Type": "application/json"}
    
    response = requests.post(url, auth=auth, headers=headers, json=search_query)
    
    if response.status_code == 200:
        results = response.json()
        hits = results["hits"]["hits"]
        print(f"🔍 {len(hits)} restaurantes encontrados para cuisine: {cuisine_type}")
        for hit in hits:
            print(f"- {hit['_source']['restaurant_id']} | {hit['_source']['cuisine']}")
    else:
        print(f" Error en la búsqueda: {response.status_code}, {response.text}")

# 🔹 Prueba la búsqueda
search_by_cuisine("Chinese")

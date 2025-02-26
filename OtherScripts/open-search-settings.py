import requests
from requests_aws4auth import AWS4Auth
import boto3

# Configuración de AWS
REGION = "us-east-1"
SERVICE = "es"  # OpenSearch antes llamado Elasticsearch
OPENSEARCH_URL = "https://search-restaurants-opensearch-yl6nmx64chp4wr5mzklj2jhdge.aos.us-east-1.on.aws"

# Obtener credenciales de AWS automáticamente
session = boto3.Session()
credentials = session.get_credentials()
aws_auth = AWS4Auth(credentials.access_key, credentials.secret_key, REGION, SERVICE, session_token=credentials.token)

def check_cluster_settings():
    """Consulta la configuración del clúster de OpenSearch usando IAM Authentication"""
    url = f"{OPENSEARCH_URL}/_cluster/settings?include_defaults=true"

    response = requests.get(url, auth=aws_auth)

    if response.status_code == 200:
        print(" Configuración del clúster obtenida con éxito:")
        print(response.json())  # Imprime toda la configuración
    else:
        print(f" Error al obtener configuración: {response.status_code} - {response.text}")

check_cluster_settings()

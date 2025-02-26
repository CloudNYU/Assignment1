import requests
import boto3
import json
import time
from datetime import datetime
from decimal import Decimal

# Configuración de Yelp API
API_KEY = "yADxiM45qdQAnsOycO-iLHKse-9SMEkFg2aWfJv_cI4YiQBPQYFUfpUpl3wI4lPIQ4etK3ggwQ-AAPCVKVhQ9brhqTC7vtxhm2Tm9r5EaQ-blIDHl_7Dv_l3ZgK9Z3Yx"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}
YELP_ENDPOINT = "https://api.yelp.com/v3/businesses/search"

# Configuración de DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table("yelp-restaurants")

# Cocinas a buscar
CUISINES = ["Steakhouse", "Street", "Kebab", "International"]	

# Diferentes términos de búsqueda para cada tipo de cocina
SEARCH_TERMS = ["restaurants", "food", "dining", "best places"]

# Barrios de Manhattan para diversificar la búsqueda
NEIGHBORHOODS = [
    "Harlem, NY", "SoHo, NY", "Upper East Side, NY", "Midtown, NY",
    "Chelsea, NY", "Tribeca, NY", "Greenwich Village, NY"
]

LIMIT = 50  # Yelp permite un máximo de 50 por consulta

# Función para convertir Decimal a float antes de serializar JSON
def decimal_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError("Object of type %s is not JSON serializable" % type(obj))

def get_restaurants(cuisine, search_term, location, offset=0):
    """Consulta Yelp API para obtener restaurantes de un tipo de cocina específico en una ubicación."""
    params = {
        "term": f"{cuisine} {search_term}",
        "location": location,
        "limit": LIMIT,
        "offset": offset,
        "sort_by": "rating"
    }
    
    response = requests.get(YELP_ENDPOINT, headers=HEADERS, params=params)
    
    if response.status_code != 200:
        print(f"Error: {response.status_code}, {response.json()}")
        return []
    
    return response.json().get("businesses", [])

def restaurant_exists(business_id):
    """Verifica si un restaurante ya existe en la base de datos para evitar duplicados."""
    response = table.get_item(Key={'business_id': business_id})
    return 'Item' in response

def store_restaurants_in_dynamodb(restaurants, cuisine):
    """Guarda los restaurantes en DynamoDB sin duplicar datos."""
    for restaurant in restaurants:
        try:
            business_id = restaurant["id"]

            # Evitar insertar duplicados en DynamoDB
            if restaurant_exists(business_id):
                print(f" Already exists: {restaurant['name']}")
                continue

            item = {
                "business_id": business_id,
                "name": restaurant["name"],
                "address": ", ".join(restaurant["location"]["display_address"]),
                "coordinates": json.dumps({
                    "latitude": Decimal(str(restaurant["coordinates"]["latitude"])),
                    "longitude": Decimal(str(restaurant["coordinates"]["longitude"]))
                }, default=decimal_to_float),
                "review_count": int(restaurant["review_count"]),
                "rating": Decimal(str(restaurant["rating"])),
                "zip_code": restaurant["location"].get("zip_code", "N/A"),
                "cuisine": cuisine,
                "insertedAtTimestamp": datetime.utcnow().isoformat()
            }
            
            table.put_item(Item=item)
            print(f" Inserted: {restaurant['name']}")

        except Exception as e:
            print(f" Error inserting {restaurant['name']}: {e}")

def scrape_yelp():
    """Obtiene y almacena 1000+ restaurantes por tipo de cocina variando los parámetros de búsqueda."""
    for cuisine in CUISINES:
        print(f"\n🔍 Scraping cuisine: {cuisine}")

        count = 0
        for search_term in SEARCH_TERMS:
            for neighborhood in NEIGHBORHOODS:
                offset = 0

                while count < 1000 and offset <= 190:
                    restaurants = get_restaurants(cuisine, search_term, neighborhood, offset)
                    if not restaurants:
                        break
                    
                    store_restaurants_in_dynamodb(restaurants, cuisine)
                    count += len(restaurants)
                    offset += LIMIT
                    time.sleep(1)

if __name__ == "__main__":
    scrape_yelp()
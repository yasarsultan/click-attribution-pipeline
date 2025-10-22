import os
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

PROJECT_ID = os.getenv('PROJECT_ID')
DATASET_ID = os.getenv('DATASET_ID')
TABLE_ID = 'streaming_events'
NUM_EVENTS = 20

USERS = [f"user_{i}.{random.randint(1000000000, 9999999999)}" for i in range(100, 105)]
SOURCES = ['google', 'facebook', 'direct', 'twitter', 'newsletter']
MEDIUMS = {'google': ['organic', 'cpc'], 'facebook':['social', 'cpc'], 'direct':[None], 'twitter':['social'], 'newsletter':['email']}
EVENTS = ['session_start', 'page_view', 'page_view','page_view', 'purchase']

def generate_event():
    now_time = int(datetime.now(timezone.utc).timestamp() * 1000000)
    source = random.choice(SOURCES)
    event_name = random.choice(EVENTS)

    return {
        'event_date': datetime.now(timezone.utc).strftime('%Y%m%d'),
        'event_timestamp': now_time + random.randint(0, 300) * 1000000,
        'event_name': event_name,
        'user_pseudo_id': random.choice(USERS),
        'traffic_source': source,
        'traffic_medium': random.choice(MEDIUMS[source]),
        'purchase_revenue': round(random.uniform(20, 400), 2) if event_name == 'purchase' else None
        }

def stream_events():
    credentials = service_account.Credentials.from_service_account_file('service-account-file.json')
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    client.get_table(table_ref)

    events = [generate_event() for _ in range(NUM_EVENTS)]
    
    purchases = sum(1 for e in events if e['event_name'] == 'purchase')
    revenue = sum(e['purchase_revenue'] or 0 for e in events)
    
    print(f"\nStreaming {NUM_EVENTS} events...")
    print(f"  - Purchases: {purchases}")
    print(f"  - Revenue: {revenue:.2f}\n")
    
    errors = client.insert_rows_json(table_ref, events)
    
    if errors:
        print(f"Errors: {errors}")
        return False
    else:
        print(f"Successfully streamed {NUM_EVENTS} events!")
        return True


if __name__ == "__main__":
    try:
        stream_events()
    except Exception as e:
        print(f"Error: {e}")
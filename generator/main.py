import json
import time
import random
import datetime
from faker import Faker
from confluent_kafka import Producer
import config

fake = Faker()

def delivery_report(err, msg):
    if err is not None:
        print(f" Message delivery failed: {err}")
    else:
        pass

def generate_event() -> dict:
    user_id = random.choice(config.USER_IDS)
    event_type = random.choices(config.EVENT_TYPES, weights=config.EVENT_WEIGHTS)[0]
    
    event = {
        "event_id": fake.uuid4(),
        "user_id": user_id,
        "session_id": fake.uuid4(),
        "event_type": event_type,
        "page_url": random.choice(config.PAGES),
        "referrer": fake.url() if random.random() > 0.4 else None,
        "user_agent": fake.user_agent(),
        "ip_address": fake.ipv4(),
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "attributes": {
            "item_category": random.choice(config.CATEGORIES) if event_type in ["add_to_cart", "checkout"] else None,
            "price": round(random.uniform(10.0, 1500.0), 2) if event_type in ["add_to_cart", "checkout"] else 0.0,
            "search_query": fake.word() if event_type == "search" else None
        }
    }
    return event

def main():
    print(f"Starting Event Generator targetting broker: {config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Streaming events to topic: '{config.TOPIC_NAME}'")

    producer_conf = {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'uba-data-generator',
        'acks': 'all', 
        'retries': 5
    }

    producer = Producer(producer_conf)
    
    count = 0
    try:
        while True:
            event = generate_event()
            payload = json.dumps(event).encode('utf-8')
            
            producer.produce(
                topic=config.TOPIC_NAME,
                key=event["user_id"].encode('utf-8'),
                value=payload,
                on_delivery=delivery_report
            )
            
            producer.poll(0)
            
            count += 1
            if count % 20 == 0:
                print(f"Generated and sent {count} events so far... (Latest: {event['event_type']} by {event['user_id']})")
                producer.flush()
                
            time.sleep(1.0 / config.EVENTS_PER_SECOND)

    except KeyboardInterrupt:
        print("\n Stopping Data Generator...")
    finally:
        print("Flushing remaining messages...")
        producer.flush()

if __name__ == "__main__":
    main()
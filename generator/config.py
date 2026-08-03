import os

# Redpanda Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "user_events")

# Generation Rate (Events per second)
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "5.0"))

# Sample User IDs for realistic events matching metadata in Postgres
USER_IDS = ["usr_1001", "usr_1002", "usr_1003", "usr_1004"]
# Event Types and Weights (Page views are more common than purchases)
EVENT_TYPES = ["page_view", "click", "search", "add_to_cart", "checkout"]
EVENT_WEIGHTS = [0.50, 0.25, 0.10, 0.10, 0.05]

# Sample Pages & Categories
PAGES = [
    "/home", "/search", "/products/laptop", "/products/phone", 
    "/cart", "/checkout", "/profile", "/deals"
]
CATEGORIES = ["electronics", "clothing", "home_decor", "books", "beauty"]
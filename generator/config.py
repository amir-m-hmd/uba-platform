import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "user_events")

EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "5.0"))

USER_IDS = ["usr_1001", "usr_1002", "usr_1003", "usr_1004"]
EVENT_TYPES = ["page_view", "click", "search", "add_to_cart", "checkout"]
EVENT_WEIGHTS = [0.50, 0.25, 0.10, 0.10, 0.05]

PAGES = [
    "/home", "/search", "/products/laptop", "/products/phone", 
    "/cart", "/checkout", "/profile", "/deals"
]
CATEGORIES = ["electronics", "clothing", "home_decor", "books", "beauty"]
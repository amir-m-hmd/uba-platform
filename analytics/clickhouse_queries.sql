SELECT 
    event_type,
    count() AS total_events,
    uniqExact(user_id) AS unique_users,
    round(avg(price), 2) AS avg_item_price
FROM uba_analytics.user_events_enriched
GROUP BY event_type
ORDER BY total_events DESC;
SELECT 
    country,
    device_type,
    count() AS event_count,
    countDistinct(user_id) AS active_users
FROM uba_analytics.user_events_enriched
WHERE country IS NOT NULL
GROUP BY country, device_type
ORDER BY event_count DESC;
SELECT 
    username,
    email,
    countIf(event_type = 'add_to_cart') AS cart_additions,
    countIf(event_type = 'checkout') AS checkouts,
    sumIf(price, event_type = 'checkout') AS total_spent
FROM uba_analytics.user_events_enriched
WHERE is_premium = 1
GROUP BY username, email
ORDER BY total_spent DESC;
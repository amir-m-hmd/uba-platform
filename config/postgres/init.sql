-- Create Airflow Metadata Database
CREATE DATABASE airflow_metadata;
CREATE DATABASE superset_metadata;

\c uba_metadata;
-- Create users table for enrichment
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(64) PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    country VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    device_type VARCHAR(50) NOT NULL,
    signup_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_premium BOOLEAN DEFAULT FALSE
);

-- Insert sample metadata for demonstration
INSERT INTO users (user_id, username, email, country, city, device_type, is_premium)
VALUES 
    ('usr_1001', 'ali_rezai', 'ali@example.com', 'Iran', 'Tehran', 'Mobile', true),
    ('usr_1002', 'sara_ahmadi', 'sara@example.com', 'Iran', 'Isfahan', 'Desktop', false),
    ('usr_1003', 'john_doe', 'john@example.com', 'Germany', 'Berlin', 'Mobile', true),
    ('usr_1004', 'maria_garcia', 'maria@example.com', 'Spain', 'Madrid', 'Tablet', false)
ON CONFLICT (user_id) DO NOTHING;
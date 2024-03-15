CREATE TABLE processed_agent_data (
    id SERIAL PRIMARY KEY,
    road_state varchar NOT NULL,
    user_id int,
    x FLOAT,
    y FLOAT,
    z FLOAT,
    latitude FLOAT,
    longitude FLOAT,
    timestamp TIMESTAMP
);
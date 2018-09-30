DROP TABLE IF EXISTS weather;
CREATE TABLE weather(
record_id INTEGER NOT NULL PRIMARY KEY,
city_id INTEGER references city(city_id),
dmy DATE,
t FLOAT,
humidity FLOAT,
wind_speed INTEGER,
wind_direction VARCHAR,
precipitation FLOAT,
precipitation_type VARCHAR
);
SET datestyle = dmy;
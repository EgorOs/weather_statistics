DROP TABLE IF EXISTS weather;
CREATE TABLE weather(
city_id INTEGER references city(city_id),
dmy DATE,
time_of_day TIME,
t FLOAT,
humidity FLOAT,
wind_speed INTEGER,
wind_direction VARCHAR,
precipitation INTEGER,
precipitation_type VARCHAR
);
SET datestyle = dmy;
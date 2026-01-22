SELECT
    city,
    AVG(temperature_2m) as avg_temperature
FROM {{ source('weather', 'weather_data') }}
GROUP BY city

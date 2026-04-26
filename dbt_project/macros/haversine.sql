/*
    Macro: Haversine distance calculation.

    Calculates the great-circle distance between two points on Earth
    given their latitude and longitude in degrees.

    Formula: d = 2r * arcsin(sqrt(
        sin^2((lat2-lat1)/2) + cos(lat1)*cos(lat2)*sin^2((lon2-lon1)/2)
    ))

    Returns distance in kilometers.
*/

{% macro haversine(lat1, lon1, lat2, lon2) %}
    6371.0 * 2 * asin(sqrt(
        power(sin(radians({{ lat2 }} - {{ lat1 }}) / 2), 2)
        + cos(radians({{ lat1 }}))
          * cos(radians({{ lat2 }}))
          * power(sin(radians({{ lon2 }} - {{ lon1 }}) / 2), 2)
    ))
{% endmacro %}

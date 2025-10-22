select stationname 
from weather_stations 
where stationid = (
    select stationid 
    from weather_station_measurements 
    order by ABS(feeltemperature - temperature) desc 
    limit 1
    )
;
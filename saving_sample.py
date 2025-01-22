from save_to_redis import save_to_redis
from database_connect import *

redis_db = redis_connect()
station_collection, wojewodztwa_collection, powiaty_collection = mongodb_connect()

# redis_db.flushdb()

# województwo Mazowieckie
stations_mazowieckie = station_collection.find({"wojewodztwo": "mazowieckie"})
stations_mazowieckie = [station['ifcid'] for station in stations_mazowieckie]

# save_to_redis(redis_db, station_collection, stations_mazowieckie, 2024, 10)

#  Warszawa
stations_warszawa = station_collection.find({"powiat": "Warszawa"})
stations_warszawa = [station['ifcid'] for station in stations_warszawa]

save_to_redis(redis_db, station_collection, stations_warszawa, 2024, 10)
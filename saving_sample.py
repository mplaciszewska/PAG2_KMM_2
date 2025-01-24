from save_to_redis import save_to_redis
from database_connect import *

redis_db = redis_connect()
station_collection, wojewodztwa_collection, powiaty_collection = mongodb_connect()

# redis_db.flushdb()

# województwa
names = ['mazowieckie', 'łódzkie', 'opolskie', 'śląskie']
for name in names:
    stations = station_collection.find({"wojewodztwo": name})
    stations = [station['ifcid'] for station in stations]
    save_to_redis(redis_db, station_collection, stations, 2024, 10)

#  Warszawa
stations_warszawa = station_collection.find({"powiat": "Warszawa"})
stations_warszawa = [station['ifcid'] for station in stations_warszawa]
for month in range(1, 13):
    save_to_redis(redis_db, station_collection, stations_warszawa, 2024, month)


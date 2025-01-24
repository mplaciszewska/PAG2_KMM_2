import redis
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

def redis_connect():
    return redis.Redis(
        host='host',
        port=11605,
        decode_responses=True,
        username="default",
        password="password",
    )

def mongodb_connect():
    uri = "uri"
    client = MongoClient(uri, server_api=ServerApi('1'))
    mongo_db = client.effacility

    station_collection = mongo_db.stacje
    wojewodztwa_collection = mongo_db.wojewodztwa
    powiaty_collection = mongo_db.powiaty

    return station_collection, wojewodztwa_collection, powiaty_collection

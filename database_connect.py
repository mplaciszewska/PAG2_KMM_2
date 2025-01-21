import redis
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

def redis_connect():
    return redis.Redis(
        host='redis-11605.c300.eu-central-1-1.ec2.redns.redis-cloud.com',
        port=11605,
        decode_responses=True,
        username="default",
        password="98T457YxhsCuPalg8mIaz1qIPKmgaudE",
    )

def mongodb_connect():
    uri = "mongodb+srv://majakretstud:7Dq8XoLzmvl9X9Co@pag2-kmm.o1dnv.mongodb.net/?retryWrites=true&w=majority&appName=pag2-KMM"
    client = MongoClient(uri, server_api=ServerApi('1'))
    mongo_db = client.effacility

    station_collection = mongo_db.stacje
    wojewodztwa_collection = mongo_db.wojewodztwa
    powiaty_collection = mongo_db.powiaty

    return station_collection, wojewodztwa_collection, powiaty_collection
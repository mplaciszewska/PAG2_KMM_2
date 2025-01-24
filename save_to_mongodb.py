import numpy as np
import pandas as pd
import geopandas as gpd
import datetime as dt
from database_connect import *

def read_effacility(path):
    
    effacility = gpd.read_file(path)
    effacility2180 = effacility.to_crs(epsg=2180)

    return effacility

def save_wojewodztwa(wojewodztwa, wojewodztwa_collection):
    wojewodztwa_collection.delete_many({})

    for w in wojewodztwa["name"]:
        woj_gpd = wojewodztwa[wojewodztwa["name"] == w]
        geometry = woj_gpd.iloc[0].geometry.__geo_interface__  
        wojewodztwa_collection.insert_one({"name": w, "geometry": geometry})

    return wojewodztwa


def save_powiaty(powiaty, powiaty_collection):      
    powiaty_collection.delete_many({})

    wojewodztwa_teryt = {
        "02": "dolnośląskie",
        "04": "kujawsko-pomorskie",
        "06": "lubelskie",
        "08": "lubuskie",
        "10": "łódzkie",
        "12": "małopolskie",
        "14": "mazowieckie",
        "16": "opolskie",
        "18": "podkarpackie",
        "20": "podlaskie",
        "22": "pomorskie",
        "24": "śląskie",
        "26": "świętokrzyskie",
        "28": "warmińsko-mazurskie",
        "30": "wielkopolskie",
        "32": "zachodniopomorskie",
    }

    for teryt in powiaty["national_c"]:
        pow_gpd = powiaty[powiaty["national_c"] == teryt]
        name = pow_gpd.iloc[0]["name"]
        wojewodztwo = wojewodztwa_teryt[teryt[:2]]
        geometry = pow_gpd.iloc[0].geometry.__geo_interface__  
        powiaty_collection.insert_one({
            "name": name,
            "TERYT": teryt,
            "wojewodztwo": wojewodztwo,
            "geometry": geometry
        })

        return powiaty


def save_stations(path, station_collection, powiaty, wojewodztwa):
    station_collection.delete_many({})

    effacility = read_effacility(path)

    # exclude stations with ifcid starting with 1
    effacility = effacility[~effacility['ifcid'].astype(str).str.startswith('1')]

    for col in effacility.columns:
        if pd.api.types.is_datetime64_any_dtype(effacility[col]):
            
            effacility[col] = effacility[col].where(effacility[col].notna(), None)

            effacility[col] = effacility[col].apply(
                lambda x: x.tz_convert('UTC') if pd.notna(x) and x.tzinfo else x
            )

            effacility[col] = effacility[col].apply(
                lambda x: x.isoformat() if pd.notna(x) else None
            )

    effacility_dict = effacility.to_dict('records')

    for record in effacility_dict:
        record['geometry'] = record['geometry'].__geo_interface__

    try:
        station_collection.insert_many(effacility_dict)
    except Exception as e:
        print(f"An error occurred: {e}")

    # spatial join assign powiat to stacje
    stacje_with_powiat = gpd.sjoin(effacility, powiaty, how="left", predicate="within", lsuffix = "stacja", rsuffix = "powiat")

    for _, row in stacje_with_powiat.iterrows():
        stacja_geojson = row.geometry.__geo_interface__
        station_collection.update_one(
            {"name": row["name_stacja"]},
            {
                "$set": {
                    "powiat": row["name_powiat"],
                    "geometry": stacja_geojson,
                }
            },
            upsert=True
        )

    stacje_with_woj = gpd.sjoin(effacility, wojewodztwa, how="left", predicate="within", lsuffix = "stacja", rsuffix = "woj")
    for _, row in stacje_with_woj.iterrows():
        stacja_geojson = row.geometry.__geo_interface__
        station_collection.update_one(
            {"name": row["name_stacja"]},
            {
                "$set": {
                    "wojewodztwo" : row["name_woj"],
                    "geometry": stacja_geojson,
                }
            },
            upsert=True
        )

def sava_all_data2mongo():
    powiaty_path = r"dane_administracyjne\powiaty.shp"
    woj_path = r"dane_administracyjne\woj.shp"
    effacility_path = r"dane_meteorologiczne\effacility.geojson"

    station_collection, wojewodztwa_collection, powiaty_collection = mongodb_connect()

    woj_data = gpd.read_file(woj_path)
    wojewodztwa = woj_data.to_crs(epsg=2180)
    # save_wojewodztwa(wojewodztwa, wojewodztwa_collection)

    powiaty_data = gpd.read_file(powiaty_path)
    powiaty = powiaty_data.to_crs(epsg=2180)
    # save_powiaty(powiaty, powiaty_collection)
    
    save_stations(effacility_path, station_collection, powiaty, wojewodztwa)

    # create indexes
    wojewodztwa_collection.create_index("name")
    powiaty_collection.create_index("wojewodztwo")
    station_collection.create_index("powiat")
    station_collection.create_index("wojewodztwo")

if __name__ == "__main__":
    sava_all_data2mongo()

import numpy as np
import pandas as pd
import geopandas as gpd
import datetime as dt

def read_effacility():
    effacility_path = r"dane_meteorologiczne\effacility.geojson"
    effacility = gpd.read_file(effacility_path)
    effacility2180 = effacility.to_crs(epsg=2180)

    return effacility

def save_wojewodztwa(wojewodztwa_collection):
    wojewodztwa_collection.delete_many({})

    woj_path = r"dane_administracyjne\woj.shp"

    woj_data = gpd.read_file(woj_path)
    wojewodztwa = woj_data.to_crs(epsg=2180)
    # woj.plot()

    for w in wojewodztwa["name"]:
        woj_gpd = wojewodztwa[wojewodztwa["name"] == w]
        geometry = woj_gpd.iloc[0].geometry.__geo_interface__  
        wojewodztwa_collection.insert_one({"name": w, "geometry": geometry})


def save_powiaty(powiaty_collection):      
    powiaty_collection.delete_many({})

    powiaty_path = r"dane_administracyjne\powiaty.shp"

    powiaty_data = gpd.read_file(powiaty_path)
    powiaty = powiaty_data.to_crs(epsg=2180)
    # powiaty.plot()

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


def save_stations(station_collection, powiaty, wojewodztwa):
    station_collection.delete_many({})

    effacility = read_effacility()

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

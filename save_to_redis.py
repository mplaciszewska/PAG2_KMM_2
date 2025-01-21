import numpy as np
import pandas as pd
import geopandas as gpd
import datetime as dt
import requests
import zipfile
from astral import LocationInfo 
from astral.sun import sun
from astral.location import Location
from bs4 import BeautifulSoup
from scipy import stats
import redis
import json
import pymongo
import tkinter as tkinter
from tkinter import StringVar, OptionMenu, Label, filedialog
from datetime import date
from pyproj import Transformer
import os

def request_meteo_data(INPUT_year, INPUT_month):
    requests_path = "https://danepubliczne.imgw.pl/datastore/getfiledown/Arch/Telemetria/Meteo"

    requests_path = requests_path + "/" + str(INPUT_year) + "/Meteo_" + str(INPUT_year) + "-" + str(INPUT_month).zfill(2) + ".zip"
    requests.get(requests_path)
    if requests.get(requests_path).status_code == 200:
        data = requests.get(requests_path)
        with open("dane_meteorologiczne/Meteo.zip", "wb") as file:
            file.write(data.content)
        print("File downloaded")

    # Unzip the file
    with zipfile.ZipFile("dane_meteorologiczne/Meteo.zip", "r") as zip_ref:
        zip_ref.extractall("dane_meteorologiczne/Meteo")
        print("File unzipped")


def read_csv_to_dataframes(INPUT_year, INPUT_month):
    variables_names = ["air_temp", "ground_temp", "wind_direction", "average_wind_speed", "max_wind_speed", "rainfall_10min", "rainfall_24h", "rainfall_1h", "humidity", "wind_gust", "snow_water_reserve"]
    
    # Meteo files paths
    air_temp_path = "dane_meteorologiczne/Meteo/B00300S_" + str(INPUT_year) + "_" + str(INPUT_month).zfill(2) + ".csv"
    ground_temp_path = "dane_meteorologiczne/Meteo/B00305A_" + str(INPUT_year) + "_" + str(INPUT_month).zfill(2) + ".csv"
    wind_direction_path = "dane_meteorologiczne/Meteo/B00202A_" + str(INPUT_year) + "_" + str(INPUT_month).zfill(2) + ".csv"
    average_wind_speed_path = "dane_meteorologiczne/Meteo/B00702A_" + str(INPUT_year) + "_" + str(INPUT_month).zfill(2) + ".csv"
    max_wind_speed_path = "dane_meteorologiczne/Meteo/B00703A_" + str(INPUT_year) + "_" + str(INPUT_month).zfill(2) + ".csv"
    rainfall_10min_path = "dane_meteorologiczne/Meteo/B00608S_" + str(INPUT_year) + "_" + str(INPUT_month).zfill(2) + ".csv"
    rainfall_24h_path = "dane_meteorologiczne/Meteo/B00604S_" + str(INPUT_year) + "_" + str(INPUT_month).zfill(2) + ".csv"
    rainfall_1h_path = "dane_meteorologiczne/Meteo/B00606S_" + str(INPUT_year) + "_" + str(INPUT_month).zfill(2) + ".csv"
    humidity_path = "dane_meteorologiczne/Meteo/B00802A_" + str(INPUT_year) + "_" + str(INPUT_month).zfill(2) + ".csv"
    wind_gust_path = "dane_meteorologiczne/Meteo/B00714A_" + str(INPUT_year) + "_" + str(INPUT_month).zfill(2) + ".csv"
    snow_water_reserve_path = "dane_meteorologiczne/Meteo/B00910A_" + str(INPUT_year) + "_" + str(INPUT_month).zfill(2) + ".csv"

    # check if file exists
    if not os.path.exists(air_temp_path) and not os.path.exists(ground_temp_path) and not os.path.exists(wind_direction_path) and not os.path.exists(average_wind_speed_path) and not os.path.exists(max_wind_speed_path) and not os.path.exists(rainfall_10min_path) and not os.path.exists(rainfall_24h_path) and not os.path.exists(rainfall_1h_path) and not os.path.exists(humidity_path) and not os.path.exists(wind_gust_path) and not os.path.exists(snow_water_reserve_path):
        request_meteo_data(INPUT_year, INPUT_month)

    all_paths = [air_temp_path, ground_temp_path, wind_direction_path, average_wind_speed_path, max_wind_speed_path, rainfall_10min_path, rainfall_24h_path, rainfall_1h_path, humidity_path, wind_gust_path, snow_water_reserve_path]

    dataframes = {}

    for path in all_paths:
        # check if file exists
        try:
            data = pd.read_csv(path, delimiter=";", header=None, names=["station_code", "parameter_code", "date", "value"])
            dataframes[variables_names[all_paths.index(path)]] = data

            # change date to datetime format and set its as GMT timezone
            dataframes[variables_names[all_paths.index(path)]].date = pd.to_datetime(dataframes[variables_names[all_paths.index(path)]].date)
            dataframes[variables_names[all_paths.index(path)]].date = dataframes[variables_names[all_paths.index(path)]].date.dt.tz_localize("GMT")
                    
        except FileNotFoundError:
            dataframes[variables_names[all_paths.index(path)]] = None

    air_temp, ground_temp, wind_direction, average_wind_speed, max_wind_speed, rainfall_10min, rainfall_24h, rainfall_1h, humidity, wind_gust, snow_water_reserve = dataframes.values()

    return dataframes
def save_to_redis(redis_db, station_collection, INPUT_stations, INPUT_year, INPUT_month):
    variables_names = ["air_temp", "ground_temp", "wind_direction", "average_wind_speed", 
                       "max_wind_speed", "rainfall_10min", "rainfall_24h", "rainfall_1h", 
                       "humidity", "wind_gust", "snow_water_reserve"]
                       
    if INPUT_month == 12:
        days_in_INPUT_month = dt.datetime(INPUT_year + 1, 1, 1) - dt.datetime(INPUT_year, 12, 1)
    else:
        days_in_INPUT_month = dt.datetime(INPUT_year, INPUT_month + 1, 1) - dt.datetime(INPUT_year, INPUT_month, 1)

    dataframes = read_csv_to_dataframes(INPUT_year, INPUT_month)

    for i, dataframe in enumerate(dataframes.values()):
        parameter = variables_names[i]
        if dataframe is None or dataframe.empty:
            continue

        # Safeguard for date parsing
        try:
            dataframe["date"] = pd.to_datetime(dataframe["date"], errors="coerce")
        except Exception as e:
            print(f"Error parsing dates in parameter {parameter}: {e}")
            continue

        dataframe = dataframe.dropna(subset=["date"])  # Drop rows with invalid dates
        
        for station in INPUT_stations:
            station_data = {}

            this_station_data = dataframe[dataframe["station_code"] == station].copy()
            if this_station_data.empty:
                continue

            station_coords = station_collection.find_one({'ifcid': station})['geometry']['coordinates']

            original_crs = "EPSG:2180"
            target_crs = "EPSG:4326"

            transformer = Transformer.from_crs(original_crs, target_crs, always_xy=True)
            x, y = transformer.transform(station_coords[0], station_coords[1])

            INPUT_location = LocationInfo("Warsaw", "Poland", "Europe/Warsaw", latitude=y, longitude=x)

            for day in range(1, days_in_INPUT_month.days + 1):
                date = dt.datetime(INPUT_year, INPUT_month, day)

                start = dt.datetime(INPUT_year, INPUT_month, day, 0, 0, 0)
                end = dt.datetime(INPUT_year, INPUT_month, day, 23, 59, 59)
                start = start.astimezone(dt.timezone.utc)
                end = end.astimezone(dt.timezone.utc)

                sun_data = sun(INPUT_location.observer, date=date.date(), tzinfo=INPUT_location.timezone)
                sunrise = sun_data['sunrise']
                sunset = sun_data['sunset']

                sunrise_rounded = sunrise.replace(minute=(10 * round(sunrise.minute / 10)) % 60)
                sunset_rounded = sunset.replace(minute=(10 * round(sunset.minute / 10)) % 60)
                sunset_rounded = sunset_rounded.replace(second=0, microsecond=0)
                sunrise_rounded = sunrise_rounded.replace(second=0, microsecond=0)

                sunrise_rounded = sunrise_rounded.astimezone(dt.timezone.utc)
                sunset_rounded = sunset_rounded.astimezone(dt.timezone.utc)

                day_data = this_station_data[(this_station_data["date"] >= sunrise_rounded) & (this_station_data["date"] <= sunset_rounded)]
                night_data = this_station_data[(this_station_data["date"] >= start) & (this_station_data["date"] <= end)]
                night_data = night_data[(night_data["date"] < sunrise_rounded) | (night_data["date"] > sunset_rounded)]

                mean_value_day = day_data["value"].mean()
                median_value_day = day_data["value"].median()
                trimmed_mean_value_day = stats.trim_mean(day_data["value"], 0.1)

                mean_value_night = night_data["value"].mean()
                median_value_night = night_data["value"].median()
                trimmed_mean_value_night = stats.trim_mean(night_data["value"], 0.1)

                day_key = f"{station}:{parameter}:{INPUT_year}_{INPUT_month}"
                if day_key not in station_data:
                    station_data[day_key] = {}

                station_data[day_key][day] = {
                    "day_average": round(mean_value_day, 2) if not pd.isna(mean_value_day) else None,
                    "night_average": round(mean_value_night, 2) if not pd.isna(mean_value_night) else None,
                    "day_median": round(median_value_day, 2) if not pd.isna(median_value_day) else None,
                    "night_median": round(median_value_night, 2) if not pd.isna(median_value_night) else None,
                    "day_trimmed_mean": round(trimmed_mean_value_day, 2) if not pd.isna(trimmed_mean_value_day) else None,
                    "night_trimmed_mean": round(trimmed_mean_value_night, 2) if not pd.isna(trimmed_mean_value_night) else None,
                }

            for key, daily_data in station_data.items():
                for day, metrics in daily_data.items():
                    redis_hash_key = f"{key}:{day:02d}"
                    redis_db.delete(redis_hash_key)
                    if not redis_db.exists(redis_hash_key):
                        redis_db.hset(redis_hash_key, mapping=metrics)

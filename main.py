import customtkinter as ctk 
import datetime as dt
from tkinter import filedialog
import json

from database_connect import *
from save_to_redis import *
from save_to_mongodb import *

global station_collection, wojewodztwa_collection, powiaty_collection
global redis_db

possible_parameters = ["air_temp", "ground_temp", "wind_direction", "average_wind_speed", "max_wind_speed", "rainfall_10min", "rainfall_24h", "rainfall_1h", "humidity", "wind_gust", "snow_water_reserve"]
possible_values = ["day_average", "night_average", "day_median", "night_median", "day_trimmed_mean", "night_trimmed_mean"]

colors = {
    "1": "#faa307",
   "2": "#f48c06",
   "3": "#e85d04",
   "4": "#dc2f02", 
   "5": "#d00000",
   "6": "#9d0208",
   "7": "#6a040f",
   "8": "#370617",
}

redis_db = redis_connect()
station_collection, wojewodztwa_collection, powiaty_collection = mongodb_connect()


def get_station_data_from_redis(selected_station, parameter, INPUT_year, INPUT_month):
    try:
        station_name_parts = selected_station.split(' - ')
        selected_station = station_collection.find_one({
            'name1': station_name_parts[0],
            'additional': station_name_parts[1]
        })
    except ValueError:
        print("Niepoprawne dane wejściowe.")
        return None

    if selected_station:
        station_name = f"{selected_station['name1']} - {selected_station['additional']}"
        station_code = selected_station['ifcid']
    else:
        print(f"Nie znaleziono stacji {station_code} w bazie danych.")
        return
 
    redis_key = f"{station_code}:{parameter}:{INPUT_year}_{INPUT_month}:01"
 
    if redis_db.exists(redis_key):
        keys = redis_db.keys(f"{station_code}:{parameter}:{INPUT_year}_{INPUT_month}:*")
        filename = f"{station_name}_{parameter}_{INPUT_year}_{INPUT_month}.json"

        number_of_days = dt.datetime(INPUT_year, INPUT_month + 1, 1) - dt.datetime(INPUT_year, INPUT_month, 1)
        json_dict = {}
        for day in range(1, number_of_days.days + 1):
            key = f"{station_code}:{parameter}:{INPUT_year}_{INPUT_month}:{day:02d}"
            data = redis_db.hgetall(key)

            json_dict[day] = data

        return json_dict
    
    else:
        print(f"Dane dla stacji {station_code} i daty {INPUT_year}_{INPUT_month} nie istnieją w bazie danych.")
        return None
    

# Create function to output the month and year
def printMonthYear(month, year):
    
    # Create table for the written month
    if month == 1:
        writtenMonth = "January"
    elif month == 2:
        writtenMonth = "February"
    elif month == 3:
        writtenMonth = "March"
    elif month == 4:
        writtenMonth = "April"
    elif month == 5:
        writtenMonth = "May"
    elif month == 6:
        writtenMonth = "June"
    elif month == 7:
        writtenMonth = "July"
    elif month == 8:
        writtenMonth = "August"
    elif month == 9:
        writtenMonth = "September"
    elif month == 10:
        writtenMonth = "October"
    elif month == 11:
        writtenMonth = "November"
    else:
        writtenMonth = "December"

    # Output month and year at top of calendar
    monthYear = ctk.CTkLabel(calendarFrame,  text = writtenMonth + " " + str(year), font= ("Arial", 20))
    monthYear.grid(column = 2, row = 0, columnspan = 3)

# Function to switch month calendar (1 for forwards and -1 for backwards)
def switchMonths(direction):
    global calendarFrame
    global month
    global year
    #check if we are goint to a new year
    if month == 12 and direction == 1:
        month = 0
        year += 1
    if month == 1 and direction == -1:
        month = 13 
        year -= 1

    # Clears the old dictionarys so they can be used in the next month
    textObjectDict.clear()
    saveDict.clear()
    
    # Reprint the calendar with the new values
    calendarFrame.destroy()
    calendarFrame = ctk.CTkFrame(window)
    calendarFrame.grid()
    printMonthYear(month + direction, year) # pylint: disable=E0601
    monthGenerator(dayMonthStarts(month + direction, year), daysInMonth(month + direction, year))
    month += direction

def classifyDay(value, minv, maxv):
    step = (maxv - minv) / 8
    for i in range(1, 9):
        if value <= minv + step * i:
            return i
    return 7

# Creates most of the calendar
def monthGenerator(startDate, numberOfDays):
    # Holds the names for each day of the week 
    dayNames = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    # Places the days of the week on the top of the calender
    for nameNumber in range(len(dayNames)):
        names = ctk.CTkLabel(calendarFrame, text = dayNames[nameNumber])
        names.grid(column = nameNumber, row = 1, sticky = 'nsew')

    index = 0
    day = 1
    for row in range(6):
        for column in range(7):
            if index >= startDate and index <= startDate + numberOfDays-1:
                # Creates a frame that will hold each day and text box
                dayFrame = ctk.CTkFrame(calendarFrame)
   

                # Creates a textbox inside the dayframe
                t = tkinter.Text(dayFrame, width = 10, height = 3)
                t.grid(row = 1)

                # Adds the text object to the save dict
                textObjectDict[day] = t 

                # Changes changes dayframe to be formated correctly
                dayFrame.grid(row=row + 2, column=column, sticky = 'nsew')
                dayFrame.columnconfigure(0, weight = 1)
                dayNumber = ctk.CTkLabel(dayFrame, text = day)
                dayNumber.grid(row = 0)
                day += 1
            index += 1
    
# Create function for calculating if it is a leap year
def isLeapYear(year):
    if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
        return True
    else:
        return False

# Create function for calculating what day month starts
def dayMonthStarts(month, year):
    # Get last two digits (default 21 for 2021)
    lastTwoYear = year - 2000
    # Integer division by 4
    calculation = lastTwoYear // 4
    # Add day of month (always 1)
    calculation += 1
    # Table for adding proper month key
    if month == 1 or month == 10:
        calculation += 1
    elif month == 2 or month == 3 or month == 11:
        calculation += 4
    elif month == 5:
        calculation += 2
    elif month == 6:
        calculation += 5
    elif month == 8:
        calculation += 3
    elif month == 9 or month == 12:
        calculation += 6
    else:
        calculation += 0
    # Check if the year is a leap year
    leapYear = isLeapYear(year)
    # Subtract 1 if it is January or February of a leap year
    if leapYear and (month == 1 or month == 2):
        calculation -= 1
    # Add century code (assume we are in 2000's)
    calculation += 6
    # Add last two digits to the caluclation
    calculation += lastTwoYear
    # Get number output based on calculation (Sunday = 1, Monday =2..... Saturday =0)
    dayOfWeek = (calculation % 7 - 2) % 7 

    return dayOfWeek

# Create function to figure out how many days are in a month
def daysInMonth (month, year):
    # All months that have 31 days
    if month == 1 or month == 3 or month == 5 or month == 7 or month == 8 or month == 12 or month == 10:
        numberDays = 31
    # All months that have 30 days
    elif month == 4 or month == 6 or month == 9 or month == 11:
        numberDays = 30
    else:
        # Check to see if leap year to determine how many days in Feb
        leapYear = isLeapYear(year)
        if leapYear:
            numberDays = 29
        else:
            numberDays = 28
    return numberDays

# Holds the raw text input for each day
saveDict = {}

# Holds the text objects on each day
textObjectDict = {}
 
ctk.set_appearance_mode("System")  
ctk.set_default_color_theme("blue")  
 
window = ctk.CTk()
window.title("Meteo Data")
window.geometry("1400x800")
 
# Dropdown menu for year
year_label = ctk.CTkLabel(window, text="Rok")
year_label.grid(row=1, column=0, padx=10, pady=10)
year_var = ctk.StringVar()
year_var.set("2024")
year_dropdown = ctk.CTkOptionMenu(window, variable=year_var, values=[str(year) for year in range(2008, 2025)])
year_dropdown.grid(row=1, column=1, padx=10, pady=10)
 
# Dropdown menu for month
month_label = ctk.CTkLabel(window, text="Miesiąc")
month_label.grid(row=2, column=0, padx=10, pady=10)
month_var = ctk.StringVar()
month_var.set("10")
month_dropdown = ctk.CTkOptionMenu(window, variable=month_var, values=[str(month) for month in range(1, 13)])
month_dropdown.grid(row=2, column=1, padx=10, pady=10)
 
# Dropdown menu for wojewodztwo
possible_wojewodztwa = wojewodztwa_collection.find()
woj_names = sorted([wojewodztwo["name"] for wojewodztwo in possible_wojewodztwa])
 
woj_label = ctk.CTkLabel(window, text="Województwo")
woj_label.grid(row=4, column=0, padx=10, pady=10)
woj_var = ctk.StringVar()
woj_var.set(woj_names[0])
woj_dropdown = ctk.CTkOptionMenu(window, variable=woj_var, values=woj_names)
woj_dropdown.grid(row=4, column=1, padx=10, pady=10)
 
# Dropdown menu for powiat
def update_powiaty(*args):
    selected_woj = woj_var.get()
    possible_powiaty = powiaty_collection.find({"wojewodztwo": selected_woj})
    powiaty_names = sorted([powiat["name"] for powiat in possible_powiaty])
    powiat_var.set(powiaty_names[0] if powiaty_names else "")
    powiat_dropdown.configure(values=powiaty_names)
    update_stacje()
 
woj_var.trace("w", update_powiaty)
 
powiat_label = ctk.CTkLabel(window, text="Powiat")
powiat_label.grid(row=5, column=0, padx=10, pady=10)
powiat_var = ctk.StringVar()
powiat_var.set("")
powiat_dropdown = ctk.CTkOptionMenu(window, variable=powiat_var, values=[])
powiat_dropdown.grid(row=5, column=1, padx=10, pady=10)
 
# Dropdown menu for stacja
def update_stacje(*args):
    selected_powiat = powiat_var.get()
    possible_stacje = station_collection.find({"powiat": selected_powiat})
    stacje_names = sorted([f"{stacja['name1']} - {stacja['additional']}" for stacja in possible_stacje])
    stacja_var.set(stacje_names[0] if stacje_names else "no stations")
    stacja_dropdown.configure(values=stacje_names)
 
powiat_var.trace("w", update_stacje)
 
stacja_label = ctk.CTkLabel(window, text="Stacja")
stacja_label.grid(row=6, column=0, padx=10, pady=10)
stacja_var = ctk.StringVar()
stacja_var = ctk.StringVar()
stacja_var.set("")
stacja_dropdown = ctk.CTkOptionMenu(window, variable=stacja_var, values=[], width=50)
stacja_dropdown.grid(row=6, column=1, padx=10, pady=10)
 
# Save button
def on_save():
    try: 
        INPUT_wojewodztwo = woj_var.get()
        INPUT_powiat = powiat_var.get()

        powiat_stations = station_collection.find({'powiat': INPUT_powiat})
        INPUT_stations = [station['ifcid'] for station in powiat_stations]

        INPUT_year = int(year_var.get())
        INPUT_month = int(month_var.get())
    except ValueError:
        data_label.configure(text="Niepoprawne dane wejściowe.")
        return

    for station in INPUT_stations:
        key = f"{station}:air_temperature:{INPUT_year}_{INPUT_month}"

    save_to_redis(redis_db, station_collection, INPUT_stations, INPUT_year, INPUT_month)

    print(f"Zapisano dane")
 
save_button = ctk.CTkButton(window, text="Zapisz dane dla powiatu", command=on_save)
save_button.grid(row=7, column=0, columnspan=2, pady=20)
 
# Dropdown menu for parameter
parameter_label = ctk.CTkLabel(window, text="Rodzaj pomiaru")
parameter_label.grid(row=8, column=0, padx=10, pady=10)
 
parameter_var = ctk.StringVar()
parameter_var.set("")
parameter_dropdown = ctk.CTkOptionMenu(window, variable=parameter_var, values=possible_parameters)  
parameter_dropdown.grid(row=8, column=1, padx=10, pady=10)

value_label = ctk.CTkLabel(window, text="Wartość")
value_label.grid(row=9, column=0, padx=10, pady=10)

value_var = ctk.StringVar()
value_var.set("")
value_dropdown = ctk.CTkOptionMenu(window, variable=value_var, values = possible_values)
value_dropdown.grid(row=9, column=1, padx=10, pady=10)
 
# Display data
data_label = ctk.CTkLabel(window, text=" ", justify="left")
data_label.grid(row=11, column=0, columnspan=2, padx=10, pady=10)
 
def on_get_data():
    try: 
        selected_station = stacja_var.get()
        selected_parameter = parameter_var.get()
        selected_value = value_var.get()

        INPUT_year = int(year_var.get())
        INPUT_month = int(month_var.get())
    except ValueError:
        data_label.configure(text="Niepoprawne dane wejściowe.")
        return
 
    station_data = get_station_data_from_redis(selected_station, selected_parameter, INPUT_year, INPUT_month)

    if station_data:
        pretty_data = json.dumps(station_data, indent=4)
        # minv = np.min([station_data[day][selected_value] for day in station_data])
        # maxv = np.max([station_data[day][selected_value] for day in station_data])

        for day in station_data:
            value = station_data[day]
            value = value[selected_value]
            textObjectDict[day].delete("1.0", "end")
            textObjectDict[day].insert("1.0", value)

            # color = colors[str(classifyDay(value, minv, maxv))]
            # textObjectDict[day].config(bg=color, fg="white")
    else:
        data_label.configure(text="Nie znaleziono danych dla wybranej stacji.")
 
get_data_button = ctk.CTkButton(window, text="Pobierz dane", command=on_get_data)
get_data_button.grid(row=10, column=0, columnspan=2, pady=20)
 

calendarFrame = ctk.CTkFrame(window)
calendarFrame.grid(row=0, column=2, rowspan=11, padx=10, pady=10)

def update_calendar(*args):
    global calendarFrame, month, year
    
    # Get the selected month and year
    month = int(month_var.get())
    year = int(year_var.get())
    
    # Clear and regenerate the calendar
    calendarFrame.destroy()
    calendarFrame = ctk.CTkFrame(window)
    calendarFrame.grid(row=0, column=2, rowspan=11, padx=10, pady=10)
    
    printMonthYear(month, year)
    monthGenerator(dayMonthStarts(month, year), daysInMonth(month, year))

month_var.trace("w", update_calendar)
year_var.trace("w", update_calendar)

month = int(month_var.get())
year = int(year_var.get())

printMonthYear(month, year)
monthGenerator(dayMonthStarts(month, year), daysInMonth(month, year))

def on_closing():
    window.destroy()
    window.quit()


 
window.protocol("WM_DELETE_WINDOW", on_closing)
window.mainloop()
 
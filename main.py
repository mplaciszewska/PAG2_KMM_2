import customtkinter as ctk 
import datetime as dt
from tkinter import filedialog

from database_connect import *
from save_to_redis import *
from save_to_mongodb import *

global station_collection, wojewodztwa_collection, powiaty_collection
global redis_db
global stations_with_data

parameters_names = ["Temperatura powietrza", "Temperatura gruntu", "Kierunek wiatru", "Średnia prędkość wiatru", "Maksymalna prędkość wiatru", "Opad 10 min", "Opad 24h", "Opad 1h", "Wilgotność", "Porywy wiatru", "Zasób wodny śniegu"]
possible_parameters = ["air_temp", "ground_temp", "wind_direction", "average_wind_speed", "max_wind_speed", "rainfall_10min", "rainfall_24h", "rainfall_1h", "humidity", "wind_gust", "snow_water_reserve"]
parameters_dict = dict(zip(parameters_names, possible_parameters))

values_names = ["Średnia w dzień", "Średnia w nocy", "Mediana w dzień", "Mediana w nocy", "Średnia obcięta w dzień", "Średnia obcięta w nocy"]
possible_values = ["day_average", "night_average", "day_median", "night_median", "day_trimmed_mean", "night_trimmed_mean"]
values_dict = dict(zip(values_names, possible_values))

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


# Automatically resize window to fit content
def resize_to_fit():
    window.update() 
    width = window.winfo_reqwidth() 
    height = window.winfo_reqheight()
    window.geometry(f"{width}x{height}") 

def get_station_data_from_redis(selected_station, parameter, INPUT_year, INPUT_month):
    if selected_station == 'brak danych':
        return None
    try:
        station_name_parts = selected_station.split(' - ')
        selected_station = station_collection.find_one({
            'name1': station_name_parts[0],
            'additional': station_name_parts[1]
        })
    except ValueError:
        return None

    if selected_station:
        station_code = selected_station['ifcid']

    key_pattern = f"{station_code}:{parameter}:{INPUT_year}_{INPUT_month}:*"
    redis_key = redis_db.keys(key_pattern)
 
    if redis_key:
        if INPUT_month == 12:
            next_year = INPUT_year + 1
            next_month = 1
        else:
            next_year = INPUT_year
            next_month = INPUT_month + 1

        number_of_days = dt.datetime(next_year, next_month, 1) - dt.datetime(INPUT_year, INPUT_month, 1)

        json_dict = {}
        for day in range(1, number_of_days.days + 1):
            key = f"{station_code}:{parameter}:{INPUT_year}_{INPUT_month}:{day:02d}"
            if redis_db.exists(key):
                data = redis_db.hgetall(key)
            else:
                data = None

            json_dict[day] = data

        return json_dict
    
    else:
        return None
    
# Function to output the month and year
def printMonthYear(month, year):
    
    if month == 1:
        writtenMonth = "Styczeń"
    elif month == 2:
        writtenMonth = "Luty"
    elif month == 3:
        writtenMonth = "Marzec"
    elif month == 4:
        writtenMonth = "Kwiecień"
    elif month == 5:
        writtenMonth = "Maj"
    elif month == 6:
        writtenMonth = "Czerwiec"
    elif month == 7:
        writtenMonth = "Lipiec"
    elif month == 8:
        writtenMonth = "Sierpień"
    elif month == 9:
        writtenMonth = "Wrzesień"
    elif month == 10:
        writtenMonth = "Październik"
    elif month == 11:
        writtenMonth = "Listopad"
    else:
        writtenMonth = "Grudzień"

    # Output month and year at top of calendar
    monthYear = ctk.CTkLabel(calendarFrame,  text = writtenMonth + " " + str(year), font= ("Arial", 20))
    monthYear.grid(column = 2, row = 0, columnspan = 3)

# Function to switch month calendar (1 for forwards and -1 for backwards)
def switchMonths(direction):
    global calendarFrame
    global month
    global year
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
    printMonthYear(month + direction, year)
    monthGenerator(dayMonthStarts(month + direction, year), daysInMonth(month + direction, year))
    month += direction

    resize_to_fit()

def classifyDay(value, minv, maxv):
    step = (maxv - minv) / 8
    for i in range(1, 9):
        if value <= minv + step * i:
            return i
    return 7

# Creates most of the calendar
def monthGenerator(startDate, numberOfDays):
    dayNames = ["Poniedziałek", "Wtorek", "Środa", "Czwartek", "Piątek", "Sobota", "Niedziela"]

    # Places the days of the week on the top of the calender
    for nameNumber in range(len(dayNames)):
        names = ctk.CTkLabel(calendarFrame, text = dayNames[nameNumber])
        names.grid(column = nameNumber, row = 1, sticky = 'nsew')

    index = 0
    day = 1
    for row in range(6):
        for column in range(7):
            if index >= startDate and index <= startDate + numberOfDays-1:
                dayFrame = ctk.CTkFrame(calendarFrame)
   
                t = tkinter.Text(dayFrame, width = 10, height = 3)
                t.configure(state="disabled")
                t.grid(row = 1)

                textObjectDict[day] = t 

                # Changes changes dayframe to be formated correctly
                dayFrame.grid(row=row + 2, column=column, sticky = 'nsew')
                dayFrame.columnconfigure(0, weight = 1)
                dayNumber = ctk.CTkLabel(dayFrame, text = day)
                dayNumber.grid(row = 0)
                day += 1
            index += 1

    resize_to_fit()
    
# Create function for calculating if it is a leap year
def isLeapYear(year):
    if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
        return True
    else:
        return False

# Create function for calculating what day month starts
def dayMonthStarts(month, year):
    lastTwoYear = year - 2000
    calculation = lastTwoYear // 4
    calculation += 1
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
    leapYear = isLeapYear(year)

    if leapYear and (month == 1 or month == 2):
        calculation -= 1

    calculation += 6

    calculation += lastTwoYear
    dayOfWeek = (calculation % 7 - 2) % 7 

    return dayOfWeek

# Create function to figure out how many days are in a month
def daysInMonth (month, year):
    if month == 1 or month == 3 or month == 5 or month == 7 or month == 8 or month == 12 or month == 10:
        numberDays = 31
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

saveDict = {}

textObjectDict = {}
 
ctk.set_appearance_mode("light")  
ctk.set_default_color_theme("blue")  
 
window = ctk.CTk()
window.title("Dane meteorologiczne")
window.geometry("1400x800")
 
# Dropdown menu for year
year_label = ctk.CTkLabel(window, text="Rok")
year_label.grid(row=1, column=0, padx=10, pady=10)
year_var = ctk.StringVar()
year_dropdown = ctk.CTkOptionMenu(window, variable=year_var, values=[str(year) for year in range(2017, 2025)])
year_dropdown.grid(row=1, column=1, padx=10, pady=10)
 
# Dropdown menu for month
month_label = ctk.CTkLabel(window, text="Miesiąc")
month_label.grid(row=2, column=0, padx=10, pady=10)
month_var = ctk.StringVar()
month_dropdown = ctk.CTkOptionMenu(window, variable=month_var, values=[str(month) for month in range(1, 13)])
month_dropdown.grid(row=2, column=1, padx=10, pady=10)

 
# Dropdown menu for wojewodztwo
possible_wojewodztwa = wojewodztwa_collection.find()
woj_names = sorted([wojewodztwo["name"] for wojewodztwo in possible_wojewodztwa])
 
woj_label = ctk.CTkLabel(window, text="Województwo")
woj_label.grid(row=4, column=0, padx=10, pady=10)
woj_var = ctk.StringVar()
woj_dropdown = ctk.CTkOptionMenu(window, variable=woj_var, values=woj_names)
woj_dropdown.grid(row=4, column=1, padx=10, pady=10)
 
# Dropdown menu for powiat
def update_powiaty(*args):
    selected_woj = woj_var.get()
    possible_powiaty = powiaty_collection.find({"wojewodztwo": selected_woj})
    powiaty_names = sorted([powiat["name"] for powiat in possible_powiaty])
    powiat_var.set("")
    powiat_dropdown.configure(values=powiaty_names)
    update_stacje()

 
woj_var.trace_add("write", update_powiaty)
 
powiat_label = ctk.CTkLabel(window, text="Powiat")
powiat_label.grid(row=5, column=0, padx=10, pady=10)
powiat_var = ctk.StringVar()
powiat_dropdown = ctk.CTkOptionMenu(window, variable=powiat_var, values=[])
powiat_dropdown.grid(row=5, column=1, padx=10, pady=10)
 
# Dropdown menu for stacja
def update_stacje(*args):
    selected_woj = woj_var.get()
    selected_powiat = powiat_var.get()
    year = year_var.get()
    month = month_var.get()
    
    stacje_names = [] 
    
    if selected_woj:
        query = {"wojewodztwo": selected_woj}
        if selected_powiat:
            query["powiat"] = selected_powiat
        
        possible_stacje = list(station_collection.find(query))
        for stacja in possible_stacje:
            ifcid = stacja.get('ifcid')
            if not ifcid:
                continue
            
            # Check if Redis contains keys for the station
            key_pattern = f"{ifcid}:*:{year}_{month}:*"
            if redis_db.keys(key_pattern):
                display_name = f"{stacja['name1']} - {stacja['additional']}"
                stacje_names.append(display_name)
    
    stacja_var.set("")
    stacja_dropdown.configure(values=stacje_names if stacje_names else ["brak danych"])

powiat_var.trace_add("write", update_stacje)
month_var.trace_add("write", update_stacje)
year_var.trace_add("write", update_stacje)

stacja_label = ctk.CTkLabel(window, text="Stacja")
stacja_label.grid(row=6, column=0, padx=10, pady=10)
stacja_var = ctk.StringVar()
stacja_dropdown = ctk.CTkOptionMenu(window, variable=stacja_var, values=[], width=400)
stacja_dropdown.grid(row=6, column=1, padx=10, pady=10)
 
# Save button
def on_save():
    try: 
        INPUT_wojewodztwo = woj_var.get()
        INPUT_powiat = powiat_var.get()
        if not INPUT_wojewodztwo or not INPUT_powiat:
            data_label.configure(text="Niepoprawne dane wejściowe.")
            return

        possible_stations = station_collection.find({'wojewodztwo': INPUT_wojewodztwo, 'powiat': INPUT_powiat})

        INPUT_stations = [station['ifcid'] for station in possible_stations]

        INPUT_year = int(year_var.get())
        INPUT_month = int(month_var.get())

    except ValueError:
        data_label.configure(text="Niepoprawne dane wejściowe.")
        return

    existing_keys = []
    for station in INPUT_stations:
        key = f"{station}:*:{INPUT_year}_{INPUT_month}:*"
        keys = redis_db.keys(key)
        existing_keys.extend(keys)

    if existing_keys:
        data_label.configure(text="Dane dla wybranego powiatu i daty już istnieją.")
        return
    
    save_to_redis(redis_db, station_collection, INPUT_stations, INPUT_year, INPUT_month)
    data_label.configure(text="Dane zapisane do bazy Redis.")
    update_stacje()

 
save_button = ctk.CTkButton(window, text="Zapisz dane dla powiatu", command=on_save)
save_button.grid(row=7, column=0, columnspan=2, pady=20)
 
# Dropdown menu for parameter
parameter_label = ctk.CTkLabel(window, text="Rodzaj pomiaru")
parameter_label.grid(row=8, column=0, padx=10, pady=10)
 
parameter_var = ctk.StringVar()
parameter_dropdown = ctk.CTkOptionMenu(window, variable=parameter_var, values=parameters_names)  
parameter_dropdown.grid(row=8, column=1, padx=10, pady=10)

value_label = ctk.CTkLabel(window, text="Wartość")
value_label.grid(row=9, column=0, padx=10, pady=10)

value_var = ctk.StringVar()
value_dropdown = ctk.CTkOptionMenu(window, variable=value_var, values = values_names)
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
    
    if not selected_parameter or selected_parameter == "" or not selected_value or selected_value == "":
        data_label.configure(text="Niepoprawne dane wejściowe.")
        return
    if not selected_station or selected_station == "":
        data_label.configure(text="Niepoprawne dane wejściowe.")
        return
    if not INPUT_year or not INPUT_month:
        data_label.configure(text="Niepoprawne dane wejściowe.")
        return
    
    sel_parameter = parameters_dict[selected_parameter]
    sel_value = values_dict[selected_value]
 
    station_data = get_station_data_from_redis(selected_station, sel_parameter, INPUT_year, INPUT_month)

    if station_data:
        # minv = np.min([station_data[day][selected_value] for day in station_data])
        # maxv = np.max([station_data[day][selected_value] for day in station_data])

        for day in station_data:
            textObjectDict[day].config(state="normal")
            value = station_data[day]
            value = value[sel_value]
            textObjectDict[day].delete("1.0", "end")
            textObjectDict[day].insert("1.0", value)
            # disable editing
            textObjectDict[day].config(state="disabled")

            # color = colors[str(classifyDay(value, minv, maxv))]
            # textObjectDict[day].config(bg=color, fg="white")

        data_label.configure(text="")
    else:
        for day in textObjectDict:
            textObjectDict[day].config(state="normal")
            textObjectDict[day].delete("1.0", "end")
            textObjectDict[day].config(state="disabled")

        data_label.configure(text="Nie znaleziono danych dla wybranej stacji.")

    resize_to_fit()
 
get_data_button = ctk.CTkButton(window, text="Pobierz dane", command=on_get_data)
get_data_button.grid(row=10, column=0, columnspan=2, pady=20)
 
calendarFrame = ctk.CTkFrame(window)
calendarFrame.grid(row=0, column=2, rowspan=11, padx=10, pady=10)

def update_calendar(*args):
    global calendarFrame, month, year
    
    # Get the selected month and year
    if month_var.get() == "" or year_var.get() == "":
        return
    month = int(month_var.get())
    year = int(year_var.get())
    
    # Clear and regenerate the calendar
    calendarFrame.destroy()
    calendarFrame = ctk.CTkFrame(window)
    calendarFrame.grid(row=0, column=2, rowspan=11, padx=10, pady=10)

    textObjectDict.clear() 
    printMonthYear(month, year)
    monthGenerator(dayMonthStarts(month, year), daysInMonth(month, year))

    resize_to_fit()

month_var.trace_add("write", update_calendar)
year_var.trace_add("write", update_calendar)

resize_to_fit()

def on_closing():
    window.destroy()
    window.quit()

window.protocol("WM_DELETE_WINDOW", on_closing)
window.mainloop()

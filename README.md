# Aplikacja do obsługi danych meteorologicznych z zastosowaniem baz NoSQL

## Twórcy:
- Maja Płaciszewska
- Maja Kret
- Karolina Pawla

## Opis programu

Celem ćwiczenia było zaimplementowanie aplikacji do wizualizacji danych z Instytutu Meteorologii i Gospodarki Wodnej, która będzie wykorzystywać funkcjonalność baz NoSQL do celów przechowywania danych i wykonywania analiz. 

## Pliki programu
1.	**main.py** – główny plik aplikacji
2.	**database_connect.py** – moduł łączenia się aplikacji do bazy Redis i MongoDB
3.	**save_to_redis.py** – funkcje do odczytu danych meteorologicznych pobranych z IMGW i zapisu ich do bazy Redis
4.	**save_to_mongodb.py** – funkcje do zapisu danych dla powiatów, województw i stacji pomiarowych do bazy MongoDB

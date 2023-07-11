#Importamos csv para leer el archivo csv
import csv

#Importamos MongoClient para poder conectarnos al Cluster y crear la base de datos y coleccion
from pymongo import MongoClient

#Importamos datetime y timedelta para la creacion del atributo de aired
from datetime import datetime, timedelta

#Extraemos credenciales del archivo config.py
from config import USERNAME, PASSWORD

# Conectamos la base de datos
client = MongoClient(f"mongodb+srv://{USERNAME}:{PASSWORD}@cluster0.mjbi0oc.mongodb.net/Health_2023?retryWrites=true&w=majority")
db = client["PROYECTO"]
collection = db["uwu"]

# Ruta de acceso a CSV
csv_file = r"anime-filtered.csv"

# Leemos el archivo para crear los documentos
with open(csv_file, "r", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for row in reader:
        #Todos los atributos que deseamos almacenar en forma de array les hacemos un split y los separamos al momento de encontrar un ", "
        genres = row["Genres"].split(", ")
        producers = row["Producers"].split(", ")
        licensors = row["Licensors"].split(", ")
        
        #En el caso de aired, lo almacenamos en su formato de string y creamos dos variables para almacenar el start y end
        aired_str = row["Aired"]
        aired_start = None
        aired_end = None

        if aired_str.endswith(" to ?"):
            formato_fecha = "%b %d, %Y"  # Formato de la cadena de fecha
            try: #Intentamos crear los dos atributos donde extraemos la cantidad del string necesario en el formato y lo combinamos 
                aired_start = datetime.strptime(aired_str[:-6], formato_fecha).date()
                aired_start = datetime.combine(aired_start, datetime.min.time())
                aired_end = None
            except ValueError:
                aired_start = None
                aired_end = None
        elif "to" in aired_str: #En caso si tengamos fecha de inicio y final
            formato_fecha = "%b %d, %Y"  # Formato de la cadena de fecha
            aired_dates = aired_str.split(" to ") #Los separamos 
            try: #Creamos ambos valores
                aired_start = datetime.strptime(aired_dates[0], formato_fecha).date()
                aired_start = datetime.combine(aired_start, datetime.min.time())
                aired_end = datetime.strptime(aired_dates[1], formato_fecha).date()
                aired_end = datetime.combine(aired_end, datetime.min.time())
            except ValueError:
                aired_start = None
                aired_end = None
        elif aired_str != "?":
            try:
                aired_start = datetime.strptime(aired_str, "%b %d, %Y").date()
                aired_start = datetime.combine(aired_start, datetime.min.time())
            except ValueError:
                aired_start = None
        else:
            aired_start = None
            aired_end = None

        
        #En caso de ranked almacenamos solo si es un numero entero, caso contrario lo marcamos como None
        ranked = int(row["Ranked"]) if row["Ranked"].isdigit() else None

        #Creamos nuestro documento con todos sus atributos 
        document = {
            "Name": row["Name"],
            "Score": float(row["Score"]),
            "Genres": genres,
            "Type": row["Type"],
            "Episodes": row["Episodes"],
            "Aired": {"start": aired_start, "end": aired_end} if aired_end else aired_start,
            "Premiered": row["Premiered"],
            "Producers": producers,
            "Licensors": licensors,
            "Studios": row["Studios"],
            "Source": row["Source"],
            "Duration": row["Duration"],
            "Rating": row["Rating"],
            "Ranked": ranked,
            "Popularity": int(row["Popularity"]),
            "Members": int(row["Members"]),
            "Favorites": int(row["Favorites"]),
            "Watching": int(row["Watching"]),
            "Completed": int(row["Completed"]),
            "On-Hold": int(row["On-Hold"]),
            "Dropped": int(row["Dropped"]),
        }
        #Insertamos los documentos a la coleccion
        collection.insert_one(document)

print(f"Se llenaron todos los {collection.count_documents({})} documentos")


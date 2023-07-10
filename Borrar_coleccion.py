from pymongo import MongoClient
#Extraemos credenciales del archivo config.py
from config import USERNAME, PASSWORD

# Conectamos la base de datos
client = MongoClient(f"mongodb+srv://{USERNAME}:{PASSWORD}@cluster0.mjbi0oc.mongodb.net/Health_2023?retryWrites=true&w=majority")
db = client["PROYECTO"]
collection = db["uwu"]

result=collection.delete_many({})
print(f"Se han borrado {result.deleted_count} documentos")
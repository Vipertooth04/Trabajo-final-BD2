from pymongo import MongoClient
from tabulate import tabulate
import tkinter as tk
from tkinter import ttk

# Conexión a la base de datos
client = MongoClient(f"mongodb+srv://fernandoperalta:Fercho1597530022@cluster0.mjbi0oc.mongodb.net/Health_2023?retryWrites=true&w=majority")
db = client["Health_2023"]
collection = db["Sleep"]

# Obtener los documentos de la colección
documents = collection.find()

table_data = []

for document in documents:
    row = [
        document.get("Person_ID"),
        document.get("Gender"),
        document.get("Age"),
        document.get("Occupation"),
        document.get("Sleep_Duration"),
        document.get("Quality_of_Sleep"),
        document.get("Physical_Activity_Level"),
        document.get("Stress_Level"),
        document.get("BMI_Category"),
        document.get("Blood_Pressure"),
        document.get("Heart_Rate"),
        document.get("Daily_Steps"),
        document.get("Sleep_Disorder"),
    ]
    table_data.append(row)

table_headers = [
    "Person_ID", "Gender", "Age", "Occupation", "Sleep_Duration", "Quality_of_Sleep",
    "Physical_Activity_Level", "Stress_Level", "BMI_Category", "Blood_Pressure",
    "Heart_Rate", "Daily_Steps", "Sleep_disorder"
]

# Crear ventana
root = tk.Tk()
root.title("Tabla de documentos")
root.geometry("1200x900")

# Crear tabla
table = ttk.Treeview(root, columns=table_headers, show="headings")
table.pack(fill="both", expand=True)

# Configurar encabezados de columna
for header in table_headers:
    table.heading(header, text=header)

# Agregar filas a la tabla
for row in table_data:
    table.insert("", "end", values=row)

root.mainloop()

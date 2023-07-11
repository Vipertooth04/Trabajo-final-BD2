from pymongo import MongoClient
from config import USERNAME, PASSWORD
from py2neo import Graph, Node, Relationship
import itertools

# Conexión a MongoDB
client = MongoClient(f"mongodb+srv://{USERNAME}:{PASSWORD}@cluster0.mjbi0oc.mongodb.net/Health_2023?retryWrites=true&w=majority")
db=client["PROYECTO"]
collection=db["uwu"]

#Query que nos hara el .find en la base de datos
query1 = {
    "Genres":"Action",
    "Score":{"$gt":5},
    "Source":"Manga",
    "Members":{"$gt":10000},
    "Watching":{"$gt":3000}
}
query2 = {"Producers":"Bandai Visual","Source":"Manga","Score":{"$gt":5}}
query3 = {
    "$and": [
        {"Type": {"$eq": "OVA"}},
        {"Studios": {"$regex": "Sunrise"}},
        {"Watching":{"$gte":1000}}
    ]
}
query4={
    "$or":[
        {"$expr":{
            "$lt":[{"$size":"$Genres"},2]
        }},
        {"Genres":"Action"},
        {"Ranked":{"$lte":50}}
    ]
}

#Almacenamos los resultados obtenidos en una variable
results = collection.find(query4).sort("Popularity", 1)

#Limitamos a 30 la cantidad de iteraciones de nuestro nodo
limited_results=itertools.islice(results,5)

# Conexión a Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "12345678"))
#Borramos todos los nodos que ya esten creados en la base de datos de Neo4j
Delete_query="MATCH (n) DETACH DELETE n"
graph.run(Delete_query)
print("Se han borrado todos los nodos")

#Iteramos nuestra creacion de nodos con los resultados que obtuvimos en nuestra consulta
for result in limited_results:
    # Almacenamos nuestro atributo aired en un a variable para poder acceder de manera sencilla a start y end

    aired=result["Aired"]
 
    # Iniciamos la creacion de nuestro nodo anime
    anime_node = Node("Anime",
                        Name=result["Name"], 
                        Score=result["Score"],
                        Episodes=result["Episodes"], 
                        Aired_start=aired["start"] if aired and "start" in aired else None, # En caso exista un start en nuestro documento, se almacena, caso contrario, toma el valor de none
                        Aired_end=aired["end"] if aired and "end" in aired else None,# Lo mismo que en el start
                        Premiered=result["Premiered"],
                        Source=result["Source"],
                        Duration=result["Duration"],
                        Rating=result["Rating"],
                        Ranked=result["Ranked"],
                        Popularity=result["Popularity"],
                        Members=result["Members"],
                        Favorites=result["Favorites"],
                        Watching=result["Watching"],
                        Completed=result["Completed"],
                        On_Hold=result["On-Hold"],
                        Dropped=result["Dropped"],
    )
    graph.create(anime_node)

    #Creacion de nodos de genero
    genres_array=result["Genres"]
    for genre in genres_array:
        Genre_Node = Node("Genres", Name=genre) #Creamos el nodo
        graph.merge(Genre_Node, "Genres", "Name")# Hacemos un .merge para no tener nodos repetidos y facilitar las relaciones
        relationship = Relationship(anime_node, "genero:", Genre_Node)# Creamos una relacion cada ves que se cree el nodo
        graph.create(relationship)

    #Creacion del nodo Tipo
    Type_node = Node("Type", Name=result["Type"]) 
    graph.merge(Type_node, "Type", "Name")
    
    #Creacion del nodo productores
    producers_array=result["Producers"]
    for producer in producers_array:
        Producer_Node = Node("Producers", Name=producer)
        graph.merge(Producer_Node, "Producers", "Name")
        relacion3 =Relationship(anime_node,"Productor:",Producer_Node)
        graph.create(relacion3)


    #Creacion del nodo Licensors
    Licensors_array=result["Licensors"]
    for licensor in Licensors_array:
        Licensor_node = Node("Licensors", Name=licensor)
        graph.merge(Licensor_node, "Licensors", "Name")
        relacion4 = Relationship(anime_node, "Licencia:", Licensor_node)
        graph.create(relacion4)

    #Creacion del nodo Studios
    Studios_node = Node("Studios", Name=result["Studios"])
    graph.merge(Studios_node, "Studios", "Name")
    
    #Relacion entre anime y tipo
    relacion2 = Relationship(anime_node,"Tipo:",Type_node)
    graph.create(relacion2)

    #relacion entre anime y estudios
    relacion5=Relationship(anime_node,"Estudio:",Studios_node)
    graph.create(relacion5)

if collection.count_documents(query1)<1:
    print("En esta consulta no se han encontrado documentos de resultado")
else:
    print("Se crearon los nodos")
#print("consulta para ver toda la base de datos, consultas de cada equipo y que sea automatizada, aprender a manejar mejor los nodos y que cada nodo se controle cuales de los atributos se muestran en los nodos")



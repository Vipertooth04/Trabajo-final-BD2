from pymongo import MongoClient
from config import USERNAME, PASSWORD
from py2neo import Graph, Node, Relationship
import itertools

# Conexión a MongoDB
client = MongoClient(f"mongodb+srv://{USERNAME}:{PASSWORD}@cluster0.mjbi0oc.mongodb.net/Health_2023?retryWrites=true&w=majority")
db=client["PROYECTO"]
collection=db["uwu"]

#Query que nos hara el .find en la base de datos
query = {
    "Genres":{"$eq":"Action"},
    "Score": {"$gt": 8},
    "Source": {"$ne": "Original"}

}

#Almacenamos los resultados obtenidos en una variable
results = collection.find(query).sort("Popularity", 1).limit(5)

#Limitamos a 30 la cantidad de iteraciones de nuestro nodo
limited_results=itertools.islice(results,30)

# Conexión a Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "12345678"))
#Borramos todos los nodos que ya esten creados en la base de datos de Neo4j
Delete_query="MATCH (n) DETACH DELETE n"
graph.run(Delete_query)
print("Se han borrado todos los nodos")

#Iteramos nuestra creacion de nodos con los resultados que obtuvimos en nuestra consulta
for result in limited_results:
    # Almacenamos nuestro atributo aired en una variable para poder acceder de manera sencilla a start y end

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
        Genre_Node = Node("Genre", Name=genre) #Creamos el nodo
        graph.merge(Genre_Node, "Genre", "Name")# Hacemos un .merge para no tener nodos repetidos y facilitar las relaciones
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


print("Se crearon los nodos")
#print("consulta para ver toda la base de datos, consultas de cada equipo y que sea automatizada, aprender a manejar mejor los nodos y que cada nodo se controle cuales de los atributos se muestran en los nodos")



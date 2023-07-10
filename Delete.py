from py2neo import Graph

graph=Graph("bolt://localhost:7687",auth=("neo4j","12345678"))

query="MATCH (n) DETACH DELETE n"
graph.run(query)
print("Se han borrado todos los nodos")
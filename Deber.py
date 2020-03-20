import pymongo
import urllib.request, json

url_data = 'https://api.bsmsa.eu/ext/api/bsm/gbfs/v2/en/station_information'

with urllib.request.urlopen(url_data) as url:
    data = json.loads(url.read().decode())
    data = data.pop('data')
    #data = data.pop('stations')


    # Crear Archivo data.json
data_serialized = json.dump(data,open('data.json','w', encoding='utf-8'),indent=4)


client = pymongo.MongoClient("mongodb+srv://admin:admin@prueba-y8f3r.mongodb.net/test?retryWrites=true&w=majority")
# obtener cliente para postear en la base de datos
db = client.Prueba

#Obtener la coleccion
coleccion = db.coleccion

# enviar datos a la bd de Mongo
#coleccion.insert_many(data)


#buscar documentos de una coleccion o diccionario
results=coleccion.find()



def DatoNumerico():
    print('\n Obtener un dato Numerico en este caso de Capacity \n ')
    for x in coleccion.find({"station_id": 3}, {"capacity": 1}):
        print(x)
DatoNumerico()



def Coordenada():
    print('\n Consultar los atributos de Coordenadas \n ')
    for coor in coleccion.find({} , { "lat" : 1, "lon" : 1}):
        print(coor)
Coordenada()


result = coleccion.aggregate([
        {"$group":
             {"_id":"null",
             "PromedioCapacidad":{ "$avg": "$capacity" }
              }
         }
    ])


def Promedio(result):
    print('\n  Obtener Promedio Total de Capacidad segun el nombre\n ')
    for new in result:
        print(new)

Promedio(result)


#mydoc = coleccion.find({ "stations": { "address":{ "$gt": "C" }}})
#tra=coleccion.find( { 'station_id': { '$lt': 4} } )

def Valores():
    print('\n  Obtener los primeros 3 valores en referencia de station_id\n ')
    cursor = coleccion.find().sort([("station_id", 1)]).limit(3)
    for nuevo in cursor:
        print(nuevo)
Valores()


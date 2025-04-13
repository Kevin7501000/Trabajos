from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017/")

#creamos la base de datos y la coleccion
db = client["escuela"]
estudiantes = db["estudiantes"]

#Insertamos informacion en el documento
estudiantes.insert_many([
    {"nombre": "Julian", "edad": 23, "curso": "Ciencias", "nota": 7.0},
    {"nombre": "Alberto", "edad": 25, "curso": "Quimica", "nota": 6.5},
    {"nombre": "Jose", "edad": 30, "curso": "Matematica", "nota": 8.9},
    {"nombre": "Ana", "edad": 18, "curso": "Ciencias", "nota": 8.5},
    {"nombre": "Fernanda", "edad": 20, "curso": "Quimica", "nota": 5.2},
])

#Buscamos estudiantes con una nota mayor a 8
NotaAlta = estudiantes.find({"nota": {"$gt": 8}})
print("Estudiantes con una nota mayor a 8: ")
for estudiante in NotaAlta:
    print(estudiante)

#Buscamos estudiantes mayores de 20 años
Edad20 = estudiantes.find({"edad": 20})
print("Estudiantes con una edad de 20 años: ")
for estudiante in Edad20:
    print(estudiante)

#Cambiamos la nota de ana a 9.5
estudiantes.update_one({"nombre": "Ana"}, {"$set": {"nota": 9.5}})

#Incrementamos la edad de todos los estudiantes por 1 año
estudiantes.update_many({}, {"$inc": {"edad": 1}})

#Buscamos estudiantes con una nota de 7 a 9 y con una edad mayor a 22
EstudiantesBuscar = estudiantes.find({
    "edad": {"$lt": 22},
    "nota": {"$gte": 7, "$lte": 9},
})
print("Estudiantes con una nota entre 7 a 9 y mayores de 20 años: ")
for estudiante in EstudiantesBuscar:
    print(estudiante)

#Buscamos el promedio de las notas de todos los estudiantes
Promedio = estudiantes.aggregate([{"$group": {"_id": None, "Promedio": {"$avg": "$nota"}}}])
promedio = list(Promedio)[0]["Promedio"]
print("Nota promedio de los estudiantes: ", {promedio})

#Buscamos la nota promedio por cursos
PromedioCursos = estudiantes.aggregate([
    {"$group": {"_id": "$curso", "promedio": {"$avg": "$nota"}}}
])
print("Promedio de notas en cada curso: ")
for curso in PromedioCursos:
    print(f"Curso: {curso['_id']}, Promedio: {curso['promedio']}")

#Creamos un indice en "nota"
estudiantes.create_index("nota")
print("Indice creado en el campo nota")

#Usamos un indice para buscar los estudiantes aprobados (nota igual o mayor a 6)
EstAprobados = estudiantes.find({"nota": {"$gte": 6}})
print("Estudiantes aprobados: ")
for estudiante in EstAprobados:
    print(estudiante)

#Eliminamos los estudiantes con una nota menor a 6
estudiantes.delete_many({"nota": {"$lt": 6}})
print("Se han eliminado los estudiantes con una nota menor a 6")

#Creamos una coleccion cursos
cursos = db["cursos"]

#Insertamos los documentos en la coleccion cursos
cursos.insert_many([
    {"nombre": "Ciencias", "estudiantes": ["Julian", "Ana"]},
    {"nombre": "Quimica", "estudiantes": ["Fernanda", "Alberto"]},
    {"nombre": "Matematica", "estudiantes": ["Jose"]},
])

#Buscamos los cursos donde esta inscrito algun estudiante
Estudiante = "Alberto"
Cursos = cursos.find({"estudiantes": Estudiante})
print("Cursos en los que está inscrito", {Estudiante}, ": ")
for curso in Cursos:
    print(curso["nombre"])

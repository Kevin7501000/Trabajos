from pymongo import MongoClient
from bson import ObjectId
import time

def imprimir_separador(titulo):
    print("\n" + "="*50)
    print(titulo)
    print("="*50)
    
try:
    cliente = MongoClient("mongodb://localhost:27017")
    db = cliente["tiendavirtual"]
    print("Conexion exitosa a MongoDB")
except Exception as e:
    print(f"Error al conectar a MongoDB: {e}")
    exit(1)
    
productos = db["productos"]
pedidos = db["pedidos"]
detalles_pedido = db["detalles_pedido"]

productos.delete_many({})
pedidos.delete_many({})
detalles_pedido.delete_many({})

imprimir_separador("1. insertar un documento")
doc = {
    "nombre": "regadera",
    "precio": 12000,
    "stock": 10
}
resultado = productos.insert_one(doc)
print(f"ID del documento insertado: {resultado.inserted_id}")

imprimir_separador("2. insertar multiples documentos")
nuevos_productos = [
    {"nombre": "tijera", "precio": 8000, "stock": 15},
    {"nombre": "maceta", "precio": 15000, "stock": 20}
]
resultado = productos.insert_many(nuevos_productos)
print(f"IDs de documentos insertados: {resultado.inserted_ids}")

imprimir_separador("3. consultar todos lo documentos")
for producto in productos.find():
    print(producto)
    
imprimir_separador("4. consultar prodcutos con precio mayor a 10000")
for producto in productos.find({"precio": {"$gt": 10000}}):
    print(producto)
    
imprimir_separador("5. consultar un producto especifico")
producto = productos.find_one({"nombre": "maceta"})
print(producto)

imprimir_separador("6. actualizar un documento")
productos.update_one(
    {"nombre": "tijera"},
    {"$set": {"precio": 8500}}
)

imprimir_separador("7. actualizar varios documentos")
resultado = productos.update_many(
    {},
    {"$set": {"disponible": True}}
)
print(f"Cantidad de documentos actualizados {resultado.modified_count}")

imprimir_separador("8. contar documentos")
total = productos.count_documents({})
print(f"Total de productos en la base de datos: {total}")

imprimir_separador("9. Productos ordenados por precio (descendente)")
for producto in productos.find().sort("precio", -1):
    print(productos)
    
imprimir_separador("10. primeros 2 productos")
for producto in productos.find().limit(2):
    print(producto)
    
imprimir_separador("11. crear indice")
indice = productos.create_index("nombre")
print(f"indice creado: {indice}")

imprimir_separador("12. agregacion - productos por rango de precio")
pipeline = [
    {
        "$group": {
            "_id": {
                "$switch": {
                    "branches": [
                        {"case": {"$lt": ["$precio", 10000]}, "then": "economico"},
                        {"case": {"$lt": ["$precio", 15000]}, "then": "medio"},
                    ],
                    "default": "premiun"
                }
            },
            "cantidad": {"$sum": 1},
            "precio_promedio": {"$avg": "$precio"}
        }
    }
]
for resultado in productos.aggregate(pipeline):
    print(resultado)
    
imprimir_separador("13. ejemplo de $lookup (union de colecciones)")
pedido_id = pedidos.insert_one({
    "fecha": "2024-01-20",
    "cliente": "Cliente Ejemplo"
}).inserted_id

detalles_pedido.insert_many([
    {"pedidoId": pedido_id, "producto": "regadera", "cantidad": 1},
    {"pedidoId": pedido_id, "producto": "maceta", "cantidad": 2}    
])

pipeline = [
    {
        "$lookup": {
            "from": "detalles_pedido",
            "localField": "_id",
            "foreignField": "pedidoId",
            "as": "detalles"
        }
    }
]
for pedido in pedidos.aggregate(pipeline):
    print("Pedido completo con sus detalles:")
    print(pedido)
    
imprimir_separador("14. Eliminar un documento")
resultado = productos.delete_one({"nombre": "tijera"})
print(f"Cantidad de productos eliminados {resultado.deleted_count}")

cliente.close()
print("\nDemostracion completa. Conexion cerrada")
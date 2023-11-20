from datetime import datetime

# Fecha en formato de cadena
fecha_cadena = "Wed, 01 Nov 2023 00:00:00 GMT"

# Convertir la cadena a un objeto de fecha
fecha_objeto = datetime.strptime(fecha_cadena, "%a, %d %b %Y %H:%M:%S %Z")

# Formatear la fecha en el formato deseado ('YYYY-MM-DD')
fecha_formateada = fecha_objeto.strftime("%Y-%m-%d")

print(fecha_formateada)

# Imports
# ----------------------------------------------------------
import pandas as pd

# Funciones
# ----------------------------------------------------------
def insertar_datos(cursor, df: pd.DataFrame, table_name: str):
    """
    Función para insertar los datos de un DataFrame en una tabla de una base de datos utilizando un cursor de SQL.

    Args:
        cursor: Objeto de cursor de la base de datos, utilizado para ejecutar las consultas SQL.
        df (pd.DataFrame): DataFrame que contiene los datos a insertar en la tabla.
        table_name (str): Nombre de la tabla en la que se insertarán los datos.

    Proceso:
        1. Genera dinámicamente una consulta SQL de inserción con las columnas del DataFrame.
        2. Itera sobre cada fila del DataFrame y ejecuta la consulta para insertar los valores en la tabla.
        3. Maneja cualquier excepción que ocurra durante el proceso de inserción e imprime el error correspondiente.

    Returns:
        None

    Notas:
        - La tabla debe existir previamente en la base de datos, y las columnas del DataFrame deben coincidir
          con las de la tabla en nombre y tipo.
        - La función utiliza parámetros seguros (`%s`) para prevenir inyecciones SQL.
        - En caso de error, la excepción se eleva después de imprimir el mensaje, permitiendo el manejo en niveles superiores.
    """
    try:
        # Generó la consulta de inserción
        columns = ", ".join(df.columns)
        values = ", ".join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

        # Inserto los datos
        for _, row in df.iterrows():
            cursor.execute(insert_query, tuple(row))
    except Exception as e:
        print(f"Error al insertar datos en la tabla {table_name}: {e}")
        raise
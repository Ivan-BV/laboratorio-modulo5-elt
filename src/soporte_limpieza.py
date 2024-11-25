
# Imports
# ----------------------------------------------------------
import os
import pandas as pd
import numpy as np

# Funciones
# ----------------------------------------------------------
def formatear_datos_ree(ruta: str):
    """
    Función para formatear y consolidar datos de generación eléctrica almacenados en archivos CSV
    en una ruta específica.

    Args:
        ruta (str): Ruta al directorio que contiene los archivos CSV con los datos de generación eléctrica.

    Proceso:
        1. Recorre todos los archivos en el directorio especificado.
        2. Filtra los archivos con extensión ".csv".
        3. Para cada archivo:
           - Lee los datos en un DataFrame de pandas.
           - Añade una columna con el nombre de la provincia basado en el nombre del archivo.
           - Extrae el año del nombre del archivo y lo añade como una columna.
           - Limpia y formatea el nombre de la provincia eliminando el año y la extensión.
           - Reordena las columnas en el orden: ["provincia", "año", "value", "percentage", "datetime", "COD-CCAA"].
        4. Combina todos los DataFrames individuales en uno solo.
        5. Retorna el DataFrame consolidado.

    Returns:
        pandas.DataFrame: DataFrame combinado y formateado con los datos de todos los archivos CSV.

    Notas:
        - Asegúrate de que los archivos CSV en la ruta contengan las columnas requeridas para el reordenamiento.
        - Se espera que los nombres de los archivos sigan el formato: "provinciaYYYY.csv".
    """
    lista_df = []
    for archivo in os.listdir(ruta):
        if archivo.endswith(".csv"):
            ruta_archivo = os.path.join(ruta, archivo)
            df = pd.read_csv(ruta_archivo, index_col=0)
            df["provincia"] = archivo
            df["año"] = df["provincia"].str.extract(r'(\d{4})')
            df["provincia"] = df["provincia"].str.extract(r'^(.+?)(?:\d{4})?\.csv$', expand=False)
            df = df.reindex(columns=["provincia", "año", "value", "percentage", "datetime", "COD-CCAA"])
            lista_df.append(df)
    return pd.concat(lista_df, ignore_index=True)

def estandarizar_provincias(df: pd.DataFrame, columna_provincia: str):
    """
    Función para estandarizar los nombres de las provincias en una columna de un DataFrame.

    Args:
        df (pd.DataFrame): DataFrame que contiene los datos a procesar.
        columna_provincia (str): Nombre de la columna en el DataFrame que contiene los nombres de las provincias.

    Proceso:
        1. Elimina los espacios en blanco al inicio y al final de los valores en la columna especificada.
        2. Convierte los valores a mayúsculas para garantizar la uniformidad.

    Returns:
        pd.DataFrame: DataFrame con los nombres de las provincias estandarizados.

    Notas:
        - Esta función es útil para garantizar la consistencia en los nombres de las provincias antes de realizar análisis o combinaciones de datos.
        - No modifica otras columnas del DataFrame.
    """
    df[columna_provincia] = df[columna_provincia].str.strip().str.upper()
    return df

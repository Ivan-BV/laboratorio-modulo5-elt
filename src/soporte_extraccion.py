
# Imports api
# ----------------------------------------------------------
import requests
import pandas as pd
import os
from tqdm import tqdm

# Imports web scraping
# ----------------------------------------------------------
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from time import sleep
from selenium import webdriver
import os

# Funciones
# ----------------------------------------------------------
def extraer_datos_ree_demanda(ruta_ree_demanda, headers, lista_anios):
    """
    Función para extraer y guardar en formato CSV los datos de la evolución de la demanda eléctrica
    por comunidad autónoma desde la API de REE.

    Args:
        ruta_ree_demanda (str): Ruta donde se almacenarán los archivos CSV generados.
        headers (dict): Diccionario con los encabezados necesarios para realizar las solicitudes a la API.
        lista_anios (list): Lista de años (int o str) para los cuales se desean extraer los datos.

    Proceso:
        1. Recorre cada año de `lista_anios`.
        2. Para cada comunidad autónoma, genera la URL con los parámetros correspondientes.
        3. Realiza la solicitud a la API de REE.
        4. Si la respuesta es exitosa (status 200):
           - Convierte los datos JSON en un DataFrame de pandas.
           - Añade una columna con el código de la comunidad autónoma.
           - Guarda los datos en un archivo CSV en la ruta especificada.
        5. Si la respuesta no es exitosa, muestra un mensaje con el código de error y la comunidad afectada.

    Returns:
        None
    """
    for anio in lista_anios:
        for name, cod in tqdm(cod_comunidades.items()):
            url_demanda_electrica = f"https://apidatos.ree.es/es/datos/demanda/evolucion?start_date={anio}-01-01T00:00&end_date={anio}-12-31T23:59&time_trunc=month&geo_trunk=electric_system&geo_limit=ccaa&geo_ids={cod}"

            response = requests.get(url_demanda_electrica, headers=headers)
            if response.status_code == 200:
                response_json= response.json()
                df_demanda_electrica= pd.DataFrame(response_json["included"][0]["attributes"]["values"])
                df_demanda_electrica["COD-CCAA"] = cod
                df_demanda_electrica.to_csv(os.path.join(ruta_ree_demanda, f"{name}{anio}.csv"))
                print("Guardado correctamente en csv")
            else: 
                print(f"El error es {response.status_code} en {name} para el annio {anio}")

def extraer_datos_ree_generacion(ruta_ree_generacion, headers, lista_anios):
    """
    Función para extraer y guardar en formato CSV los datos de generación de energía renovable 
    por comunidad autónoma desde la API de REE.

    Args:
        ruta_ree_generacion (str): Ruta donde se almacenarán los archivos CSV generados.
        headers (dict): Diccionario con los encabezados necesarios para realizar las solicitudes a la API.
        lista_anios (list): Lista de años (int o str) para los cuales se desean extraer los datos.

    Proceso:
        1. Recorre cada año de `lista_anios`.
        2. Para cada comunidad autónoma, genera la URL con los parámetros correspondientes.
        3. Realiza la solicitud a la API de REE.
        4. Si la respuesta es exitosa (status 200):
           - Itera sobre los elementos de datos incluidos en la respuesta.
           - Convierte cada conjunto de datos en un DataFrame de pandas.
           - Añade columnas con el tipo de generación y el código de la comunidad autónoma.
           - Agrega cada DataFrame a una lista.
           - Combina todos los DataFrames en uno solo y lo guarda en un archivo CSV en la ruta especificada.
        5. Si la respuesta no es exitosa, muestra un mensaje con el código de error y la comunidad afectada.

    Returns:
        None
    """
    for anio in lista_anios:
        for name, cod in tqdm(cod_comunidades.items()):
            url_generacion_renovable = f"https://apidatos.ree.es/es/datos/generacion/estructura-renovables?start_date={anio}-01-01T00:00&end_date={anio}-12-31T23:59&time_trunc=month&geo_trunk=electric_system&geo_limit=ccaa&geo_ids={cod}"

            response_generacion = requests.get(url_generacion_renovable, headers=headers)
            if response_generacion.status_code == 200:
                response_generacion_json = response_generacion.json()
                lista_df = []
                for j in range(len(response_generacion_json["included"])):
                    df_generacion_estructura = pd.DataFrame(response_generacion_json["included"][j]["attributes"]["values"])
                    df_generacion_estructura["type"] = response_generacion_json["included"][j]["type"]
                    df_generacion_estructura["COD-CCAA"] = cod
                    lista_df.append(df_generacion_estructura)
                df_final = pd.concat(lista_df)
                df_final.to_csv(os.path.join(ruta_ree_generacion, f"{name}{anio}.csv"))
                print("Guardado correctamente en csv")
            else:
                print(f"Error {response_generacion.status_code} en {name} para el año {anio}")

def extraer_datos_ine_demograficos(url, chrome_options):
    """
    Función para automatizar la extracción de datos demográficos desde la web del INE utilizando Selenium.

    Args:
        url (str): URL de la página del INE desde donde se extraerán los datos.
        chrome_options (webdriver.ChromeOptions): Configuración de opciones para el navegador Chrome.

    Proceso:
        1. Inicia un navegador Chrome con las opciones proporcionadas.
        2. Navega a la URL especificada y acepta las cookies.
        3. Realiza interacciones con la interfaz web para seleccionar opciones en las siguientes categorías:
           - Provincias.
           - Edades.
           - Nacionalidad (extranjeros).
           - Género.
           - Periodos de tiempo.
        4. Genera una consulta con las opciones seleccionadas.
        5. Descarga los datos en formato CSV desde el iframe correspondiente.
        6. Cierra el navegador.

    Returns:
        None

    Notas:
        - Esta función utiliza tiempos de espera explícitos e implícitos para garantizar que los elementos estén disponibles antes de interactuar con ellos.
        - Las opciones seleccionadas se iteran dinámicamente, adaptándose a la cantidad de opciones en cada categoría.
        - Es necesario que la configuración de Selenium permita descargas automáticas y que el entorno soporte la ejecución del navegador Chrome.
    """
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    driver.maximize_window()
    driver.implicitly_wait(5)
    sleep(1)
    driver.find_element("css selector", "#aceptarCookie").click()
    driver.find_element("xpath", '/html/body/div[1]/main/section[2]/div[1]/div[1]/div[1]/ul/li/ul/li[3]/a').click()
    sleep(1)
    driver.find_element("xpath", "/html/body/div/main/div[2]/ul/li[4]/ul/li[1]/a").click()
    sleep(1)
    driver.find_element("xpath", "/html/body/div[1]/main/form/ul/li[1]/ul/li[1]/div/fieldset/div[2]/button[2]/i").click()
    provincias = driver.find_elements("css selector", "#cri0")
    cant_provincias = provincias[0].find_elements(By.TAG_NAME, "option")

    for n in range(2, len(cant_provincias) + 1):
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, f'//*[@id="cri0"]/option[{n}]'))).click()
        sleep(0.2)

    driver.find_element("xpath", "/html/body/div[1]/main/form/ul/li[1]/ul/li[2]/div/fieldset/div[2]/button[2]/i").click()
    edades = driver.find_elements("css selector", "#cri1")
    cant_edades = edades[0].find_elements(By.TAG_NAME, "option")

    for n in range(2, len(cant_edades) + 1):
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, f'//*[@id="cri1"]/option[{n}]'))).click()
        sleep(0.2)

    driver.find_element("xpath", "/html/body/div/main/form/ul/li[1]/ul/li[3]/div/fieldset/div[2]/button[2]/i").click()
    extranjeros = driver.find_elements("css selector", "#cri2")
    cant_extranjeros = extranjeros[0].find_elements(By.TAG_NAME, "option")

    for n in range(2, len(cant_extranjeros)):
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, f'//*[@id="cri2"]/option[{n}]'))).click()
        sleep(0.2)

    driver.find_element("xpath", "/html/body/div/main/form/ul/li[1]/ul/li[4]/div/fieldset/div[2]/button[2]/i").click()
    generos = driver.find_elements("css selector", "#cri3")
    cant_generos = generos[0].find_elements(By.TAG_NAME, "option")

    for n in range(2, len(cant_generos) + 1):
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, f'//*[@id="cri3"]/option[{n}]'))).click()
        sleep(0.2)

    driver.find_element("xpath", "/html/body/div[1]/main/form/ul/li[1]/ul/li[5]/div/fieldset/div[3]/button[2]/i").click()

    for n in range(2, 5):
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, f'//*[@id="periodo"]/option[{n}]'))).click()
        sleep(0.2)

    # Forma para seleccionar el iframe para descargar csv
    driver.find_element("css selector", "#botonConsulSele").click()
    sleep(1)
    driver.find_element("xpath", "/html/body/div[1]/main/ul/li/div/div/form[2]/button/i").click()
    iframe = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="thickBoxINEfrm"]')))
    driver.switch_to.frame(iframe)
    driver.find_element("xpath", "/html/body/form/ul/li[4]/label").click()
    sleep(1)
    driver.switch_to.default_content()
    sleep(1)
    driver.close()
    print("Datos extraidos correctamente")

def extraer_datos_ine_economicos(url, chrome_options):
    """
    Función para automatizar la extracción de datos económicos desde la web del INE utilizando Selenium.

    Args:
        url (str): URL de la página del INE desde donde se extraerán los datos.
        chrome_options (webdriver.ChromeOptions): Configuración de opciones para el navegador Chrome.

    Proceso:
        1. Inicia un navegador Chrome con las opciones proporcionadas.
        2. Navega a la URL especificada y acepta las cookies.
        3. Realiza interacciones con la interfaz web para seleccionar opciones en las siguientes categorías:
           - Actividades económicas.
           - Períodos de tiempo.
        4. Genera una consulta con las opciones seleccionadas.
        5. Descarga los datos en formato CSV desde el iframe correspondiente.
        6. Cierra el navegador.

    Returns:
        None

    Notas:
        - Utiliza tiempos de espera explícitos e implícitos para garantizar que los elementos estén disponibles antes de interactuar con ellos.
        - Las opciones seleccionadas se iteran dinámicamente, adaptándose a la cantidad de opciones en cada categoría.
        - Es necesario que la configuración de Selenium permita descargas automáticas y que el entorno soporte la ejecución del navegador Chrome.
        - Asegúrate de que los permisos para las descargas y accesos al sistema estén configurados correctamente para guardar el archivo.
    """
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    driver.maximize_window()
    driver.implicitly_wait(5)
    sleep(1)
    driver.find_element("css selector", "#aceptarCookie").click()
    driver.find_element("xpath", '/html/body/div[1]/main/div[2]/ul/li[3]/ul/li[1]/a').click()
    driver.find_element("xpath", "/html/body/div[1]/main/form/ul/li[1]/ul/li[2]/div/fieldset/div[2]/button[2]/i").click()
    actividades = driver.find_elements("css selector", "#cri1")
    cant_actividades = actividades[0].find_elements(By.TAG_NAME, "option")

    for n in range(2, len(cant_actividades)):
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, f'//*[@id="cri1"]/option[{n}]'))).click()
        sleep(0.2)

    driver.find_element("xpath", "/html/body/div[1]/main/form/ul/li[1]/ul/li[3]/div/fieldset/div[3]/button[2]/i").click()
    
    for n in range(1, 4):
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, f'//*[@id="periodo"]/option[{n}]'))).click()
        sleep(0.5)

    driver.find_element("css selector", "#botonConsulSele").click()
    sleep(1)

    # Forma para seleccionar el iframe para descargar csv
    driver.find_element("xpath", "/html/body/div[1]/main/ul[1]/li/div/div/form[2]/button").click()
    iframe = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="thickBoxINEfrm"]')))
    driver.switch_to.frame(iframe)
    sleep(1)
    driver.find_element("xpath", "/html/body/form/ul/li[4]/label").click()
    sleep(1)
    driver.switch_to.default_content()
    sleep(1)
    driver.close()
    print("Datos extraidos correctamente")

# Variables
# ----------------------------------------------------------
cod_comunidades = {'Ceuta': 8744,
                    'Melilla': 8745,
                    'Andalucía': 4,
                    'Aragón': 5,
                    'Cantabria': 6,
                    'Castilla - La Mancha': 7,
                    'Castilla y León': 8,
                    'Cataluña': 9,
                    'País Vasco': 10,
                    'Principado de Asturias': 11,
                    'Comunidad de Madrid': 13,
                    'Comunidad Foral de Navarra': 14,
                    'Comunitat Valenciana': 15,
                    'Extremadura': 16,
                    'Galicia': 17,
                    'Illes Balears': 8743,
                    'Canarias': 8742,
                    'Región de Murcia': 21,
                    'La Rioja': 20}

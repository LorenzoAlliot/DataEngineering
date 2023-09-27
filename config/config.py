import configparser

# Función para obtener los datos de conexión desde un archivo remoto
def read_config():
    ruta_archivo = '/opt/airflow/config/apikey.ini'  # Ruta completa al archivo
    
    config = configparser.ConfigParser()
    
    # Usar el método read con la ruta del archivo
    config.read(ruta_archivo)
    # config.read_string('G:/Mi unidad/DataEngenieering Coderhouse/ProyectoFinal/config/apikey.ini')
    
    return config
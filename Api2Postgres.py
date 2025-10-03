import pandas as pd
from CAT_Conexions.src.conexions import apiSagedCAT, pgDataLake
from CAT_Conexions.src.Config.BaseConfig import IOT_CORE_IP, IOT_CORE_TOKEN_DIC
from datetime import datetime, timedelta
import logging

# Configuración de logs
start = datetime.now()
logging.basicConfig(
    format='[%(asctime)s] %(filename)-15s | %(levelname)-9s| %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S',
    filename=f'./logs/api2postgres_{start.strftime("%m-%d-%Y")}.log',
    level=logging.INFO
)
logger = logging.getLogger('Api2Postgres')

# Parámetros comunes
nom_vista = "Temps de funcionament i actuacions"
token = IOT_CORE_TOKEN_DIC.get('API_CAT')
headers = {
    'nexustoken': token,
    'Content-Type': 'application/json'
}
URL = IOT_CORE_IP

logger.info(f"Parámetros definidos: vista={nom_vista}, URL={URL}")

# Rango de fechas
fecha_fin = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
fecha_inicio = fecha_fin - timedelta(days=5)


logger.info(f"Rango de fechas definido: desde {fecha_inicio} hasta {fecha_fin}")

# Inicializar API y vistas
try:
    api = apiSagedCAT(nom_vista, 'tags')
    logger.info("API inicializada")
    vistas_dict = api.get_vistas(wanted_vistas=nom_vista)
    logger.info(f"Vistas obtenidas: {list(vistas_dict.keys())}")
except Exception as e:
    logger.exception(f"Error al inicializar la API o al obtener las vistas: {e}")
    raise


# Función actualizada para procesar e insertar datos sin pasar tags manualmente
def procesar_e_insertar(resolucion, nombre_tabla):
    try:
        logger.info(f"Iniciando procesamiento para tabla '{nombre_tabla}' con resolución '{resolucion}'")

        num = resolucion.split("_")[1]
        tags = api.get_column_names_from_view(vistas_dict[nom_vista],headers)
        df = api.get_data(vistas_dict[nom_vista], URL, headers, num, resolucion, tags, fecha_inicio, fecha_fin)
        logger.info(f"Datos obtenidos: {len(df)} registros")

        df_reset = df.reset_index().rename(columns={'fecha': 'data'})

        # Transformar a formato largo
        df_long = df_reset.melt(id_vars='data', var_name='tag', value_name='valor')
        
        
        # Convertir la columna 'valor' a tipo numérico (double)
        df_long['valor'] = pd.to_numeric(df_long['valor'], errors='coerce')


        # Obtener claves existentes desde la base de datos
        pg = pgDataLake()
        df_existentes = pg.DB_query_data(nombre_tabla, schema="ga_landing")

        claves_existentes = df_existentes[['data', 'tag']]
        df_filtrado = df_long.merge(claves_existentes, on=['data', 'tag'], how='left', indicator=True)
        df_nuevos = df_filtrado[df_filtrado['_merge'] == 'left_only'].drop(columns=['_merge'])

        logger.info(f"Filtrado completado: {len(df_nuevos)} registros nuevos a insertar")

        if not df_nuevos.empty:
            pg.insert_dataframe(table_name=nombre_tabla, schema="ga_landing", df=df_nuevos)
            logger.info(f"Datos insertados correctamente en la tabla 'ga_landing.{nombre_tabla}'")
            print(f"[CORRECTO] Datos insertados correctamente en la tabla 'ga_landing.{nombre_tabla}'")
        else:
            logger.info(f"No hay datos nuevos para insertar en 'ga_landing.{nombre_tabla}'")
            print(f"[INFO] No hay datos nuevos para insertar en 'ga_landing.{nombre_tabla}'")
    except Exception as e:
        logger.exception(f"Error durante el procesamiento o inserción de datos para la tabla '{nombre_tabla}': {e}")

# Bloque principal actualizado
try:
    resolucion_produccion = "RES_1_DAY"
    procesar_e_insertar(resolucion_produccion, "ite_tfaact_tempsinumactuacions")
except Exception as e:
    logger.exception(f"Error en el bloque de producción solar: {e}")

logger.info("Script finalizado correctamente")




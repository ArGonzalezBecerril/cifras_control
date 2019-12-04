import os
import fuente.dao.impala.DaoExtraccionProp as daoProp
from pyspark.sql.types import *
import sys
import datetime as fec

reload(sys)
sys.setdefaultencoding('utf-8')


def obt_ruta(nom_directorio, nom_archivo):
    if sys.platform != 'win32':
        if nom_directorio == '':
            directorio = nom_archivo
        else:
            directorio = nom_directorio + "/" + nom_archivo
    else:
        if nom_directorio == '':
            directorio = nom_archivo
        else:
            directorio = nom_directorio + "\\" + nom_archivo
    return directorio


def obten_diccionario_de_tablas():
    path_actual = os.path.dirname(os.path.abspath(__file__))
    dir_txt_conn_impala = 'configuracion/grupo_tablas.properties'
    txt_cnx_impala = obt_ruta(path_actual.replace('/util', ''), dir_txt_conn_impala)

    dao_grupo_tablas = daoProp.DaoExtraccionProp(txt_cnx_impala, 'catalogo_de_tablas')
    dict_tablas_impala = dao_grupo_tablas.obten()
    return dict_tablas_impala


def obten_dict_de_conexion_a_impala():
    path_actual = os.path.dirname(os.path.abspath(__file__))
    txt_cnx_impala = obt_ruta(path_actual.replace('/util', '/configuracion'), 'conexion_impala.properties')
    dao_extrac_prop = daoProp.DaoExtraccionProp(txt_cnx_impala, 'conexion_local')
    return dao_extrac_prop.obten()


def tipo_equivalente(tipo_de_formato):
    if tipo_de_formato == 'datetime64[ns]':
        return DateType()
    elif tipo_de_formato == 'int64':
        return LongType()
    elif tipo_de_formato == 'int32':
        return IntegerType()
    elif tipo_de_formato == 'float64':
        return FloatType()
    else:
        return StringType()


def define_estructura(cadena, tipo_formato):
    try:
        tipo = tipo_equivalente(tipo_formato)
    except:
        tipo = StringType()
    return StructField(cadena, tipo)


def pandas_a_spark(sql_context, pandas_df):
    columnas = list()
    [columnas.append(columna.upper().strip()) for columna in pandas_df.columns]

    tipos = list(pandas_df.dtypes)
    estructura_del_esquema = []
    for columna, tipo in zip(columnas, tipos):
        estructura_del_esquema.append(define_estructura(columna, tipo))
    esquema = StructType(estructura_del_esquema)
    return sql_context.createDataFrame(pandas_df, esquema)


def remueve_carac_especiales(dataframe, caracteres=',|\\t|\\n|\\r|\\|\\"|\\/|\"'):
    df_sin_caracteres_esp = dataframe.replace(caracteres, "", regex=True)
    return df_sin_caracteres_esp


def obt_fecha_actual():
    fecha_hoy = fec.date.today()
    fecha_con_formato = fecha_hoy.strftime("%d_%m_%Y")
    return str(fecha_con_formato)
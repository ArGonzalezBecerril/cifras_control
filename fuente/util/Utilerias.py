import sys
import os
import fuente.dao.impala.DaoExtraccionProp as daoProp


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
    print(txt_cnx_impala)
    dao_extrac_prop = daoProp.DaoExtraccionProp(txt_cnx_impala, 'conexion_local')
    return dao_extrac_prop

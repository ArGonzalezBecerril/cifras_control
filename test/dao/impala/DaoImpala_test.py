import pytest
import os
import fuente.util.Utilerias as Util
import fuente.util.Excepciones as Exp
import fuente.fabrica.ConexionImpala as cnxImp
import fuente.dao.impala.DaoImpala as daoImp
import fuente.dao.impala.DaoExtraccionProp as daoProp

dir_actual = os.path.dirname(os.path.abspath(__file__))
txt_cnx_impala = Util.obt_ruta(dir_actual, 'conexion_impala.properties')
dao_extrac_prop = daoProp.DaoExtraccionProp(txt_cnx_impala, 'conexion_local')


'''
@Author : Arturo Gonzalez B.
@email  : arturo.gonzalez@tusventasdigitales.com
@version: 0.1
'''


def setup_module(module):
    with open(txt_cnx_impala, 'w+') as fichero:
        fichero.write('[conexion_local]\n')
        fichero.write('hdfs_host=localhost\n')
        fichero.write('hdfs_puerto=50070\n')
        fichero.write('impala_host=localhost\n')
        fichero.write('impala_puerto=21050\n')


def teardown_module(module):
    if os.path.exists(txt_cnx_impala):
        print("Se elimino el siguiente archivo de test:" + txt_cnx_impala)
        os.remove(txt_cnx_impala)
    else:
        print("El fichero de test:" + txt_cnx_impala + " no existe por lo tanto no se ha eliminado")


'''
@Paquete: fuente.dao.impala.DaoImpala
@Clase:   DaoImpala
'''


@pytest.mark.parametrize('obj_cnx_impala, esquema, nom_tabla', [('', '', ''),
                                                                (None, None, None),
                                                                ('', None, '')])
def test_valida_instancia(obj_cnx_impala, esquema, nom_tabla):
    with pytest.raises(Exp.ObjetoNoValido):
        daoImp.DaoImpala(obj_cnx_impala, esquema, nom_tabla)


def test_listado_tables_de_bd():   # Return to list
    dict_cnx_impala = dao_extrac_prop.obten()
    obj_conexion = cnxImp.ConexionImpala(dict_cnx_impala)
    dao_impala = daoImp.DaoImpala(obj_conexion, 'default', 'impala_orders')
    listado_de_tablas = dao_impala.listado_tablas_de_bd()
    lista_tablas_esperadas = ['impala_orders']

    assert listado_de_tablas == lista_tablas_esperadas, "Los grupos de tablas son distintos, \n" \
                                                        "Esperado:" + str(lista_tablas_esperadas) + " \n" \
                                                        "Fuente:" + str(listado_de_tablas)


def test_obten_total_de_archivos():  # Return a pandas dataframe
    dict_cnx_impala = dao_extrac_prop.obten()
    obj_conexion = cnxImp.ConexionImpala(dict_cnx_impala)
    dao_impala = daoImp.DaoImpala(obj_conexion, 'default', 'impala_orders')
    total_de_buckets = dao_impala.obten_total_de_archivos()
    assert not total_de_buckets.empty, "No se obtuvo ningun resultado"


def test_obten_estadisticas_de_tabla():  # Return a data frame of pandas
    dict_cnx_impala = dao_extrac_prop.obten()
    obj_conexion = cnxImp.ConexionImpala(dict_cnx_impala)
    dao_impala = daoImp.DaoImpala(obj_conexion, 'default', 'impala_orders')
    estadisticas_de_tabla = dao_impala.obten_estadisticas_de_tabla()
    columnas_esperadas = ['#Rows', '#Files', 'Size',
                          'Bytes Cached', 'Cache Replication',
                          'Format', 'Incremental stats', 'Location']
    assert estadisticas_de_tabla.columns.tolist() == columnas_esperadas, 'El dataframe de resultado no es el esperado'


def test_obten_estadistica_de_columna():  # Return a pandas dataframe
    dict_cnx_impala = dao_extrac_prop.obten()
    obj_conexion = cnxImp.ConexionImpala(dict_cnx_impala)
    dao_impala = daoImp.DaoImpala(obj_conexion, 'default', 'impala_orders')
    estadistica_de_columna = dao_impala.obten_estadisticas_de_columna()
    columnas_esperadas = ['Column', 'Type', '#Distinct Values', '#Nulls', 'Max Size', 'Avg Size']
    assert estadistica_de_columna.columns.tolist() == columnas_esperadas, 'El resultado de estadistica de columnas' \
                                                                          ' no es el esperado.'


def test_esta_particionada_la_tabla():  # Return a boolean value
    dict_cnx_impala = dao_extrac_prop.obten()
    obj_conexion = cnxImp.ConexionImpala(dict_cnx_impala)
    dao_impala = daoImp.DaoImpala(obj_conexion, 'default', 'impala_orders')
    es_tabla_particionada = dao_impala.esta_particionada_la_tabla()
    assert not es_tabla_particionada, "El resultado correcto, no es el esperado"

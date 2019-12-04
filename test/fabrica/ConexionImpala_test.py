import pytest
import os
import fuente.util.Utilerias as Util
import fuente.util.Excepciones as Exp
import fuente.fabrica.ConexionImpala as cnxImp
import fuente.dao.impala.DaoExtraccionProp as daoProp

dir_actual = os.path.dirname(os.path.abspath(__file__))
txt_cnx_impala = Util.obt_ruta(dir_actual, 'conexion_impala.properties')


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
@Paquete: fuente.fabrica.ConexionImpala
@Clase:   ConexionImpala
'''


@pytest.mark.parametrize('dict_datos_cnx', [{}])
def test_valida_instancia(dict_datos_cnx):
    with pytest.raises(Exp.DiccionarioVacio):
        cnxImp.ConexionImpala(dict_datos_cnx=dict_datos_cnx)


@pytest.mark.parametrize('nom_archivo, nom_seccion', [(txt_cnx_impala, 'conexion_local')])
def test_obten(nom_archivo, nom_seccion):
    dao_extrac_prop = daoProp.DaoExtraccionProp(nom_archivo, nom_seccion)
    dict_cnx_impala = dao_extrac_prop.obten()
    obj_conexion = cnxImp.ConexionImpala(dict_cnx_impala)
    conexion_impala = obj_conexion.obten()

    assert conexion_impala is not None, "Errores al tratar de conecatarse a impala"

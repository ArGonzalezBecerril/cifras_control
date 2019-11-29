from abc import ABCMeta, abstractmethod
import fuente.util.Asserciones as Asercion
import ibis

'''
@Author : Arturo Gonzalez B.
@email  : arturo.gonzalez@tusventasdigitales.com
@version: 0.1
'''


class ConexionImpalaAbs(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def obten(self):
        pass


class ConexionImpala(ConexionImpalaAbs):
    def __init__(self, dict_datos_cnx):
        self.valida_diccionario(dict_datos_cnx)
        self.valida_parametros(dict_datos_cnx)
        self.hdfs_host = dict_datos_cnx['hdfs_host']
        self.hdfs_puerto = int(dict_datos_cnx['hdfs_puerto'])
        self.impala_host = dict_datos_cnx['impala_host']
        self.impala_puerto = int(dict_datos_cnx['impala_puerto'])

    def valida_diccionario(self, dict_datos_conexion):
        Asercion.no_es_diccinario_vacio(
            dict_datos_conexion,
            '\n*Causa: El diccionario de conexion a impala esta vacio'
            '\n*Accion: Revise que el *archivo conexion_impala* no este vacio y que el objeto que accede a dicho '
            'fichero este accediendo a los parametros de conexion')

    def valida_parametros(self, dict_datos_cnx):
        atributos_de_conexion = dict_datos_cnx.items()
        for nom_atributo, valor in atributos_de_conexion:
            Asercion.no_es_cadena_vacia(
                valor,
                '\n*Causa: El atributo' + nom_atributo + ' esta vacio'
                '\n*Accion: Revise que el *archivo conexion_impala* no este vacio y que el objeto que accede a dicho '
                'fichero este accediendo a los parametros de conexion')

    def obten(self):
        cnx_hdfs = ibis.hdfs_connect(host=self.hdfs_host, port=self.hdfs_puerto)
        conexion_impala = ibis.impala.connect(
            host=self.impala_host,
            port=self.impala_puerto,
            hdfs_client=cnx_hdfs,
        )
        return conexion_impala

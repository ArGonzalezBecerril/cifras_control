import fuente.util.Asserciones as Asercion
from abc import ABCMeta, abstractmethod
import fuente.fabrica.ConexionImpala as cnxImp
import fuente.util.Utilerias as Util
import fuente.dao.impala.DaoImpala as daoImp
import pyspark.sql as pysql
import fuente.util.LoggerImpl as Log
from pyspark.sql.functions import lit

'''
@Author : Arturo Gonzalez B.
@email  : arturo.gonzalez@tusventasdigitales.com
@version: 0.1
'''


class EtlEstadisticaImpalaAbs:
    __metaclass__ = ABCMeta

    @abstractmethod
    def extrae(self):
        pass

    @abstractmethod
    def transforma(self):
        pass

    @abstractmethod
    def carga(self):
        pass


class EtlEstadisticaImpala(EtlEstadisticaImpalaAbs):
    def __init__(self, context, esquema, nom_tabla):
        self.esquema = esquema
        self.nom_tabla = nom_tabla
        self.spark_context = context
        self.sql_context = pysql.SQLContext(context)
        self.valida_parametros()

        self.dao_impala = None

    def valida_parametros(self):
        Asercion.no_es_cadena_vacia(
            self.sql_context,
            '\n*Causa: El contexto de spark esta vacio(nulo)'
            '\n*Accion: Revise que el objeto sql_context no este vacio e intente imprimir el valor del objeto')
        Asercion.no_es_cadena_vacia(
            self.esquema,
            '\n*Causa: El atributo *esquema* esta vacio'
            '\n*Accion: Revise que el archivo de configuracion tenga el *esquema* y que la clase DaoExtraccionProp'
            ' este extrayendo el valor')
        Asercion.no_es_cadena_vacia(
            self.nom_tabla,
            '\n*Causa: El contexto de spark esta vacio(nulo)'
            '\n*Accion: Revise que el objeto sql_context no este vacio e intente imprimir el valor del objeto')

    @Log.logger('Extraccion')
    def extrae(self):
        dict_conexion_impala = Util.obten_dict_de_conexion_a_impala()
        obj_cnx_impala = cnxImp.ConexionImpala(dict_conexion_impala)
        self.dao_impala = daoImp.DaoImpala(obj_cnx_impala, self.esquema, self.nom_tabla)

    @Log.logger('Transformacion')
    def transforma(self):
        total_de_archivos = self.dao_impala.obten_total_de_archivos()
        df_total_de_archivos = Util.pandas_a_spark(self.sql_context, total_de_archivos)
        df_estadistica_de_tabla = Util.pandas_a_spark(self.sql_context, self.dao_impala.obten_estadisticas_de_tabla())
        df_estadistica_de_columna = Util.pandas_a_spark(self.sql_context, self.dao_impala.obten_estadisticas_de_columna())

        df_descripcion_tabla = df_estadistica_de_tabla.\
            selectExpr('LOCATION', 'SIZE').\
            withColumn("NOMBRE_TABLA", lit(self.nom_tabla)).\
            withColumn("PESO", lit(self.esquema))

        df_descripcion_tabla.show(10, False)
        df_estadistica_de_columna.show()

    @Log.logger('Carga')
    def carga(self):
        pass

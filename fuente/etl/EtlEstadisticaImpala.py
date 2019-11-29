import fuente.util.Asserciones as Asercion
from abc import ABCMeta, abstractmethod
import fuente.dao.impala.DaoExtraccionProp as daoProp
import os
import fuente.util.Utilerias as Util


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
    def __init__(self, sql_context):
        self.sql_context = sql_context
        self.valida_parametros()
        path_actual = os.path.dirname(os.path.abspath(__file__))
        self.txt_cnx_impala = Util.obt_ruta(path_actual.replace('/etl', ''), 'configuracion/conexion_impala.properties')

    def valida_parametros(self):
        Asercion.no_es_cadena_vacia(
            self.sql_context,
            '\n*Causa: El contexto de spark esta vacio(nulo)'
            '\n*Accion: Revise que el objeto sql_context no este vacio e intente imprimir el valor del objeto')

    def extrae(self):
        dao_extrac_prop = daoProp.DaoExtraccionProp(self.txt_cnx_impala, 'conexion_local')

    def transforma(self):
        pass

    def carga(self):
        pass
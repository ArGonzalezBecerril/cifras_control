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
    def __init__(self, sql_context, esquema, nom_tabla):
        self.esquema = esquema
        self.nom_tabla = nom_tabla
        self.sql_context = sql_context
        self.valida_parametros()

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

    def extrae(self):
        dict_conexion_impala = Util.obten_dict_de_conexion_a_impala()
        print(dict_conexion_impala.obten())

    def transforma(self):
        pass

    def carga(self):
        pass
import fuente.dao.AdministradorDao as Dao
import fuente.util.Asserciones as Asercion
import configparser

'''
@Author : Arturo Gonzalez B.
@email  : arturo.gonzalez@tusventasdigitales.com
@version: 0.1
'''


class DaoExtraccionProp(Dao.DaoExtraccionPropAbc):
    def __init__(self, nom_archivo, nom_seccion):
        self.nom_archivo = nom_archivo
        self.nom_seccion = nom_seccion
        self.valida_parametros()
        self._config = configparser.RawConfigParser()
        self.propiedades = None

    def valida_parametros(self):
        Asercion.no_es_cadena_vacia(
            self.nom_archivo,
            '\n*Causa: El archivo que intenta abrir no existe'
            '\n*Accion: Revise que el archivo exista y vuelva a intentarlo ')
        Asercion.no_es_cadena_vacia(
            self.nom_seccion,
            '\n*Causa:  El nombre de la fuente vacio o sin valor'
            '\n*Accion: Ingrese un valor valido para el nombre de la fuente')

    def obten(self):
        self._config.read(self.nom_archivo)
        self.propiedades = dict(self._config.items(self.nom_seccion))
        return self.propiedades

    def __str__(self):
        return ', '.join(['{key}={value}'.
                         format(key=key, value=self.__dict__.get(key))
                          for key in self.__dict__])


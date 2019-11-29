from abc import ABCMeta, abstractmethod
import fuente.util.Excepciones as Exc

'''
@Author : Arturo Gonzalez B.
@email  : arturo.gonzalez@tusventasdigitales.com
@version: 0.1
'''


class DaoAbc(object):
    __metaclass__ = ABCMeta


class AdministradorDao:
    def __init__(self, dao, *args):
        self.es_tipo_dao(
            dao, "Causa: Se ha utilizado un objeto que no es el del tipo DAO.\n"
                 " Accion: Utilice un objeto DAO en el AdministradorDao.")
        self.dao = dao(*args)

    def es_tipo_dao(self, dao, txt_mensaje):
        try:
            assert type(dao) == type(DaoAbc), txt_mensaje
        except AssertionError:
            raise Exc.InstanciaInvalida(txt_mensaje)


class DaoExtraccionPropAbc:
    __metaclass__ = ABCMeta

    @abstractmethod
    def obten(self):
        pass


class ComandosSQL(object):
    __metaclass__ = ABCMeta


class ComandosDMLAbs(ComandosSQL):
    @abstractmethod
    def obten_datos(self):
        pass

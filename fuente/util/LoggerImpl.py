import fuente.dao.AdministradorDao as adminDao
from abc import ABCMeta, abstractmethod
import fuente.util.Utilerias as Util
import os
import sys


class LoggerAbs(object):

    @abstractmethod
    def persiste(self):
        pass


class Traceback:
    def __init__(self, tipo, valor, traceback):
        self.tipo = tipo
        self.valor = valor
        self.traceback = traceback

    def __str__(self):
        return ', '.join(['{key}={value}'.
                         format(key=key, value=self.__dict__.get(key))
                          for key in self.__dict__])


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Logger(object):
    __metaclass__ = Singleton

    def __init__(self, txt_trace, fase_etl, nom_script, fec_proceso, estatus):
        self.txt_trace = txt_trace
        self.fase_etl = fase_etl
        self.nom_script = nom_script
        self.fec_proceso = fec_proceso
        self.estatus = estatus

    def __str__(self):
        return ', '.join(['{key}={value}'.
                         format(key=key, value=self.__dict__.get(key))
                          for key in self.__dict__])


class LoggerTxt(LoggerAbs):

    def __init__(self, obj_traceback, dto_logger):
        self.obj_traceback = obj_traceback
        self.dto_logger = dto_logger

    def persiste(self):
        path_actual = os.path.dirname(os.path.abspath(__file__))
        dir_txt_log = 'logs/generador_de_insumos_' + Util.obt_fecha_actual() + '.log'
        nom_fichero_log = Util.obt_ruta(path_actual.replace('/util', ''), dir_txt_log)

        with adminDao.AdministradorDeFicheros(nom_fichero_log, 'a+') as fichero:
            fichero.write('***************************************************\n')
            fichero.write('Nombre_script:' + str(self.dto_logger.nom_script) + '\n')
            fichero.write('Fase_etl:     ' + str(self.dto_logger.fase_etl) + '\n')
            fichero.write('Seccion_etl:  ' + str(self.dto_logger.estatus) + '\n')
            fichero.write('Fec_proceso:  ' + str(self.dto_logger.fec_proceso) + '\n')
            fichero.write('Trace:        ' + str(self.dto_logger.txt_trace) + '\n')
            fichero.write('Valor:        ' + str(self.obj_traceback.valor) + '\n')
            fichero.write('Tipo:         ' + str(self.obj_traceback.tipo) + '\n')


def trace_error(exctype, value, tb):
    obj_traceback = Traceback(tipo=exctype, valor=value, traceback=tb)
    dto_logger = Logger(txt_trace=tb, fase_etl='', nom_script='', fec_proceso=Util.obt_fecha_actual(), estatus='')
    dto_logger.fec_proceso = Util.obt_fecha_actual()
    dto_logger.txt_trace = tb

    obj_logger_txt = LoggerTxt(obj_traceback, dto_logger)
    obj_logger_txt.persiste()


def logger(nom_fase):
    def decorador_logger(funcion):
        def decorador_wrapper(*args, **kwargs):
            agrega_detalle_log("INICIO", nom_fase)
            funcion(*args, **kwargs)
            agrega_detalle_log("FIN", nom_fase)

        return decorador_wrapper
    return decorador_logger


def logger_con_retorno(nom_fase):
    def decorador_logger(funcion):
        def decorador_wrapper(*args, **kwargs):
            agrega_detalle_log("INICIO", nom_fase)
            salida = funcion(*args, **kwargs)
            agrega_detalle_log("FIN", nom_fase)
            return salida

        return decorador_wrapper
    return decorador_logger


def agrega_detalle_log(estatus, nom_fase):
    log = Logger('', nom_fase, '', '', estatus)
    log.fase_etl = nom_fase
    log.estatus = estatus

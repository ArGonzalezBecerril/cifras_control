"""
@Author : Arturo Gonzalez B.
@email  : arturo.gonzalez@tusventasdigitales.com
@version: 0.1
"""


class InstanciaInvalida(Exception):
    def __init__(self, mensaje):
        Exception.__init__(self, mensaje)


class EntradaSalidaIO(Exception):
    def __init__(self, mensaje):
        Exception.__init__(self, mensaje)


class CadenaVacia(Exception):
    def __init__(self, mensaje):
        Exception.__init__(self, mensaje)


class ObjetoNoNulo(Exception):
    def __init__(self, mensaje):
        Exception.__init__(self, mensaje)


class ObjetoNoValido(Exception):
    def __init__(self, mensaje):
        Exception.__init__(self, mensaje)


class DiccionarioVacio(Exception):
    def __init__(self, mensaje):
        Exception.__init__(self, mensaje)

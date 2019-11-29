import fuente.dao.AdministradorDao as adminDao
import fuente.util.Asserciones as Asercion


class DaoImpala(adminDao.ComandosDMLAbs):
    def __init__(self, ojb_cnx_impala, esquema, nom_tabla):
        self.obj_cnx_impala = ojb_cnx_impala
        self.esquema = esquema
        self.nom_tabla = nom_tabla

        self.valida_parametros()
        self.conexion_impala = self.obj_cnx_impala.obten()

    def valida_parametros(self):
        Asercion.no_es_nulo(
            self.obj_cnx_impala,
            '\n*Causa: El objeto ConexionImpala esta vacio'
            '\n*Accion: Revise los datos de conexion y que la clase ConexionImpala este instanciada correctamente')
        Asercion.no_es_nulo(
            self.esquema,
            '\n*Causa: El atributo esquema no puede ir vacio'
            '\n*Accion: Revise que este asignando un valor al atributo *esquema*')
        Asercion.no_es_nulo(
            self.nom_tabla,
            '\n*Causa: El atributo nom_tabla no puede ir vacio'
            '\n*Accion: Revise que este asignando un valor al atributo *nom_tabla*')

    def obten_datos(self):
        pass

    def listado_tablas_de_bd(self):
        base_de_datos = self.conexion_impala.database(self.esquema)
        return base_de_datos.list_tables()

    def obten_total_de_archivos(self):
        total_de_mappers = self.conexion_impala.table(self.esquema + '.' + self.nom_tabla)
        return total_de_mappers.files()

    def obten_estadisticas_de_tabla(self):
        tabla_impala = self.conexion_impala.table(self.esquema + '.' + self.nom_tabla)
        estadistica_de_tabla = tabla_impala.stats()
        return estadistica_de_tabla

    def obten_estadisticas_de_columna(self):
        tabla_impala = self.conexion_impala.table(self.esquema + '.' + self.nom_tabla)
        estadistica_de_columna = tabla_impala.column_stats()
        return estadistica_de_columna

    def esta_particionada_la_tabla(self):
        tabla_impala = self.conexion_impala.table(self.esquema + '.' + self.nom_tabla)
        return tabla_impala.is_partitioned

    def obten_particiones_de_tabla(self):
        tabla_impala = self.conexion_impala.table(self.esquema + '.' + self.nom_tabla)
        return tabla_impala.partitions()

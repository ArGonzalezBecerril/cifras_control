import fuente.etl.EtlEstadisticaImpala as Etl
import findspark
import pyspark as pspk
import pyspark.sql as pysql
import fuente.util.Utilerias as Util
import sys
import fuente.util.LoggerImpl as Log


reload(sys)
sys.setdefaultencoding('utf-8')
sys.excepthook = Log.trace_error


findspark.init("/home/arturo/Software/spark-2.2.3-bin-hadoop2.7")
context = pspk.SparkContext.getOrCreate()
sql_context = pysql.SQLContext(context)

'''
@Author : Arturo Gonzalez B.
@email  : arturo.gonzalez@tusventasdigitales.com
@version: 0.1
'''


'''
@Paquete: fuente.etl.EtlEstadisticaImpala
@Clase:   EtlEstadisticaImpala
@Test: Integracion de logs y escritura en txt
'''

dto_logger = Log.Logger('', '', 'Generador_de_insumos', '', '')


dict_tablas_impala = Util.obten_diccionario_de_tablas()
grupo_de_tablas = dict_tablas_impala.items()

for nom_tabla, esquema in grupo_de_tablas:
    etl_impala = Etl.EtlEstadisticaImpala(context, esquema, nom_tabla)
    etl_impala.extrae()
    etl_impala.transforma()


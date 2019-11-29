import pytest
import fuente.util.Excepciones as Exp
import fuente.etl.EtlEstadisticaImpala as Etl
import findspark
import pyspark as pspk
import pyspark.sql as pysql
import fuente.util.Utilerias as Util


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
'''


@pytest.mark.parametrize('contexto', [None, ''])
def test_valida_instancia(contexto):
    with pytest.raises(Exp.CadenaVacia):
        Etl.EtlEstadisticaImpala(sql_context, None, None)


def test_extrae():
    dict_tablas_impala = Util.obten_diccionario_de_tablas()
    grupo_de_tablas = dict_tablas_impala.items()

    for nom_tabla, esquema in grupo_de_tablas:
        etl_impala = Etl.EtlEstadisticaImpala(context, esquema, nom_tabla)
        etl_impala.extrae()

import pandas as pd

from scr.proceso_agrupacion_tablas_pandas import proceso_agrupacion_tablas_pandas
from scr.proceso_agrupacion_tablas_spark import proceso_agrupacion_tablas_spark


def dict_to_pandas(diccionario: dict):
    return pd.DataFrame.from_dict(diccionario)

def dict_to_spark(diccionario: dict, spark):
    return spark.createDataFrame(diccionario)


def run_function(spark):
    dict_tablon_usuario = {"user_id": [1, 2, 3, 4],
    "fecha_creacion": ["2020-02-01", "2020-02-01",
    "2020-02-01", "2020-02-01"],
    "tipo_usuario": ["Socio", "Registrado", "Socio",
    "Socio"]}
    dict_tablon_suscripcion = {"suscription_id": [1, 2, 3, 4],
    "user_id": [1, 3, 1, 4],
    "fecha_creacion_suscripcion": ["2020-01-01",
    "2022-12-01", "2022-12-01", "2020-01-01"],
    "suscripcion_activa": [False, True, True, True]}
    # Add lines
    pandas_dataframe_usuario = dict_to_pandas(dict_tablon_usuario)
    pandas_dataframe_suscripcion = dict_to_pandas(dict_tablon_suscripcion)

    spark_dataframe_usuario = dict_to_spark(dict_tablon_usuario, spark)
    spark_dataframe_suscripcion = dict_to_spark(dict_tablon_suscripcion, spark)

    proceso_agrupacion_tablas_pandas(pandas_dataframe_usuario, pandas_dataframe_suscripcion)
    proceso_agrupacion_tablas_spark(spark_dataframe_usuario, spark_dataframe_suscripcion)

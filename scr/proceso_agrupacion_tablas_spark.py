from pyspark.sql.window import Window
from pyspark.sql.functions import f, row_number


def proceso_agrupacion_tablas_spark(dataframe_usuario: "pd.DataFrame", dataframe_suscripcion: "pd.DataFrame"):
    dataframe_usuario = dataframe_usuario.select("user_id", "fecha_creacion")
    join_dataframe = dataframe_suscripcion.join(dataframe_usuario,
                                                dataframe_suscripcion.user_id == dataframe_usuario.user_id, "left")
    df_first_creation_date = extract_first_creation_date(join_dataframe)
    count_subscripciones = df_first_creation_date.count()
    print(f"numero de suscripciones {count_subscripciones}")
    return count_subscripciones


def extract_first_creation_date(dataframe: "pd.DataFrame"):
    w2 = Window.partitionBy("user_id").orderBy(f.col("fecha_creacion").desc())
    return dataframe.withColumn("row", row_number().over(w2)).filter(f.col("row") == 1).drop("row")

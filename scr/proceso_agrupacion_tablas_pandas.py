import pandas as pd


def proceso_agrupacion_tablas_pandas(dataframe_usuario: "pd.DataFrame", dataframe_suscripcion: "pd.DataFrame"):
    df_subscricipcion_activa = subscripciones_activas(dataframe_suscripcion)
    df_subs_fecha_creacion = suscripcion_fecha_creacion_usuario(dataframe_usuario, df_subscricipcion_activa)
    df_ordenado = ordenar_dataframe_fechas(df_subs_fecha_creacion)
    count_suscripciones = contar_suscripciones(df_ordenado)
    print(f"numero de suscripciones {count_suscripciones}")
    return count_suscripciones


def subscripciones_activas(dataframe: "pd.DataFrame"):
    return dataframe[dataframe['suscripcion_activa'] == True]


def ordenar_dataframe_fechas(dataframe: "pd.DataFrame"):
    return dataframe.sort_values("fecha_creacion_suscripcion", ascending=True)


def contar_suscripciones(dataframe: "pd.DataFrame"):
    return dataframe['suscription_id'].count()


def suscripcion_fecha_creacion_usuario(dataframe_usuario: "pd.DataFrame", dataframe_suscripcion: "pd.DataFrame"):
    return pd.merge(dataframe_suscripcion, dataframe_usuario[['fecha_creacion', 'user_id']], how='left', on='user_id')

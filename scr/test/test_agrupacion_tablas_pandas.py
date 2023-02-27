import pandas as pd
import pytest

from scr.proceso_agrupacion_tablas_pandas import subscripciones_activas, suscripcion_fecha_creacion_usuario, \
    ordenar_dataframe_fechas, contar_suscripciones


class TestPreprocesado:
    @pytest.fixture(scope="class")
    def data_input_usuarios(self):
        data_input = pd.DataFrame([(1, "2020-02-01"), (1, "2021-02-01")],
                                  columns=['user_id', 'fecha_creacion'])
        return data_input

    @pytest.fixture(scope="class")
    def data_input_suscripcion(self):
        data_input = pd.DataFrame([(1, 1, "2020-02-01", "False"), (2, 1, "2022-12-01", "True")],
                                  columns=['suscription_id', 'user_id', 'fecha_creacion_suscripcion',
                                           'suscripcion_activa'])
        return data_input

    def test_suscripcion_activas(self, data_input_suscripcion):
        susc_activas = subscripciones_activas(data_input_suscripcion)
        assert all(susc_activas['suscripcion_activa'] == True)

    def test_cruce_dataframe_usuario(self, data_input_usuarios, data_input_suscripcion):
        join = suscripcion_fecha_creacion_usuario(data_input_usuarios, data_input_suscripcion)
        assert "fecha_creacion" in join.columns
        assert len(join[join['user_id'] == 1]) == 4

    def test_ordenar_usuario(self, data_input_suscripcion):
        suscripcion_ordenadada = ordenar_dataframe_fechas(data_input_suscripcion)
        assert suscripcion_ordenadada.iloc[0]['fecha_creacion_suscripcion'] == "2020-02-01"

    def test_conteo(self, data_input_suscripcion):
        conteo = contar_suscripciones(data_input_suscripcion)
        assert conteo == 2

from __future__ import annotations

from datetime import date

from pyspark.sql import functions as F

from pipeline import quality
from pipeline.gold import (
    COLUMNAS_LLUVIA_EMPRESA,
    COLUMNAS_TARIFA_POR_MILLA,
    TABLA_SINERGIA4,
    TABLAS_GOLD,
    tarifa_por_milla_por_barrio,
    viajes_por_empresa_y_lluvia,
)
from pipeline.silver import clima_diario, enriquecer, limpiar_viajes

MESES = ["2024-01"]


# --- el ejemplo de la casa: tarifa por milla por barrio y franja del día -----------------------

ESQUEMA_ENRIQUECIDO = (
    "pickup_date date, pickup_datetime string, borough string, distancia_mi double, "
    "total_pagado double"
)


def _enriquecido(spark, filas):
    """La hora llega como texto y se convierte con Spark: así hora y franja usan la misma zona."""
    df = spark.createDataFrame(filas, ESQUEMA_ENRIQUECIDO)
    return df.withColumn("pickup_datetime", F.to_timestamp("pickup_datetime"))


def test_tabla_de_la_casa_esta_en_tablas_gold():
    assert list(TABLAS_GOLD) == ["tarifa_por_milla_barrio", TABLA_SINERGIA4]
    assert TABLAS_GOLD["tarifa_por_milla_barrio"] is tarifa_por_milla_por_barrio
    assert TABLAS_GOLD[TABLA_SINERGIA4] is viajes_por_empresa_y_lluvia


# --- equipo sinergia4: viajes por empresa y nivel de lluvia -----------------------------------

ESQUEMA_LLUVIA = (
    "pickup_date date, empresa string, lluvia_mm double, total_pagado double, propina double"
)


def test_lluvia_empresa_una_fila_por_fecha_y_empresa(spark):
    dia = date(2024, 1, 9)
    filas = [
        (dia, "Uber", 30.0, 20.0, 2.0),
        (dia, "Uber", 30.0, 10.0, 0.0),
        (dia, "Lyft", 30.0, 15.0, 1.5),
    ]
    gold = viajes_por_empresa_y_lluvia(spark.createDataFrame(filas, ESQUEMA_LLUVIA))

    assert gold.columns == COLUMNAS_LLUVIA_EMPRESA
    por_empresa = {r["empresa"]: r for r in gold.collect()}
    assert por_empresa["Uber"]["viajes"] == 2
    assert por_empresa["Uber"]["ingreso_total"] == 30.0
    assert por_empresa["Uber"]["propina_total"] == 2.0
    assert por_empresa["Uber"]["lluvia_mm"] == 30.0  # la lluvia del día no se suma
    assert por_empresa["Lyft"]["viajes"] == 1


def test_nivel_lluvia_cortes_y_nulos(spark):
    casos = {date(2024, 1, d): mm for d, mm in [(1, 0.0), (2, 0.1), (3, 4.9), (4, 5.0), (5, None)]}
    filas = [(dia, "Uber", mm, 1.0, 0.0) for dia, mm in casos.items()]
    gold = viajes_por_empresa_y_lluvia(spark.createDataFrame(filas, ESQUEMA_LLUVIA))
    niveles = {r["fecha"].day: r["nivel_lluvia"] for r in gold.collect()}
    assert niveles == {1: "sin_lluvia", 2: "ligera", 3: "ligera", 4: "fuerte", 5: "sin_dato"}


def test_lluvia_empresa_suma_de_viajes_igual_a_filas_de_entrada(spark):
    filas = [(date(2024, 1, d % 3 + 1), ("Uber", "Lyft")[d % 2], 1.0, 5.0, 0.5) for d in range(10)]
    gold = viajes_por_empresa_y_lluvia(spark.createDataFrame(filas, ESQUEMA_LLUVIA))
    assert gold.agg(F.sum("viajes")).first()[0] == 10


def test_tarifa_por_milla_valores_conocidos(spark):
    dia = date(2024, 1, 1)
    enriquecido = _enriquecido(
        spark,
        [
            (dia, "2024-01-01 08:00:00", "Manhattan", 2.0, 10.0),
            (dia, "2024-01-01 09:30:00", "Manhattan", 3.0, 20.0),
            (dia, "2024-01-01 08:05:00", "Queens", 10.0, 25.0),
        ],
    )

    gold = tarifa_por_milla_por_barrio(enriquecido)

    assert gold.columns == COLUMNAS_TARIFA_POR_MILLA
    filas = {(r["borough"], r["franja"]): r for r in gold.collect()}
    manhattan = filas[("Manhattan", "mañana")]
    assert manhattan["viajes"] == 2
    assert manhattan["distancia_total_mi"] == 5.0
    assert manhattan["ingreso_total"] == 30.0
    assert manhattan["tarifa_por_milla"] == 6.0  # 30 / 5
    assert filas[("Queens", "mañana")]["tarifa_por_milla"] == 2.5


def test_las_franjas_del_dia_cortan_a_las_6_12_y_18(spark):
    dia = date(2024, 1, 1)
    horas = {0: "madrugada", 5: "madrugada", 6: "mañana", 11: "mañana", 12: "tarde"}
    horas.update({17: "tarde", 18: "noche", 23: "noche"})
    enriquecido = _enriquecido(
        spark,
        [(dia, f"2024-01-01 {h:02d}:30:00", f"B{h}", 1.0, 5.0) for h in horas],
    )
    franjas = {
        r["borough"]: r["franja"] for r in tarifa_por_milla_por_barrio(enriquecido).collect()
    }
    assert franjas == {f"B{h}": franja for h, franja in horas.items()}


def test_un_viaje_sin_barrio_cuenta_como_desconocido(spark):
    enriquecido = _enriquecido(spark, [(date(2024, 1, 1), "2024-01-01 08:00:00", None, 1.0, 5.0)])
    gold = tarifa_por_milla_por_barrio(enriquecido)
    assert gold.first()["borough"] == "Desconocido"


def test_tarifa_por_milla_pasa_calidad_con_la_muestra(muestra_viajes, muestra_zonas, muestra_clima):
    enriquecido = enriquecer(
        limpiar_viajes(muestra_viajes, MESES), muestra_zonas, clima_diario(muestra_clima)
    )
    gold = tarifa_por_milla_por_barrio(enriquecido)

    assert gold.agg({"viajes": "sum"}).first()[0] == 90
    quality.validar(gold, nombre="gold")
    assert gold.where("tarifa_por_milla <= 0").count() == 0

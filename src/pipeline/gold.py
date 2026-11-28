"""Capa gold: las tablas de negocio. Cada una responde una pregunta.

Ejemplo de la casa (NO es una pregunta del Taller): «¿A qué hora del día y en qué barrio se paga
más por milla?». Se responde con `tarifa_por_milla_por_barrio`, que escribe
`gold/tarifa_por_milla_barrio/mes=AAAA-MM/`.

Cómo se arma una tabla de gold (la receta de `main`, por mes y por tabla):

    leer silver (fhvhv, taxi_zones, clima) -> silver.enriquecer(...) -> tu función
        -> validar -> escribir_capa (reemplaza mes=<mes>/)

Tu función recibe el `enriquecido` (viajes con barrio, zona y clima) y devuelve la tabla de gold.
Para agregar la tuya: escribe la función y súmala al diccionario TABLAS_GOLD (y su CREATE TABLE a
carga.py). Gold no tiene huecos.

Un detalle que se ve al reprocesar: `enriquecer` une zonas y clima, así que si silver trae las
tablas duplicadas (silver no deduplica todavía) cada viaje se repite una vez por cada fila
repetida de zonas y de clima: con dos copias de cada tabla, ×2 · ×2 · ×2 = ×8 viajes.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline import quality, silver
from pipeline.herramientas.capas import escribir_capa, leer_capa
from pipeline.herramientas.control import con_control
from pipeline.herramientas.corrida import Resultado
from pipeline.herramientas.rutas import (
    TABLA_CLIMA,
    TABLA_VIAJES,
    TABLA_ZONAS,
    ruta_lake,
)

CAPA = "gold"

# --- el ejemplo de la casa: tarifa por milla por barrio y franja ------------------------------

COLUMNAS_TARIFA_POR_MILLA = [
    "fecha",
    "borough",
    "franja",
    "viajes",
    "distancia_total_mi",
    "ingreso_total",
    "tarifa_por_milla",
]

# Para los viajes cuya zona de recogida no está en el lookup (el barrio queda nulo).
BARRIO_DESCONOCIDO = "Desconocido"


def _franja_del_dia(hora: F.Column) -> F.Column:
    """madrugada (0 a 5 h), mañana (6 a 11), tarde (12 a 17) o noche (18 a 23)."""
    return (
        F.when(hora < 6, "madrugada")
        .when(hora < 12, "mañana")
        .when(hora < 18, "tarde")
        .otherwise("noche")
    )


def tarifa_por_milla_por_barrio(enriquecido: DataFrame) -> DataFrame:
    """Por fecha, barrio y franja del día: viajes, millas, ingreso y cuánto se paga por milla.

    tarifa_por_milla = ingreso_total / distancia_total_mi. Se calcula con las sumas completas y
    se redondea al final; así el redondeo de las sumas no mueve el cociente.
    """
    con_franja = enriquecido.select(
        F.col("pickup_date").alias("fecha"),
        F.coalesce(F.col("borough"), F.lit(BARRIO_DESCONOCIDO)).alias("borough"),
        _franja_del_dia(F.hour("pickup_datetime")).alias("franja"),
        F.col("distancia_mi"),
        F.col("total_pagado"),
    )
    return (
        con_franja.groupBy("fecha", "borough", "franja")
        .agg(
            F.count(F.lit(1)).alias("viajes"),
            F.sum("distancia_mi").alias("distancia"),
            F.sum("total_pagado").alias("ingreso"),
        )
        .select(
            "fecha",
            "borough",
            "franja",
            "viajes",
            F.round("distancia", 2).alias("distancia_total_mi"),
            F.round("ingreso", 2).alias("ingreso_total"),
            F.round(F.col("ingreso") / F.col("distancia"), 2).alias("tarifa_por_milla"),
        )
        .select(*COLUMNAS_TARIFA_POR_MILLA)
    )


# --- equipo sinergia4: concentración Uber/Lyft por zona y nivel de lluvia ---------------------
#
# Pregunta (taller2/contrato.md): ¿la lluvia cambia la concentración del mercado entre Uber y Lyft
# por zona en Nueva York durante 2024?
#
# Grano: una fila por fecha, zona de recogida y empresa. La lluvia es del día, así que lluvia_mm y
# nivel_lluvia se repiten en todas las filas de esa fecha: los días se cuentan con
# COUNT(DISTINCT fecha), nunca sumando lluvia_mm. La participación de cada empresa se calcula
# después, en Snowflake, con SUM(viajes) / SUM(viajes) (ponderada, no promedio de promedios).

TABLA_SINERGIA4 = "sinergia4_concentracion_lluvia"

COLUMNAS_CONCENTRACION_LLUVIA = [
    "fecha",
    "pu_location_id",
    "borough",
    "zona",
    "empresa",
    "lluvia_mm",
    "nivel_lluvia",
    "viajes",
    "ingreso_total",
    "propina_total",
]

# Umbral de lluvia fuerte en mm por día (decisión del contrato, sección 4).
LLUVIA_FUERTE_MM = 5.0


def nivel_lluvia(lluvia_mm: F.Column) -> F.Column:
    """sin_dato (nulo), sin_lluvia (0 mm), ligera (más de 0 y menos de 5 mm) o fuerte (5 mm o más).

    Nulo = el día no tiene clima (el left join de enriquecer no encontró la fecha): se cuenta
    aparte para no confundirlo con un día seco.
    """
    return (
        F.when(lluvia_mm.isNull(), "sin_dato")
        .when(lluvia_mm <= 0, "sin_lluvia")
        .when(lluvia_mm < LLUVIA_FUERTE_MM, "ligera")
        .otherwise("fuerte")
    )


def viajes_por_zona_empresa_y_lluvia(enriquecido: DataFrame) -> DataFrame:
    """Por fecha, zona de recogida y empresa: viajes, ingreso y propinas, con la lluvia del día.

    viajes cuenta filas de silver (un viaje por fila), así que SUM(viajes) de esta tabla debe ser
    igual a las filas de silver del mes: es la prueba de que no hay duplicados ni pérdidas.
    Un viaje sin zona en el lookup queda con borough 'Desconocido' (no se pierde). Las sumas de
    dinero se redondean al final, sobre el total del grupo.
    """
    return (
        enriquecido.withColumn("borough", F.coalesce(F.col("borough"), F.lit("Desconocido")))
        .groupBy(
            F.col("pickup_date").alias("fecha"),
            "pu_location_id",
            "borough",
            "zona",
            "empresa",
        )
        .agg(
            F.count(F.lit(1)).alias("viajes"),
            # La lluvia es la misma en todas las filas del día: max solo la toma una vez.
            F.max("lluvia_mm").alias("lluvia_mm"),
            F.sum("total_pagado").alias("ingreso"),
            F.sum("propina").alias("propina"),
        )
        .select(
            "fecha",
            "pu_location_id",
            "borough",
            "zona",
            "empresa",
            "lluvia_mm",
            nivel_lluvia(F.col("lluvia_mm")).alias("nivel_lluvia"),
            "viajes",
            F.round("ingreso", 2).alias("ingreso_total"),
            F.round("propina", 2).alias("propina_total"),
        )
        .select(*COLUMNAS_CONCENTRACION_LLUVIA)
    )


# nombre de la tabla en gold -> función que la arma a partir del enriquecido. El equipo agrega la
# suya aquí. La de la casa se conserva: sirve de control de duplicados con la misma corrida.
TABLAS_GOLD = {
    "tarifa_por_milla_barrio": tarifa_por_milla_por_barrio,
    TABLA_SINERGIA4: viajes_por_zona_empresa_y_lluvia,
}


# --- el paso ----------------------------------------------------------------------------------


def _gold_de_una_tabla(spark: Any, corrida: Any, mes: str, nombre: str, construir) -> Resultado:
    """La puerta decide; si pasa, silver -> enriquecer -> tu función -> validar -> gold."""
    entorno = corrida.entorno
    fuente = ruta_lake(entorno, "silver", TABLA_VIAJES, mes)

    def trabajo() -> tuple[int, int]:
        viajes = leer_capa(spark, entorno, "silver", TABLA_VIAJES, mes)
        zonas = leer_capa(spark, entorno, "silver", TABLA_ZONAS, mes)
        clima = leer_capa(spark, entorno, "silver", TABLA_CLIMA, mes)
        enriquecido = silver.enriquecer(viajes, zonas, clima)
        # Las filas que entran al cálculo: aquí se nota si silver duplicó (cada tabla repetida
        # multiplica los viajes).
        leidas = enriquecido.count()

        gold = construir(enriquecido)
        quality.validar(gold, nombre=f"gold.{nombre}")
        escritas = escribir_capa(gold, entorno, CAPA, nombre, mes)
        return leidas, escritas

    return con_control(corrida, nombre, CAPA, mes, fuente, trabajo)


def main(spark: Any, corrida: Any) -> list[Resultado]:
    """Gold de los meses de la corrida (sin banner ni cierre); un Resultado por tabla."""
    resultados: list[Resultado] = []
    for mes in corrida.meses:
        for nombre, construir in TABLAS_GOLD.items():
            resultados.append(_gold_de_una_tabla(spark, corrida, mes, nombre, construir))
    return resultados

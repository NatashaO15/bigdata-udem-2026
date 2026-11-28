# Contrato del Taller 2

Equipo: sinergia4 · Anfitriona (dueña del fork): Natasha Orozco (`NatashaO15`)

## 1. Integrantes y roles

Los roles rotan respecto al Taller 1: nadie repite.

| Integrante | Rol en el Taller 1 | Rol en el Taller 2 |
|---|---|---|
| Xiomy Baquero | Ingeniera de datos | Analista de negocio y cumplimiento |
| Juan Carlos Monterrosa | No hizo parte del equipo en el Taller 1 (ingreso autorizado por el instructor) | Modelador SQL |
| Natasha Orozco | Analista | Ingeniera de datos |
| Miguel Ángel Vélez | Modelador | Operador cloud y calidad |

La evidencia sale de la cuenta de la anfitriona (AWS y Snowflake de Natasha). Miguel, como operador, dirige las corridas de Glue y Airflow, revisa el costo y el apagado, y lo hace sobre esa cuenta junto con Natasha (pantalla compartida).

## 2. Pregunta de negocio

> ¿La lluvia cambia la concentración del mercado entre Uber y Lyft por zona en Nueva York durante 2024 (viajes FHVHV de la TLC, enero a diciembre, y lluvia diaria de Open-Meteo)?

**Para quién es la respuesta y qué decisión apoyaría:** para la autoridad de movilidad del Taller 1 (caso A), que vio que Uber tiene cerca del 74 % de los viajes y que su predominio por zona cambia de un mes a otro. Si la lluvia aumenta la concentración, la supervisión de las zonas donde una plataforma supera el 70 % debería reforzarse en los días de lluvia.

## 3. Hipótesis

> En los días de lluvia fuerte el mercado se concentra más en Uber: su participación sube frente a la de los días sin lluvia y aumenta el número de zonas donde supera el 70 % de los viajes, porque su flota es más grande y absorbe mejor el aumento de demanda. La hipótesis es falsa si la participación de Uber cambia menos de 1 punto porcentual (o baja) y el número de zonas por encima del 70 % no aumenta.

## 4. Cambio elegido

| Campo | Respuesta |
|---|---|
| Capa | gold |
| Archivo y función donde va | `src/pipeline/gold.py`, función nueva `viajes_por_zona_empresa_y_lluvia` (en `TABLAS_GOLD` con el nombre `sinergia4_concentracion_lluvia`), y su `CREATE TABLE` en `src/pipeline/carga.py` (en `TABLAS_A_CARGAR`, el mismo de `taller2/crear_tablas.sql`) |
| Qué se agrega o cambia | Columna `nivel_lluvia` y una tabla de gold con grano fecha × zona de recogida × empresa: `fecha`, `pu_location_id`, `borough`, `zona`, `empresa`, `lluvia_mm`, `nivel_lluvia`, `viajes`, `ingreso_total`, `propina_total`. Un viaje sin zona en el lookup queda con borough `Desconocido` (no se pierde). |
| Definición exacta | `nivel_lluvia` sale de `lluvia_mm` del día: `sin_lluvia` si es 0; `ligera` si es mayor que 0 y menor que 5 mm; `fuerte` si es de 5 mm o más; `sin_dato` si es nula (día sin clima). Los umbrales son provisionales y se confirman con la distribución de los 366 días de 2024 (ver sección 9). |
| Por qué lo necesita la pregunta | El gold de la casa agrupa por borough y franja, no por empresa, zona ni nivel de lluvia: sin esta tabla no se puede medir la participación de cada plataforma por zona en días con y sin lluvia. |

## 5. Cómo se comprobará

- **Cifras de Athena:** en silver, `COUNT(*)` contra `COUNT(DISTINCT _hash_fila)` de `dev_silver_fhvhv` (deben ser iguales), antes y después de escribir `deduplicar` y tras un reproceso; los 366 días de `dev_silver_clima` con su `lluvia_mm`, para revisar cuántos días cae en cada nivel.
- **Cifras de Snowflake:** `SUM(VIAJES)` de la tabla del equipo en `BIGDATA.DEV`, mes por mes, igual a las filas de `dev_silver_fhvhv` de cada mes (Athena); número de días por nivel con `COUNT(DISTINCT FECHA)`; y ninguna combinación fecha-zona-empresa repetida.
- **CI y revisión:** el PR del cambio debe tener los cinco checks en verde antes del merge. Revisor: un integrante distinto de quien escribe el cambio (el cambio lo escribe Natasha; revisa Juan Carlos o Miguel).
- **Corrida en Glue:** `computo = glue_small`, `limite = todos`, una corrida por mes de `2024-01` a `2024-12`, desde el Airflow del Code Editor de la anfitriona. Las pruebas de duplicados (antes, después y reproceso) con `limite = 1000` y `mes = 2024-01`.

## 6. Qué cifra responderá la pregunta

- **Tabla de donde sale:** `BIGDATA.DEV.SINERGIA4_CONCENTRACION_LLUVIA` (en gold: `sinergia4_concentracion_lluvia`) en Snowflake.
- **La cifra o comparación:** (1) participación de Uber por nivel de lluvia en 2024 = `SUM(VIAJES)` de Uber / `SUM(VIAJES)` total, para cada `nivel_lluvia` (también por borough); (2) número de zonas donde Uber supera el 70 % de los viajes, en días sin lluvia frente a días de lluvia fuerte, contando solo zona-nivel con al menos 100 viajes (el mismo umbral del Taller 1).
- **Cómo se leerá el resultado:** si en días de lluvia fuerte la participación de Uber supera en 1 punto porcentual o más a la de días sin lluvia y hay más zonas por encima del 70 %, la hipótesis se confirma; si la diferencia es menor o negativa, se contradice.
- **Conciliación prevista:** bronze → silver (filas de bronze menos descartes por tipo: distancia, tarifa, duración, fecha fuera del mes, ubicación nula); silver → gold (filas de silver = `SUM(viajes)` de gold); gold → Snowflake (filas y `SUM(VIAJES)` iguales a lo que gold escribió, según la tabla de control).

## 7. Riesgos

| Riesgo | Cómo lo manejaremos |
|---|---|
| La lluvia se repite en todas las filas del día | La tabla tiene una fila por fecha y empresa; los días se cuentan con `COUNT(DISTINCT FECHA)`, nunca sumando `lluvia_mm` |
| Promediar participaciones de días distintos (promedio de promedios) | La participación se calcula con sumas de viajes (`SUM / SUM`), ponderada por viajes |
| Pocos días de lluvia fuerte | Se analiza el año completo (366 días); se revisa la distribución antes de fijar los umbrales y se reporta cuántos días tiene cada nivel |
| Zonas con pocos viajes dan participaciones extremas | Solo se cuentan zona-nivel con al menos 100 viajes (umbral del Taller 1); las excluidas se reportan |
| Correr 12 meses en Glue toma horas | Una corrida por mes con `glue_small`, empezando por enero; se anota el tiempo de cada mes para el informe de costo |
| Días sin dato de clima (`lluvia_mm` nula) | Nivel `sin_dato`, contado aparte y reportado |
| Duplicados al reprocesar (×8 en gold) | `agregar_metadatos` y `deduplicar` escritos; comprobar `COUNT(*)` = `COUNT(DISTINCT _hash_fila)` en silver |
| Glue o Snowflake se quedan sin presupuesto | Un solo mes, `glue_small`, pruebas con `limite = 1000`, y apagar al terminar cada sesión |

## 8. Acuerdos de trabajo

- **Reunión del equipo:** viernes 27 de noviembre de 2026, 8:00 p. m., por Microsoft Teams (y seguimiento por el mismo canal hasta la entrega)
- **Quién revisa los PR:** nadie aprueba su propio cambio; revisa otro integrante con Review changes → Approve.
- **Cuándo se hace merge:** solo con CI en verde y una revisión aprobada.
- **Hitos propios:** 28 de nov.: funciones del equipo y tabla de gold en `main`, evidencia de duplicados y corrida de los 12 meses en Glue; 29 de nov.: conciliación, respuesta, informe y release `t2-entrega` antes de las 23:59.
- **Quién apaga las cuentas al terminar cada sesión:** Miguel (operador), con Natasha en su cuenta: `make airflow-off`, `make airflow-local-down` y `make estado`.
- **Si alguien no responde en 24 horas:** la analista reasigna su parte y lo anota en la sección 9.

## 9. Cambios al contrato

| Fecha | Qué cambió | Por qué |
|---|---|---|
| 2026-11-28 | Se definen los roles del Taller 2 y entra Juan Carlos Monterrosa al equipo | Cambio de integrantes respecto al Taller 1 |
| 2026-11-28 | Umbrales de `nivel_lluvia` provisionales (0 / menos de 5 mm / 5 mm o más) | Se confirman con los días de 2024 en Athena antes de la corrida final |
| 2026-11-28 | La pregunta pasa de «participación de Uber frente a Lyft según la lluvia, en enero de 2024» a «concentración del mercado por zona según la lluvia, en todo 2024». La tabla de gold pasa de `sinergia4_lluvia_empresa` (fecha × empresa) a `sinergia4_concentracion_lluvia` (fecha × zona × empresa). | Asesoría con el instructor: retomar la pregunta del Taller 1 (caso A) y analizar el año completo, porque un solo mes no es concluyente |
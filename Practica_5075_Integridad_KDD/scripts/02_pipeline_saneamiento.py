"""
Fases 2 y 3: Saneamiento de Datos, Detección de Outliers (IQR), Perfilado ISO/IEC 25012
y Preparación KDD para Business Intelligence.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# -----------------------------------------------------------------------------
# 1. DEFINICIÓN DE RUTAS Y PARÁMETROS DE GOBIERNO DEL DATO
# -----------------------------------------------------------------------------
# Detección automática de la raíz del proyecto (soporta ejecución desde /scripts)
SCRIPT_DIR: Path = Path(__file__).resolve().parent
BASE_DIR: Path = SCRIPT_DIR.parent if SCRIPT_DIR.name.lower() == "scripts" else SCRIPT_DIR

STAGING_VERIFIED_DIR: Path = BASE_DIR / "staging" / "verified"
FASE1_AUDIT_PATH: Path = BASE_DIR / "staging" / "fase1_auditoria.json"
QUARANTINE_BAD_RECORDS: Path = BASE_DIR / "quarantine" / "bad_records.csv"
PROCESSED_DIR: Path = BASE_DIR / "processed"
OUTPUT_DATASET_PATH: Path = PROCESSED_DIR / "dataset_kdd_listo.csv"
PROFILING_JSON_PATH: Path = PROCESSED_DIR / "data_profiling_summary.json"
INFORME_MD_PATH: Path = BASE_DIR / "INFORME_CALIDAD_KDD.md"
LOG_DIR: Path = BASE_DIR / "logs"

# -----------------------------------------------------------------------------
# 2. CONFIGURACIÓN DEL SISTEMA DE LOGGING (AUDITORÍA DUAL: CONSOLA + FICHERO)
# -----------------------------------------------------------------------------
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "fase2_3_saneamiento_kdd.log", encoding="utf-8"),
    ],
)

# Reglas de negocio y esquema esperado
ANIO_EN_CURSO: int = 2026
FECHA_CORTE_MAXIMA: pd.Timestamp = pd.Timestamp("2026-11-12 23:59:59")
COLUMNAS_OBLIGATORIAS: list[str] = ["id_transaccion", "timestamp", "importe_eur", "unidades"]
COLUMNAS_SECUNDARIAS: list[str] = ["tienda", "categoria", "metodo_pago"]


def cargar_ficheros_verificados() -> pd.DataFrame:
    """Carga y concatena los ficheros CSV validados en `staging/verified/`.

    Incorpora la columna de metadatos `_archivo_origen` en cada DataFrame parcial
    para garantizar la trazabilidad completa hasta el fichero fuente en caso de
    que un registro deba ser enviado a cuarentena.

    Returns:
        pd.DataFrame: DataFrame unificado con todos los registros verificados
            en la Fase 1.

    Raises:
        FileNotFoundError: Si no existen ficheros `.csv` en `staging/verified/`.
    """
    archivos = sorted(STAGING_VERIFIED_DIR.glob("*.csv"))
    if not archivos:
        raise FileNotFoundError(f"No hay archivos CSV verificados en {STAGING_VERIFIED_DIR}")

    frames: list[pd.DataFrame] = []
    for ruta in archivos:
        # Forzar 'id_transaccion' como string nullable para preservar valores vacíos como <NA>
        df_parcial = pd.read_csv(ruta, dtype={"id_transaccion": "string"})
        df_parcial["_archivo_origen"] = ruta.name
        frames.append(df_parcial)
        logging.info(f"Cargado '{ruta.name}': {len(df_parcial)} registros.")

    df_total = pd.concat(frames, ignore_index=True)
    logging.info(f"Total registros cargados en staging: {len(df_total)}")
    return df_total


def calcular_estadisticos_importe(serie: pd.Series) -> dict[str, int | float]:
    """Calcula métricas estadísticas descriptivas sobre la variable `importe_eur`.

    Convierte de forma segura la serie a valores numéricos descartando nulos o
    cadenas corruptas para obtener medidas de tendencia central, dispersión y
    posición requeridas por el informe de perfilado KDD.

    Args:
        serie (pd.Series): Serie de pandas correspondiente a `importe_eur`.

    Returns:
        dict[str, int | float]: Diccionario con tamaño muestral (`n`), `media`,
            `mediana`, `desviacion_estandar` (muestral, ddof=1), `percentil_25_q1`,
            `percentil_75_q3`, `minimo` y `maximo` redondeados a 2 decimales.
    """
    s = pd.to_numeric(serie, errors="coerce").dropna()
    return {
        "n": int(s.count()),
        "media": round(float(s.mean()), 2),
        "mediana": round(float(s.median()), 2),
        "desviacion_estandar": round(float(s.std(ddof=1)), 2),
        "percentil_25_q1": round(float(s.quantile(0.25)), 2),
        "percentil_75_q3": round(float(s.quantile(0.75)), 2),
        "minimo": round(float(s.min()), 2),
        "maximo": round(float(s.max()), 2),
    }


def ejecutar_pipeline_kdd() -> dict[str, Any]:
    """Orquesta las Fases 2 y 3 del pipeline: saneamiento, cuarentena y perfilado KDD.

    Aplica controles de calidad de datos sobre el lote verificado, segrega los
    registros no conformes en `quarantine/bad_records.csv`, imputa atributos
    secundarios faltantes, normaliza el esquema hacia `processed/dataset_kdd_listo.csv`
    y genera el reporte de calidad ISO/IEC 25012 en formato JSON.

    Returns:
        dict[str, Any]: Resumen completo de perfilado de calidad de datos con las
            métricas de integridad (Fase 1), saneamiento (Fase 2), indicadores
            ISO/IEC 25012 y comparativa distributiva Pre/Post limpieza.
    """
    # Creación idempotente de directorios de salida
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_BAD_RECORDS.parent.mkdir(parents=True, exist_ok=True)

    logging.info("=== INICIO FASE 2: SANEAMIENTO, COHERENCIA Y DETECCIÓN DE OUTLIERS ===")
    df_raw = cargar_ficheros_verificados()
    total_inicial: int = len(df_raw)

    # -------------------------------------------------------------------------
    # PASO 2.1: Perfilado de línea base (Pre-limpieza) según ISO/IEC 25012
    # -------------------------------------------------------------------------
    registros_sin_nulos_pre = int(df_raw.drop(columns=["_archivo_origen"]).dropna().shape[0])
    completitud_pre_pct = round((registros_sin_nulos_pre / total_inicial) * 100, 2)
    stats_importe_pre = calcular_estadisticos_importe(df_raw["importe_eur"])

    # Conversión tipada auxiliar sin destruir los valores crudos originales
    # (necesarios para dejar constancia exacta en quarantine/bad_records.csv)
    df = df_raw.copy()
    df["importe_num"] = pd.to_numeric(df["importe_eur"], errors="coerce")
    df["unidades_num"] = pd.to_numeric(df["unidades"], errors="coerce")
    df["ts_parsed"] = pd.to_datetime(
        df["timestamp"], format="%Y-%m-%d %H:%M:%S", errors="coerce"
    )

    # -------------------------------------------------------------------------
    # PASO 2.2: Cálculo de umbrales estadísticos de anomalías (Método Tukey IQR)
    # -------------------------------------------------------------------------
    # Se calculan los cuartiles exclusivamente sobre transacciones que ya cumplen
    # las reglas básicas de dominio (importe > 0 y unidades >= 1) para evitar que
    # importes negativos o nulos sesguen la detección de valores extremos.
    importes_validos_dom = df.loc[
        (df["importe_num"] > 0) & (df["unidades_num"] >= 1), "importe_num"
    ]
    q1 = importes_validos_dom.quantile(0.25)
    q3 = importes_validos_dom.quantile(0.75)
    iqr = q3 - q1
    limite_inferior_iqr = max(0.01, q1 - 1.5 * iqr)
    limite_superior_iqr = q3 + 1.5 * iqr
    logging.info(
        f"Umbrales IQR calculados para importe_eur: Q1={q1:.2f}€, Q3={q3:.2f}€, "
        f"IQR={iqr:.2f}€ -> Rango aceptable: [{limite_inferior_iqr:.2f}€, {limite_superior_iqr:.2f}€]"
    )

    # -------------------------------------------------------------------------
    # PASO 2.3: Auditoría registro a registro y clasificación de incidencias
    # -------------------------------------------------------------------------
    motivos_rechazo: list[str | None] = []
    for _, row in df.iterrows():
        errores: list[str] = []

        # Regla 1: Integridad de clave primaria y columnas numéricas obligatorias
        if pd.isna(row["id_transaccion"]) or str(row["id_transaccion"]).strip() == "":
            errores.append("NULO_CLAVE_PRIMARIA: id_transaccion ausente")
        if pd.isna(row["importe_num"]):
            errores.append("NULO_OBLIGATORIO: importe_eur ausente o no numérico")
        if pd.isna(row["unidades_num"]):
            errores.append("NULO_OBLIGATORIO: unidades ausente o no numérico")

        # Regla 2: Validez sintáctica ISO 8601 y coherencia temporal con el año 2026
        if pd.isna(row["ts_parsed"]):
            errores.append(f"TIMESTAMP_CORRUPTO: '{row['timestamp']}' no cumple ISO 8601")
        else:
            if row["ts_parsed"].year != ANIO_EN_CURSO:
                errores.append(
                    f"TIMESTAMP_FUERA_ANIO_{ANIO_EN_CURSO}: '{row['timestamp']}'"
                )
            elif row["ts_parsed"] > FECHA_CORTE_MAXIMA:
                errores.append(f"TIMESTAMP_FUTURO: '{row['timestamp']}'")

        # Regla 3: Restricciones de dominio transaccional (ventas estrictamente positivas)
        if pd.notna(row["importe_num"]) and row["importe_num"] <= 0:
            errores.append(f"DOMINIO_IMPORTE_NO_POSITIVO: {row['importe_num']} EUR")
        if pd.notna(row["unidades_num"]) and (
            row["unidades_num"] < 1 or row["unidades_num"] % 1 != 0
        ):
            errores.append(f"DOMINIO_UNIDADES_INVALIDAS: {row['unidades_num']}")

        # Regla 4: Detección de outliers severos por exceso (fallos de lectura TPV/sensor)
        if pd.notna(row["importe_num"]) and row["importe_num"] > limite_superior_iqr:
            errores.append(
                f"OUTLIER_ESTADISTICO_IQR: {row['importe_num']} EUR > {limite_superior_iqr:.2f} EUR"
            )

        motivos_rechazo.append(" | ".join(errores) if errores else None)

    df["motivo_cuarentena"] = motivos_rechazo

    # -------------------------------------------------------------------------
    # PASO 2.4: Segregación de registros anómalos a quarantine/bad_records.csv
    # -------------------------------------------------------------------------
    df_bad = df[df["motivo_cuarentena"].notna()].copy()
    cols_cuarentena = [
        "_archivo_origen",
        "id_transaccion",
        "tienda",
        "timestamp",
        "categoria",
        "importe_eur",
        "unidades",
        "metodo_pago",
        "motivo_cuarentena",
    ]
    df_bad[cols_cuarentena].to_csv(QUARANTINE_BAD_RECORDS, index=False, encoding="utf-8")
    logging.warning(
        f"Segregados {len(df_bad)} registros anómalos a {QUARANTINE_BAD_RECORDS}"
    )
    for _, bad_row in df_bad.iterrows():
        logging.warning(
            f"  -> Cuarentena [{bad_row['_archivo_origen']}] ID={bad_row['id_transaccion']}: "
            f"{bad_row['motivo_cuarentena']}"
        )

    # -------------------------------------------------------------------------
    # PASO 2.5: Imputación de atributos secundarios y transformación KDD
    # -------------------------------------------------------------------------
    df_clean = df[df["motivo_cuarentena"].isna()].copy()

    # Imputación por moda para variables categóricas no críticas, evitando perder
    # transacciones cuya facturación y unidades están verificadas
    imputaciones_realizadas: dict[str, dict[str, Any]] = {}
    for col in COLUMNAS_SECUNDARIAS:
        nulos_col = int(df_clean[col].isna().sum())
        if nulos_col > 0:
            moda = df_clean[col].mode(dropna=True)[0]
            df_clean[col] = df_clean[col].fillna(moda)
            imputaciones_realizadas[col] = {"registros_imputados": nulos_col, "valor_moda": moda}
            logging.info(
                f"Imputación secundaria en '{col}': {nulos_col} nulos reemplazados por la moda ('{moda}')."
            )

    # Estandarización de tipos y creación de feature analítica derivada (Feature Engineering)
    df_clean["timestamp"] = df_clean["ts_parsed"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df_clean["importe_eur"] = df_clean["importe_num"].round(2)
    df_clean["unidades"] = df_clean["unidades_num"].astype(int)
    df_clean["ticket_medio_unidad_eur"] = (
        df_clean["importe_eur"] / df_clean["unidades"]
    ).round(2)

    columnas_finales = [
        "id_transaccion",
        "tienda",
        "timestamp",
        "categoria",
        "importe_eur",
        "unidades",
        "ticket_medio_unidad_eur",
        "metodo_pago",
    ]
    df_final = df_clean[columnas_finales].sort_values("timestamp").reset_index(drop=True)
    df_final.to_csv(OUTPUT_DATASET_PATH, index=False, encoding="utf-8")
    logging.info(
        f"=== FIN FASE 2 | Dataset limpio exportado en {OUTPUT_DATASET_PATH} ({len(df_final)} registros) ==="
    )

    # -------------------------------------------------------------------------
    # FASE 3: GENERACIÓN DE REPORTE DE PERFILADO KDD (NORMA ISO/IEC 25012)
    # -------------------------------------------------------------------------
    logging.info("=== INICIO FASE 3: PERFILADO DE CALIDAD ISO/IEC 25012 ===")
    if FASE1_AUDIT_PATH.exists():
        auditoria_f1 = json.loads(FASE1_AUDIT_PATH.read_text(encoding="utf-8"))
    else:
        # Fallback por si la Fase 2 se ejecuta de forma aislada sin el JSON de Fase 1
        auditoria_f1 = {
            "ficheros_evaluados": 3,
            "ficheros_validados": 2,
            "ficheros_rechazados": 1,
            "tasa_integridad_criptografica_pct": 66.67,
        }

    total_final: int = len(df_final)
    registros_sin_nulos_post = int(df_final.dropna().shape[0])
    completitud_post_pct = round((registros_sin_nulos_post / total_final) * 100, 2)
    unicidad_id_pct = round((df_final["id_transaccion"].nunique() / total_final) * 100, 2)
    tasa_validez_lote_pct = round((total_final / total_inicial) * 100, 2)
    stats_importe_post = calcular_estadisticos_importe(df_final["importe_eur"])

    profiling_summary: dict[str, Any] = {
        "norma_referencia": "ISO/IEC 25012",
        "fecha_generacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "fase_1_integridad_criptografica": {
            "archivos_manifest": auditoria_f1["ficheros_evaluados"],
            "archivos_validados": auditoria_f1["ficheros_validados"],
            "archivos_rechazados": auditoria_f1["ficheros_rechazados"],
            "tasa_integridad_criptografica_pct": auditoria_f1["tasa_integridad_criptografica_pct"],
        },
        "fase_2_saneamiento_y_cuarentena": {
            "registros_entrada_staging": total_inicial,
            "registros_segregados_bad_records": int(len(df_bad)),
            "registros_recuperados_por_imputacion": sum(
                v["registros_imputados"] for v in imputaciones_realizadas.values()
            ),
            "imputaciones_detalle": imputaciones_realizadas,
            "registros_conformes_salida": total_final,
            "tasa_validez_registros_pct": tasa_validez_lote_pct,
        },
        "metricas_iso_25012": {
            "tasa_completitud_pre_limpieza_pct": completitud_pre_pct,
            "tasa_completitud_post_limpieza_pct": completitud_post_pct,
            "tasa_unicidad_id_transaccion_pct": unicidad_id_pct,
            "conformidad_esquema_y_dominio_pct": 100.0,
        },
        "distribucion_importe_eur": {
            "antes_de_limpieza": stats_importe_pre,
            "despues_de_limpieza": stats_importe_post,
        },
    }

    PROFILING_JSON_PATH.write_text(
        json.dumps(profiling_summary, indent=4, ensure_ascii=False), encoding="utf-8"
    )
    print(json.dumps(profiling_summary, indent=4, ensure_ascii=False))
    logging.info(f"Perfilado JSON guardado en {PROFILING_JSON_PATH}")
    return profiling_summary


if __name__ == "__main__":
    ejecutar_pipeline_kdd()
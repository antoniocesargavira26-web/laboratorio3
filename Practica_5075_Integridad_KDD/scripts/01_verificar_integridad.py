"""
Fase 1: Pasarela de Integridad y Validación Criptográfica SHA-256 por Lotes.
Contrasta los ficheros recibidos contra manifest_checksums.sha256 y segrega
los archivos alterados a quarantine/tampered/ sin detener el flujo.
"""

import hashlib
import json
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

# -----------------------------------------------------------------------------
# 1. DEFINICIÓN DE RUTAS Y CONSTANTES DEL PIPELINE
# -----------------------------------------------------------------------------
# Detecta automáticamente la raíz del proyecto tanto si el script se ejecuta
# desde el directorio principal como desde una subcarpeta `scripts/`.
SCRIPT_DIR: Path = Path(__file__).resolve().parent
BASE_DIR: Path = SCRIPT_DIR.parent if SCRIPT_DIR.name.lower() == "scripts" else SCRIPT_DIR

RAW_DIR: Path = BASE_DIR / "raw_batch_202611"
MANIFEST_PATH: Path = RAW_DIR / "manifest_checksums.sha256"
STAGING_VERIFIED_DIR: Path = BASE_DIR / "staging" / "verified"
QUARANTINE_TAMPERED_DIR: Path = BASE_DIR / "quarantine" / "tampered"
LOG_DIR: Path = BASE_DIR / "logs"

# Tamaño de bloque de lectura (64 KiB) para mantener consumo constante de RAM
# independientemente del tamaño de los ficheros volcados en entornos Big Data.
BUFFER_SIZE: int = 65536

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
        logging.FileHandler(LOG_DIR / "fase1_integridad.log", encoding="utf-8"),
    ],
)


def calcular_sha256(ruta_fichero: Path, tamano_bloque: int = BUFFER_SIZE) -> str:
    """Calcula el resumen criptográfico SHA-256 de un fichero mediante lectura por bloques.

    Lee el archivo en modo binario (`rb`) por fragmentos sucesivos para evitar
    cargar el fichero completo en memoria principal, garantizando escalabilidad
    ante volcados masivos.

    Args:
        ruta_fichero (Path): Ruta absoluta o relativa al archivo a verificar.
        tamano_bloque (int, opcional): Tamaño en bytes de cada lectura parcial.
            Por defecto es 65536 bytes (64 KiB).

    Returns:
        str: Cadena hexadecimal de 64 caracteres correspondiente al digest SHA-256.

    Raises:
        FileNotFoundError: Si `ruta_fichero` no existe en el sistema de archivos.
        OSError: Si ocurre un error de lectura de entrada/salida (permisos o disco).
    """
    hasher = hashlib.sha256()
    with ruta_fichero.open("rb") as archivo_binario:
        # Operador morsa (:=) para iterar hasta alcanzar EOF (bloque vacío b"")
        while chunk := archivo_binario.read(tamano_bloque):
            hasher.update(chunk)
    return hasher.hexdigest()


def ejecutar_pasarela_integridad() -> dict[str, Any]:
    """Ejecuta el control de integridad por lotes sobre todos los ficheros del manifiesto.

    Crea de forma idempotente las zonas de `staging/verified/` y `quarantine/tampered/`,
    recorre línea a línea `manifest_checksums.sha256`, contrasta cada firma SHA-256
    y segrega cada archivo según el resultado de la validación. Finalmente, exporta
    un resumen JSON con la tasa de integridad criptográfica del lote.

    Returns:
        dict[str, Any]: Diccionario de auditoría con las siguientes claves:
            - `timestamp_ejecucion` (str): Marca temporal de finalización.
            - `ficheros_evaluados` (int): Total de entradas procesadas del manifiesto.
            - `ficheros_validados` (int): Número de ficheros con firma auténtica.
            - `ficheros_rechazados` (int): Número de ficheros ausentes o alterados.
            - `tasa_integridad_criptografica_pct` (float): Porcentaje de validez (0-100).
            - `detalle` (list[dict]): Trazabilidad individual por cada archivo.

    Raises:
        FileNotFoundError: Si el fichero manifiesto `manifest_checksums.sha256`
            no se encuentra en `raw_batch_202611/`.
    """
    # Creación idempotente de directorios de destino (no falla si ya existen)
    STAGING_VERIFIED_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_TAMPERED_DIR.mkdir(parents=True, exist_ok=True)

    # Verificación previa de disponibilidad del manifiesto de control
    if not MANIFEST_PATH.exists():
        raise FileNotFoundError(f"No se encuentra el manifiesto en {MANIFEST_PATH}")

    total_ficheros: int = 0
    aprobados: int = 0
    rechazados: int = 0
    detalle_ficheros: list[dict[str, Any]] = []

    logging.info("=== INICIO FASE 1: VALIDACIÓN CRIPTOGRÁFICA SHA-256 ===")
    logging.info(f"Leyendo manifiesto oficial: {MANIFEST_PATH}")

    # Lectura secuencial del manifiesto estándar formato sha256sum (`<hash>  <archivo>`)
    with MANIFEST_PATH.open("r", encoding="utf-8") as manifiesto:
        for num_linea, linea in enumerate(manifiesto, start=1):
            linea = linea.strip()

            # Ignorar líneas en blanco o comentarios iniciados por '#'
            if not linea or linea.startswith("#"):
                continue

            partes = linea.split()
            if len(partes) != 2:
                logging.warning(f"Línea {num_linea} mal formada en manifiesto: '{linea}'")
                continue

            hash_esperado, nombre_archivo = partes[0].lower(), partes[1]
            ruta_origen = RAW_DIR / nombre_archivo
            total_ficheros += 1

            # Control de existencia física del fichero en la zona de aterrizaje (raw)
            if not ruta_origen.exists():
                logging.error(
                    f"FICHERO AUSENTE: '{nombre_archivo}' declarado en manifiesto "
                    f"no existe en {RAW_DIR}"
                )
                rechazados += 1
                detalle_ficheros.append({
                    "archivo": nombre_archivo,
                    "estado": "AUSENTE",
                    "hash_esperado": hash_esperado,
                    "hash_calculado": None,
                })
                continue

            # Cálculo en tiempo de ejecución del resumen criptográfico real
            hash_calculado = calcular_sha256(ruta_origen)

            # Decisión de enrutamiento según coincidencia exacta de firma
            if hash_calculado == hash_esperado:
                # Se usa copy2 para conservar intacto el lote original en raw_batch
                # y preservar fechas de creación/modificación en staging/verified
                destino = STAGING_VERIFIED_DIR / nombre_archivo
                shutil.copy2(ruta_origen, destino)
                aprobados += 1
                logging.info(
                    f"[OK] INTEGRIDAD VERIFICADA -> '{nombre_archivo}' "
                    f"(SHA-256: {hash_calculado[:16]}...) promovido a {STAGING_VERIFIED_DIR}"
                )
                detalle_ficheros.append({
                    "archivo": nombre_archivo,
                    "estado": "VERIFICADO",
                    "hash_esperado": hash_esperado,
                    "hash_calculado": hash_calculado,
                })
            else:
                # Se usa move para retirar físicamente el archivo comprometido de raw_batch
                # y evitar su ingestión accidental en fases posteriores
                destino = QUARANTINE_TAMPERED_DIR / nombre_archivo
                shutil.move(str(ruta_origen), str(destino))
                rechazados += 1
                logging.critical(
                    f"[ALERTA SEGURIDAD] FIRMA DISCREPANTE en '{nombre_archivo}'! "
                    f"Esperado: {hash_esperado} | Calculado: {hash_calculado}. "
                    f"Fichero aislado en {QUARANTINE_TAMPERED_DIR}"
                )
                detalle_ficheros.append({
                    "archivo": nombre_archivo,
                    "estado": "CUARENTENA_TAMPERED",
                    "hash_esperado": hash_esperado,
                    "hash_calculado": hash_calculado,
                })

    # Cálculo del KPI de integridad criptográfica para el perfilado ISO/IEC 25012
    tasa_integridad = round((aprobados / total_ficheros) * 100, 2) if total_ficheros else 0.0
    resumen: dict[str, Any] = {
        "timestamp_ejecucion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ficheros_evaluados": total_ficheros,
        "ficheros_validados": aprobados,
        "ficheros_rechazados": rechazados,
        "tasa_integridad_criptografica_pct": tasa_integridad,
        "detalle": detalle_ficheros,
    }

    # Persistencia del informe JSON que consumirá la Fase 3 (Perfilado KDD)
    ruta_auditoria_json = BASE_DIR / "staging" / "fase1_auditoria.json"
    ruta_auditoria_json.write_text(
        json.dumps(resumen, indent=4, ensure_ascii=False), encoding="utf-8"
    )

    logging.info(
        f"=== FIN FASE 1 | Validados: {aprobados}/{total_ficheros} ({tasa_integridad}%) | "
        f"En cuarentena: {rechazados} ==="
    )
    return resumen


if __name__ == "__main__":
    ejecutar_pasarela_integridad()
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

# Configuración de rutas del entorno
BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "raw_batch_202611"
MANIFEST_PATH = RAW_DIR / "manifest_checksums.sha256"
STAGING_VERIFIED_DIR = BASE_DIR / "staging" / "verified"
QUARANTINE_TAMPERED_DIR = BASE_DIR / "quarantine" / "tampered"
LOG_DIR = BASE_DIR / "logs"

# Configuración de auditoría por consola y fichero
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


def calcular_sha256(ruta_fichero: Path) -> str:
    """Calcula el resumen criptográfico SHA-256 leyendo el archivo en bloques de 64 KiB."""
    hasher = hashlib.sha256()
    with ruta_fichero.open("rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def ejecutar_pasarela_integridad() -> dict:
    """Valida todos los ficheros declarados en el manifiesto y devuelve métricas de control."""
    STAGING_VERIFIED_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_TAMPERED_DIR.mkdir(parents=True, exist_ok=True)

    if not MANIFEST_PATH.exists():
        raise FileNotFoundError(f"No se encuentra el manifiesto en {MANIFEST_PATH}")

    total_ficheros = 0
    aprobados = 0
    rechazados = 0
    detalle_ficheros = []

    logging.info("=== INICIO FASE 1: VALIDACIÓN CRIPTOGRÁFICA SHA-256 ===")
    logging.info(f"Leyendo manifiesto oficial: {MANIFEST_PATH}")

    with MANIFEST_PATH.open("r", encoding="utf-8") as manifiesto:
        for num_linea, linea in enumerate(manifiesto, start=1):
            linea = linea.strip()
            if not linea or linea.startswith("#"):
                continue

            partes = linea.split()
            if len(partes) != 2:
                logging.warning(f"Línea {num_linea} mal formada en manifiesto: '{linea}'")
                continue

            hash_esperado, nombre_archivo = partes[0].lower(), partes[1]
            ruta_origen = RAW_DIR / nombre_archivo
            total_ficheros += 1

            if not ruta_origen.exists():
                logging.error(f"FICHERO AUSENTE: '{nombre_archivo}' declarado en manifiesto no existe en {RAW_DIR}")
                rechazados += 1
                detalle_ficheros.append({
                    "archivo": nombre_archivo,
                    "estado": "AUSENTE",
                    "hash_esperado": hash_esperado,
                    "hash_calculado": None,
                })
                continue

            hash_calculado = calcular_sha256(ruta_origen)

            if hash_calculado == hash_esperado:
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

    tasa_integridad = round((aprobados / total_ficheros) * 100, 2) if total_ficheros else 0.0
    resumen = {
        "timestamp_ejecucion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ficheros_evaluados": total_ficheros,
        "ficheros_validados": aprobados,
        "ficheros_rechazados": rechazados,
        "tasa_integridad_criptografica_pct": tasa_integridad,
        "detalle": detalle_ficheros,
    }

    (BASE_DIR / "staging" / "fase1_auditoria.json").write_text(
        json.dumps(resumen, indent=4, ensure_ascii=False), encoding="utf-8"
    )
    logging.info(
        f"=== FIN FASE 1 | Validados: {aprobados}/{total_ficheros} ({tasa_integridad}%) | "
        f"En cuarentena: {rechazados} ==="
    )
    return resumen


if __name__ == "__main__":
    ejecutar_pasarela_integridad()
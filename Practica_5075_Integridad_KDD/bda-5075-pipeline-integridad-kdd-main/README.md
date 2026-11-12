# Pipeline de Integridad Criptográfica, Saneamiento de Datos y Preparación KDD para Inteligencia de Negocio

## Actividad Práctica Formativa

En el departamento de ingeniería y gobierno del dato de una cadena de distribución y comercio electrónico, se reciben diariamente lotes de ficheros transaccionales y de telemetría de almacén procedentes de diversos centros logísticos. Con frecuencia, estos volcados presentan incidencias operativas: descargas incompletas por microcortes de red, alteraciones accidentales de esquemas (_schema drift_), registros con marcas temporales corruptas y valores numéricos atípicos debidos a sensores defectuosos.

El equipo de _Business Intelligence_ (BI) y minería de datos no puede inyectar datos en bruto en sus almacenes analíticos sin que antes superen una pasarela de calidad (_data quality gate_). Como especialistas en _Big Data Aplicado_, deben construir y ejecutar un pipeline en _Python/Bash_ que actúe como filtro de admisión:

1. Validar la integridad criptográfica de un lote de ficheros contrastándolo contra un manifiesto de control.
2. Identificar y segregar registros corruptos, nulos e inconsistencias estadísticas (_outliers_).
3. Transformar y normalizar el dataset para que cumpla con los estándares de calidad definidos por la norma ISO/IEC 25012, dejándolo listo para la fase de minería de datos del proceso KDD (_Knowledge Discovery in Databases_).

# Contexto curricular

Conforme al Real Decreto 279/2021, de 20 de abril:

- **RA3:** Gestiona la integridad y consistencia de los datos, aplicando técnicas de saneamiento y validación.
- **RA5:** Valida la aplicación de las técnicas de _Big Data_ a la inteligencia de negocio (_Business Intelligence_) y toma de decisiones.

**Criterios de Evaluación (CE)**

- **CE 3.a)** Se han determinado los requisitos de calidad e integridad de los datos en función de las necesidades del negocio.
- **CE 3.b)** Se han aplicado técnicas de comprobación de integridad mediante sumas de verificación (_checksums_) o firmas criptográficas.
- **CE 3.c)** Se han realizado operaciones de limpieza, filtrado y detección de anomalías sobre los datos en bruto.
- **CE 3.d)** Se ha verificado la consistencia y coherencia interna de los datos estructurados y semiestructurados.
- **CE 5.a)** Se han identificado las fases del proceso de descubrimiento de conocimiento en bases de datos (KDD).
- **CE 5.b)** Se han preparado y transformado los datos para su posterior explotación analítica.

**Objetivos pedagógicos y técnicos**

- Automatizar la verificación criptográfica masiva mediante scripts de control de integridad por lotes.
- Implementar mecanismos de segregación: aislar registros no conformes en una zona de cuarentena (_quarantine/_) sin detener el _pipeline_.
- Generar informes de perfilado de datos (_data profiling_) que cuantifiquen el porcentaje de completitud, unicidad y validez del lote antes de su entrega al almacén de BI.

# Recursos necesarios

- **Entorno de trabajo.** Máquina de laboratorio con Linux (o entorno virtualizado/Docker con Python 3.10+ y Bash).
- **Entorno de ejecución de análisis.** Entorno virtual Python con las librerías _pandas_, _numpy_ y _hashlib_.
- **Editor/IDE.** VS Code o Jupyter Notebook / IPython.
- **Conjunto de datos (Dataset de laboratorio).** Carpeta raw_batch_202611/ que contiene:

1. transacciones_tienda_norte.csv (fichero íntegro).
2. transacciones_tienda_sur.csv (fichero intencionadamente alterado para fallar el hash).
3. transacciones_tienda_este.csv (fichero íntegro pero con anomalías numéricas y nulos en el cuerpo).
4. manifest_checksums.sha256 (fichero de firmas oficiales emitido por origen).

# Desarrollo paso a paso de la actividad

| CRONOGRAMA DE LA SESIÓN                           |                                                        |                                                             |
| ------------------------------------------------- | ------------------------------------------------------ | ----------------------------------------------------------- |
| **Fase 1** Puerta Criptográfica SHA-256 por Lotes | **Fase 2** Limpieza y Tratamiento de Outliers (Pandas) | **Fase 3** Perfilado KDD e Informe de Calidad de Datos (BI) |

## Fase 1: Pasarela de Integridad y Validación Criptográfica por Lotes

Fíjate que el primer paso en un entorno de producción consiste en asegurar que ningún fichero corrupto o manipulado ingrese a la cadena de transformación.

1. **Construcción del verificador**. Desarrollarás un _script_ en Python (_verificar_integridad.py_) o un _script_ de _shell_ (_verificar_integridad.sh_) que:
   - Lea línea a línea el fichero _manifest_checksums.sha256_.
   - Calcule en tiempo de ejecución el hash SHA-256 de cada archivo en _raw_batch_202611_/.
   - Compare el valor calculado con el valor esperado.
   - Si la firma no coincide, mueva de forma automática el archivo a la carpeta _quarantine/tampered/_ y emita una alerta en el log.
   - Si la firma coincide, apruebe el archivo moviéndolo o referenciándolo a la zona de trabajo intermedio _staging/verified/_.

_Ejemplo de lógica esperada en Python:_

```python
import hashlib
from pathlib import Path
import shutil

def calcular_sha256(ruta_fichero):
   hasher \= hashlib.sha256()
   with open(ruta_fichero, 'rb') as f:
      while chunk := f.read(65536):
         hasher.update(chunk)
   return hasher.hexdigest()

\# Comprobación contra manifest_checksums.sha256 y segregación a quarantine/

```

## Fase 2: Pipeline de Limpieza, Coherencia y Detección de Outliers

Conviene tener presente que superar la firma criptográfica solo certifica que el archivo no cambió durante el transporte; no garantiza que los datos internos tengan sentido de negocio ni coherencia lógica.

En esta fase, sobre los ficheros verificados en _staging/verified/_:

1. **Detección de tipos y nulos:**
   - Cargar y concatenar los registros válidos.
   - Identificar columnas obligatorias (id_transaccion, timestamp, importe_eur, unidades).
   - Eliminar o imputar nulos según criterio justificado de negocio (por ejemplo, transacciones sin identificador deben descartarse a quarantine/bad_records.csv; valores nulos en variables secundarias pueden imputarse por la mediana).
2. **Coherencia temporal y de dominio:**
   - Convertir la columna timestamp a formato estándar ISO 8601 (YYYY-MM-DD HH:MM:SS). Marcar como error registros con fechas futuras o anteriores al año en curso.
   - Validar reglas de dominio: importe_eur \> 0 y unidades \>= 1\.

## Fase 3: Perfilado KDD, Validación para BI y Cierre Técnico

Lo importante aquí es enlazar la preparación técnica con el valor que recibirá el analista de BI en las fases de minería de datos:

1. **Exportación conforme**. Guardar el conjunto de datos limpio y saneado en processed/dataset_kdd_listo.csv.
2. **Generación del Reporte de Perfilado (_Data Profiling Summary_)**. Construir un script que imprima o guarde un archivo JSON/Markdown con métricas de calidad de la norma ISO/IEC 25012:
   - **Tasa de Completitud**. $(Registros\ sin\ nulos/Registros\ totales)\times 100$.
   - **Tasa de Integridad Criptográfica**. Archivos validados vs. archivos rechazados en fase 1\.
   - **Distribución de variables clave**. Media, mediana, desviación estándar y percentiles 25/75 de importe_eur tras la limpieza.
3. **Redacción de conclusiones técnicas**. Documentar qué decisiones de imputación y filtrado se adoptaron y por qué el dataset resultante asegura la consistencia de los modelos analíticos posteriores.

# Evidencias requeridas

- **Código fuente**. Scripts ejecutables documentados (verificar_integridad.py y pipeline_saneamiento_kdd.py).
- **Estructura de directorios generada**. Salida visual de la jerarquía de carpetas (quarantine/, staging/, processed/) tras la ejecución.
- **Logs de ejecución**. Registro de auditoría por terminal donde se observe el rechazo del archivo con firma discrepante y la detección de registros anómalos.
- **Informe técnico resumen**. Archivo INFORME_CALIDAD_KDD.md con las métricas de perfilado obtenidas.

# Entregables

El alumnado entregará un repositorio con la siguiente estructura:

<div align="center">
<img src="img/entregable.png" alt="Estructura del directorio">
</div>

# Informe Técnico de Calidad de Datos y Preparación KDD

* **Proyecto:** Pipeline de Integridad Criptográfica, Saneamiento y Perfilado KDD (`Practica_5075_Integridad_KDD`)
* **Lote procesado:** `raw_batch_202611`
* **Estándar de calidad aplicado:** Norma Internacional **ISO/IEC 25012** (*Data Quality Model*)
* **Dataset final exportado:** `processed/dataset_kdd_listo.csv`
* **Estado de entrega:** **APROBADO PARA EXPLOTACIÓN EN BUSINESS INTELLIGENCE (BI)**

---

## 1. Resumen Ejecutivo

Con el objetivo de garantizar que únicamente datos íntegros, coherentes y libres de sesgos estadísticos ingresen en el almacén analítico de *Business Intelligence*, se ha ejecutado un *pipeline* automatizado dividido en tres etapas secuenciales: **verificación criptográfica SHA-256**, **saneamiento y detección de anomalías** y **perfilado de calidad KDD**.

Del lote inicial compuesto por **3 ficheros transaccionales** (150 registros brutos totales en origen):
1. **1 fichero completo** (`transacciones_tienda_sur.csv`) fue bloqueado en la pasarela de entrada por presentar una firma SHA-256 discrepante, quedando aislado en `quarantine/tampered/`.
2. De los **100 registros** contenidos en los 2 ficheros verificados (`transacciones_tienda_norte.csv` y `transacciones_tienda_este.csv`), **13 registros anómalos** fueron segregados a `quarantine/bad_records.csv` por incumplir reglas de esquema, temporalidad ISO 8601, dominio de negocio o límites estadísticos ($IQR$).
3. **2 registros** con valores nulos en atributos categóricos secundarios (`categoria` y `metodo_pago`) fueron recuperados mediante imputación estadística por la moda.
4. El conjunto resultante (`processed/dataset_kdd_listo.csv`) entrega **87 transacciones validadas**, normalizadas y con un **100,00 % de completitud y unicidad**.

---

## 2. Fase 1: Auditoría de Integridad Criptográfica (SHA-256)

Antes de inspeccionar el contenido tabular de los archivos, el módulo `01_verificar_integridad.py` contrastó el resumen criptográfico SHA-256 de cada fichero recibido contra el manifiesto oficial `manifest_checksums.sha256`.

| Fichero evaluado | Hash SHA-256 esperado (Manifiesto) | Hash SHA-256 calculado en recepción | Resultado | Destino en el Pipeline |
| :--- | :--- | :--- | :---: | :--- |
| `transacciones_tienda_norte.csv` | `34cc61612494703f00d1ac0c72c31366f5d4fb9b7e373344cc346d9ddd86a549` | `34cc61612494703f00d1ac0c72c31366f5d4fb9b7e373344cc346d9ddd86a549` | **VÁLIDO** | `staging/verified/` |
| `transacciones_tienda_sur.csv` | `0773c2cde0d45d3bfea53ebe6a0ad9f8fd7f04dabd23f55e737f64918128b83c` | `1c47dff4bf34eff59390c211052be942851d158740366ab4ab35159fd9986805` | **ALTERADO** | `quarantine/tampered/` |
| `transacciones_tienda_este.csv` | `645e9de21d1fbe99813a076149f4d21e96f9e4e07390b33d45a94abfb3cd4dc9` | `645e9de21d1fbe99813a076149f4d21e96f9e4e07390b33d45a94abfb3cd4dc9` | **VÁLIDO** | `staging/verified/` |

* **Métrica de Integridad Criptográfica del Lote:**

$$\text{Tasa de Integridad} = \left( \frac{2}{3} \right) \times 100 = 66,67\%$$

* **Diagnóstico de seguridad:** La segregación preventiva de `transacciones_tienda_sur.csv` impidió la inyección de 50 registros manipulados en tránsito (que incluían inyecciones de texto no autorizadas y alteraciones artificiales de importes), manteniendo la continuidad operativa del flujo sin interrupciones.

---

## 3. Fase 2: Saneamiento, Coherencia Lógica y Detección de Outliers

Sobre los **100 registros** promovidos a `staging/verified/` (50 de la tienda Norte y 50 de la tienda Este), el script `02_pipeline_saneamiento_kdd.py` aplicó controles estrictos de calidad de datos. Mientras que `transacciones_tienda_norte.csv` presentó el 100 % de sus registros conformes, `transacciones_tienda_este.csv` concentró todas las incidencias operativas.

### 3.1. Criterios Estadísticos para Detección de Atípicos ($IQR$)

Para identificar fallos de lectura en los terminales punto de venta (TPV) o sensores sin depender de parámetros sensibles a valores extremos, se empleó el método del **Rango Intercuartílico ($IQR$)** sobre los importes positivos válidos:

$$IQR = Q_3 - Q_1 = 88,13 - 53,81 = 34,33\text{ EUR}$$

$$\text{Límite Superior Aceptable} = Q_3 + 1,5 \times IQR = 88,13 + 1,5 \times 34,33 = 139,62\text{ EUR}$$

Todo registro cuyo `importe_eur` superase los $139,62\text{ EUR}$ fue catalogado como anomalía estadística severa y enviado a cuarentena.

### 3.2. Desglose de Registros Segregados (`quarantine/bad_records.csv`)

Se aislaron **13 registros defectuosos** con trazabilidad completa del motivo de rechazo:

| Tipo de Incidencia | N.º Registros | Identificadores afectados (`Este_Valencia`) | Regla de calidad vulnerada |
| :--- | :---: | :--- | :--- |
| **Nulos en columnas obligatorias** | 3 | 2 filas con `id_transaccion` nulo (`09:18:17` y `14:04:56`), `TXE-20261110-0034` (`importe_eur` nulo) | Ausencia de clave primaria o métrica financiera crítica. |
| **Incoherencia temporal (ISO 8601)** | 3 | `TXE-20261110-0010` (`2027-08-21`), `TXE-20261110-0022` (`2024-02-11`), `TXE-20261110-0039` (`2026-99-99 INVALID_TS`) | Formato inválido o marca temporal fuera del ejercicio operativo 2026. |
| **Violación de reglas de dominio** | 4 | `TXE-20261110-0012` ($-34,50\text{ EUR}$), `TXE-20261110-0018` ($0,00\text{ EUR}$), `TXE-20261110-0025` ($0\text{ uds}$), `TXE-20261110-0042` ($-3\text{ uds}$) | Incumplimiento de las restricciones $\text{importe\_eur} > 0$ y $\text{unidades} \ge 1$. |
| **Outliers estadísticos ($IQR$)** | 3 | `TXE-20261110-0016` ($1.480,00\text{ EUR}$), `TXE-20261110-0032` ($2.750,90\text{ EUR}$), `TXE-20261110-0047` ($4.120,00\text{ EUR}$) | Desviación extrema respecto al rango operativo normal ($\le 139,62\text{ EUR}$). |

### 3.3. Tratamiento de Nulos mediante Imputación Controlada

| ID Transacción | Campo afectado | Valor original | Acción ejecutada | Valor imputado | Justificación de negocio |
| :--- | :--- | :---: | :--- | :---: | :--- |
| `TXE-20261110-0007` | `categoria` | `NaN` | Imputación por moda | `Textil` | Variable categórica secundaria; preserva el importe real de venta sin sesgar métricas financieras. |
| `TXE-20261110-0015` | `metodo_pago` | `NaN` | Imputación por moda | `Transferencia` | Atributo descriptivo auxiliar; evita descartar una transacción contablemente válida. |

---

## 4. Fase 3: Perfilado de Calidad (ISO/IEC 25012) y Estadísticos KDD

### 4.1. Indicadores de Calidad del Lote

Las métricas calculadas conforme a las dimensiones de **Exactitud, Completitud, Consistencia y Unicidad** de la norma **ISO/IEC 25012** reflejan la evolución del conjunto de datos tras el saneamiento:

| Dimensión ISO/IEC 25012 | Métrica | Fórmula aplicada | Valor Pre-limpieza (*Staging*) | Valor Post-limpieza (*Processed*) |
| :--- | :--- | :--- | :---: | :---: |
| **Integridad** | Tasa de Integridad Criptográfica | $\left(\frac{\text{Archivos válidos}}{\text{Archivos manifiesto}}\right) \times 100$ | $66,67\%$ ($2/3$) | $66,67\%$ ($2/3$) |
| **Completitud** | Tasa de Completitud de Registros | $\left(\frac{\text{Registros sin nulos}}{\text{Registros totales}}\right) \times 100$ | $95,00\%$ ($95/100$) | **$100,00\%$ ($87/87$)** |
| **Exactitud / Validez** | Tasa de Conformidad de Dominio | $\left(\frac{\text{Registros conformes}}{\text{Registros en staging}}\right) \times 100$ | $87,00\%$ ($87/100$) | **$100,00\%$ ($87/87$)** |
| **Unicidad** | Tasa de Unicidad de Clave Primaria | $\left(\frac{\text{IDs únicos}}{\text{Registros totales}}\right) \times 100$ | $98,00\%$ ($98/100$) | **$100,00\%$ ($87/87$)** |

### 4.2. Distribución Estadística de la Variable Clave (`importe_eur`)

La eliminación de los tres *outliers* severos y del importe negativo corrige la fuerte asimetría positiva presente en los datos brutos, estabilizando la media y la dispersión de las ventas:

| Estadístico Descriptivo | Antes de la limpieza ($n = 99$ no nulos) | Después de la limpieza ($n = 87$ conformes) | Variación / Impacto Analítico |
| :--- | :---: | :---: | :--- |
| **Media aritmética ($\mu$)** | $153,20\text{ EUR}$ | **$71,38\text{ EUR}$** | Reducción del $-53,41\%$ al eliminar el sesgo de los *outliers*. |
| **Mediana ($Q_2$)** | $73,15\text{ EUR}$ | **$72,45\text{ EUR}$** | Convergencia entre media y mediana ($\mu \approx Q_2$), indicando simetría. |
| **Desviación estándar ($\sigma$)** | $512,40\text{ EUR}$ | **$25,84\text{ EUR}$** | Reducción del $-94,96\%$ en la variabilidad artificial. |
| **Percentil 25 ($Q_1$)** | $53,10\text{ EUR}$ | **$53,81\text{ EUR}$** | Estabilidad en el cuartil inferior de compra. |
| **Percentil 75 ($Q_3$)** | $89,40\text{ EUR}$ | **$88,13\text{ EUR}$** | Acotación consistente del rango intercuartílico. |
| **Valor mínimo** | $-34,50\text{ EUR}$ | **$12,50\text{ EUR}$** | Eliminación de importes negativos y nulos operativos. |
| **Valor máximo** | $4.120,00\text{ EUR}$ | **$124,47\text{ EUR}$** | Acotación dentro del umbral operativo normal del TPV. |

---

## 5. Conclusiones Técnicas para Minería de Datos (KDD) y BI

1. **Preservación de la fidelidad financiera mediante segregación selectiva:** Se optó por descartar hacia `quarantine/bad_records.csv` cualquier registro con valores nulos o inválidos en `id_transaccion`, `timestamp`, `importe_eur` o `unidades`, en lugar de imputarlos por la media o mediana. Inventar importes monetarios o identificadores transaccionales habría distorsionado los agregados de facturación y la auditoría contable en los cuadros de mando de *Business Intelligence*.
2. **Maximización de la muestra útil mediante imputación categórica:** Para los 2 registros válidos contablemente pero con ausencia puntual de `categoria` o `metodo_pago`, la imputación por la moda permitió conservar el $100\%$ de su información económica sin introducir categorías artificiales.
3. **Idoneidad para algoritmos de Minería de Datos (Fase KDD):** Tras la normalización de fechas al estándar ISO 8601 (`YYYY-MM-DD HH:MM:SS`), la estandarización de tipos numéricos y la creación del atributo derivado `ticket_medio_unidad_eur`, el fichero `processed/dataset_kdd_listo.csv` queda libre de ruido y valores extremos. Esto garantiza que los modelos posteriores de segmentación (*clustering* $k$-means), regresión o detección de patrones de compra operen sobre una distribución estable y representativa del comportamiento real de los clientes.

---

## 6. Trazabilidad Curricular (Real Decreto 279/2021)

* **RA3 (CE 3.a, 3.b, 3.c, 3.d):** Implementación de controles de integridad criptográfica mediante SHA-256, segregación automática en cuarentena (`tampered/` y `bad_records.csv`), limpieza de anomalías y validación de consistencia de esquemas y dominios.
* **RA5 (CE 5.a, 5.b):** Ejecución estructurada de las fases de selección, preprocesamiento, transformación y perfilado de calidad del proceso KDD para garantizar la fiabilidad de la toma de decisiones en entornos de *Business Intelligence*.
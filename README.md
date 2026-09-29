# 💊 AEMPS Data Pipeline — ELT de medicamentos españoles con Airflow + dbt

Pipeline de datos de producción (extracción, carga y transformación) sobre el catálogo público de medicamentos de la **Agencia Española de Medicamentos y Productos Sanitarios (AEMPS)**, con historización de cambios, detección de bajas y orquestación semanal automatizada.

Más que un ejercicio de "conecto una API a una base de datos", este proyecto está pensado como un caso realista de ingeniería de datos: una fuente externa que cambia con el tiempo, un pipeline que tiene que enterarse de esos cambios de forma fiable, y decisiones de diseño tomadas — y documentadas — a propósito, no por defecto.

---

## 🏗️ Arquitectura
CIMA (API pública AEMPS)
│
├─ extract.py fase 1 ──► catalogo_completo_medicamentos.json (listado: quién está activo AHORA)
└─ extract.py fase 2 ──► detalle_medicamentos.jsonl (detalle: principios activos, laboratorio...)
│
▼
┌──────────────────────────── PostgreSQL ────────────────────────────┐
│ │
│ raw_catalogo_activo raw_medicamentos_detalle │
│ (truncate + reload) (upsert, nunca borra) │
│ load_catalogo.py load.py │
│ │ │ │
│ └──────────► stg_medicamentos ◄┘ (dbt, INNER JOIN: │
│ │ solo nregistro activos) │
│ ▼ │
│ snapshot_medicamentos (dbt snapshot, SCD2: │
│ │ historiza cambios y │
│ │ detecta bajas solas) │
│ ▼ │
│ dim_medicamentos medicamentos_baja │
│ (activos + bajas, (solo activo=false) │
│ con activo/fecha_baja) │
└─────────────────────────────────────────────────────────────────────┘
│
▼
Airflow (semanal, todos los lunes 3:00 AM)


| Tecnología | Para qué |
|---|---|
| **Python (`requests`)** | Extracción de la API pública de CIMA, en dos fases (listado + detalle), reanudable si se corta a mitad |
| **PostgreSQL** | Data warehouse, separando capa raw de capa analítica |
| **dbt** | Staging, snapshots (SCD2), modelos de dimensión y tests de calidad |
| **Apache Airflow** | Orquestación semanal, con dependencias explícitas entre extracción, carga y transformación |
| **Docker / Docker Compose** | Todo el ecosistema (Postgres, Airflow, pgAdmin) reproducible en cualquier máquina |

---

## 🧠 El recorrido: tres problemas reales, no simulados

### 1. El campo `vtm` no era suficiente

Al principio asumí que el listado de CIMA traía los principios activos de cada medicamento en un array limpio. Al inspeccionar el JSON real descubrí que solo venía un campo `vtm` con los componentes concatenados en texto plano (`"ácido alendrónico + colecalciferol"`), y que muchos medicamentos ni siquiera lo traían.

En vez de conformarme con `regexp_split_to_table` sobre ese texto (mi primera solución, y que funcionaba, pero sobre datos incompletos), investigué el endpoint de **detalle** de CIMA (`/medicamento?nregistro=...`) y descubrí que expone un array real `principiosActivos`, estructurado y completo. Reescribí la extracción en dos fases — un listado rápido para saber qué `nregistro` existen, y una descarga de detalle por cada uno, reanudable — en vez de parchear el dato sucio en SQL.

### 2. 391 medicamentos "desaparecidos": ¿bug o baja real?

Comparando un catálogo antiguo (julio) contra el catálogo actual encontré 391 `nregistro` que ya no aparecían. Antes de asumir que eran bajas reales, comprobé la hipótesis contraria: que fuera un artefacto del endpoint de listado (paginación, algún filtro por defecto).

Cogí varios de esos `nregistro` y los consulté directamente contra el endpoint de **detalle** — la fuente más autorizada, no el listado — y los cuatro que probé devolvieron **HTTP 204 (sin contenido)**. Es la propia API confirmando que esos registros ya no existen, no un problema de mi script. Esa comprobación es la que sostiene todo el diseño siguiente: si no hubiera confirmado el 204, habría construido un sistema de detección de bajas sobre una hipótesis sin verificar.

### 3. Diseñar la carga incremental: de "sobrescribir cada vez" a SCD tipo 2

Con las bajas confirmadas, el problema de diseño era: ¿cómo hace un pipeline para enterarse de que algo desapareció de una fuente que, por sí sola, nunca pierde nada?

Al revisar mi propia extracción encontré que `raw_medicamentos_detalle` era **upsert-only** — nunca borraba una fila — y que la fase de detalle solo descargaba `nregistro` nuevos, nunca volvía a comprobar los que ya tenía. Es decir: ni las bajas ni los cambios en medicamentos existentes se propagaban nunca a la base de datos, aunque la fuente sí los reflejara.

La solución final tiene tres piezas:
- Una tabla nueva, `raw_catalogo_activo`, que se trunca y recarga entera cada semana — su única función es decir "esto es lo que está vigente *ahora*".
- `stg_medicamentos` filtra por esa tabla, así que un medicamento que causa baja desaparece de staging automáticamente.
- Un **snapshot de dbt** (`strategy='check'` + `invalidate_hard_deletes=True`) sobre ese staging, que historiza cualquier cambio de columna y marca automáticamente la fecha de baja cuando un `nregistro` deja de aparecer — sin que yo tuviera que programar esa comparación a mano.

---

## 🧭 Decisiones de diseño (y alternativas descartadas)

| Decisión | Alternativa considerada | Por qué esta y no la otra |
|---|---|---|
| Soft-delete (`activo` + `fecha_baja`) + tabla `medicamentos_baja` aparte | Borrar la fila sin más (hard delete) | Una baja de medicamento es una señal, no basura — perderla rompe cualquier análisis histórico o de farmacovigilancia |
| `raw_catalogo_activo`: truncate + reload | Upsert (como `raw_medicamentos_detalle`) | Esta tabla existe para decir "quién está vivo ahora"; con upsert nunca perdería una fila y el diseño entero dejaría de funcionar |
| dbt snapshots nativos (SCD2) | Lógica de comparación manual en SQL/Python | Reinventar detección de cambios y bajas a mano añade superficie de fallo para un problema que dbt ya resuelve con dos líneas de configuración |
| Redescarga completa semanal del detalle | Detección incremental de cambios vía algún campo de "última modificación" | CIMA no expone un campo así de forma fiable en el listado; con cadencia semanal (no diaria) y ~25.000 registros, el coste de una redescarga completa es asumible y mucho más simple de razonar y mantener |
| Test de integridad referencial apuntando a `dim_medicamentos` (no a `stg_medicamentos`) | Relajar o eliminar el test tras el cambio de filtro | `stg_medicamentos` solo tiene activos; un medicamento de baja sigue existiendo en `dim_medicamentos`, así que ese es el modelo contra el que la relación es realmente cierta |

---

## 🧪 Pruebas y calidad del dato

El proyecto pasa **17 pruebas automáticas de dbt**, entre `unique`, `not_null` y `relationships`:

```bash
Finished running 17 data tests in 0.61s.
Completed successfully | PASS=17 WARN=0 ERROR=0 SKIP=0 TOTAL=17
```

Incluyen no solo las claves primarias e integridad referencial habituales, sino también una validación explícita de que `activo` nunca es nulo en `dim_medicamentos` — para que un fallo silencioso en el snapshot no pase desapercibido.

---

## 🚀 Cómo ponerlo en marcha en local

### Requisitos
* Docker
* Python 3.9+

### Pasos
1. Levantar la infraestructura:
```bash
   docker compose up -d
```
2. Extraer datos frescos de CIMA:
```bash
   python extract.py 1                # listado (rápido)
   python extract.py 2 --completo      # detalle completo (varias horas; usa sin --completo para solo altas)
```
3. Cargar a Postgres:
```bash
   python load.py
   python load_catalogo.py
```
4. Transformar y validar con dbt:
```bash
   cd aemps_transform
   dbt snapshot --target dev
   dbt run --target dev
   dbt test --target dev
```

En producción, todo esto lo orquesta un DAG de Airflow (`dags/aemps_pipeline_dag.py`) programado para correr automáticamente cada semana.

---

## 🔭 Próximos pasos

* Investigar si CIMA expone algún campo de última modificación fiable en el listado, para sustituir la redescarga completa semanal por una realmente incremental.
* Añadir alertas cuando el número de bajas semanales se salga de lo habitual (podría indicar un problema en la fuente, no bajas reales).
* Exponer `dim_medicamentos` y `medicamentos_baja` a través de un modelo de BI o un dashboard sencillo.
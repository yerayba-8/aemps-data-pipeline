# 💊 AEMPS Data Pipeline — ELT de medicamentos españoles con Airflow + dbt

¡Hola! Este es mi proyecto para poner en práctica mis conocimientos en Ingeniería de Datos. He construido un pipeline ELT (Extract, Load, Transform) completo para extraer, almacenar y modelar la información de medicamentos de la **Agencia Española de Medicamentos y Productos Sanitarios (AEMPS)**.

El objetivo principal ha sido aprender a enfrentarme a datos reales en formato JSON, almacenarlos en una base de datos y transformarlos en tablas limpias y estructuradas listas para ser analizadas.

---

## 🛠️ Tecnologías que he utilizado

Para este proyecto quería ir más allá de los scripts locales y montar un entorno lo más parecido posible a un flujo de trabajo real, utilizando:

*   **Extracción:** Python (`requests`) para conectarme a la API pública de la AEMPS y descargar los datos crudos.
*   **Almacenamiento:** **PostgreSQL** como mi base de datos/Data Warehouse, separando los datos en bruto de las tablas analíticas finales.
*   **Transformación:** **dbt (Data Build Tool)** para organizar mis consultas SQL, crear vistas de staging y generar la tabla final de dimensiones.
*   **Orquestación:** **Apache Airflow** para automatizar la ejecución diaria de las tareas y controlar que todo corra en el orden correcto.
*   **Infraestructura:** **Docker y Docker Compose** para meter todo el ecosistema (Postgres, Airflow, pgAdmin) en contenedores y asegurarme de que funcione en cualquier máquina.

![Flujo de Trabajo](https://github.com/user-attachments/assets/7e03d896-b550-4b6e-8da9-6b508c54c3cd)

---

## 🧠 Mi mayor aprendizaje: Resolviendo datos sucios en el JSON

Uno de los mayores desafíos (y aprendizajes) de este proyecto fue descubrir que la realidad de las APIs no siempre coincide con la documentación teórica.

### El problema: El campo "VTM"
Al principio, esperaba encontrar un array clásico con la lista de los principios activos de cada medicamento. Sin embargo, al analizar el JSON real en Postgres, descubrí que la API agrupa los componentes dentro de un objeto llamado `vtm` y en una cadena de texto plana separados por el símbolo de suma (por ejemplo: `"ácido alendrónico + colecalciferol"`). 

Esto rompía mis consultas iniciales y hacía que la tabla final mostrara que los medicamentos no tenían principios activos.

### Cómo lo solucioné con dbt y SQL
En lugar de limpiar esto volviendo a ejecutar scripts en Python, decidí resolverlo directamente en la capa de transformación usando SQL avanzado en Postgres a través de dbt. 

Utilicé la función `regexp_split_to_table` para romper el string dinámicamente por el símbolo `+` y generar una fila limpia por cada componente químico, aplicando además un `trim` para eliminar los espacios en blanco sobrantes. También unifiqué los tipos de datos de las claves primarias (`integer`) para que el `LEFT JOIN` final funcionara a la perfección.

Así quedó mi modelo de Staging corregido:

```sql
with datos_en_bruto as (
    select * from {{ source('aemps_raw_source', 'raw_medicamentos') }}
),

extraer_vtm as (
    select
        id as medicamento_id,
        data->'vtm'->>'nombre' as vtm_nombre
    from datos_en_bruto
    where data->'vtm' is not null
)

select
    medicamento_id,
    md5(medicamento_id::text || trim(principio)) as principio_activo_id,
    trim(principio) as principio_activo_nombre
from extraer_vtm,
regexp_split_to_table(vtm_nombre, '\+') as principio
where vtm_nombre is not null
```

---

## 🧪 Pruebas y Calidad del Dato (Data Quality)

Para asegurarme de que mis transformaciones no duplicaban filas ni rompían la base de datos, implementé el sistema de pruebas nativo de dbt en mi archivo `schema.yml`.

Actualmente, el proyecto pasa con éxito **14 pruebas automáticas**:
*   **`unique` y `not_null`:** Para asegurar que las claves primarias de mis tablas de medicamentos y principios activos no se repitan y nunca vengan vacías.
*   **`relationships`:** Para comprobar la integridad referencial (que todos los principios activos correspondan a un ID de medicamento real existente).

```bash
# Resultado de mis tests en local
Concurrency: 1 threads (target='dev')
1 of 14 START test not_null_dim_medicamentos_medicamento_id .......... [PASS]
...
14 of 14 START test unique_stg_principios_activos_principio_activo_id . [PASS]

Finished running 14 data tests in 0.72s.
Completed successfully | PASS=14 WARN=0 ERROR=0 SKIP=0 TOTAL=14
```

---

## 🚀 Cómo ponerlo en marcha en local

### Requisitos
*   Tener Docker instalado.
*   Python 3.9 o superior.

### Pasos para arrancar
1. Levantar los contenedores de la infraestructura:
   ```bash
   docker compose up -d
   ```
2. Entrar en la carpeta de dbt y ejecutar los modelos y los tests:
   ```bash
   cd aemps_transform
   dbt run --target dev
   dbt test --target dev
   ```

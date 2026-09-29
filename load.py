"""
load.py - Carga el JSONL de detalle de medicamentos en PostgreSQL (capa raw).

Uso:
    python load.py --limite 50 --tabla raw_medicamentos_test   # prueba
    python load.py                                              # carga completa

Es idempotente: se puede ejecutar varias veces sin duplicar filas.
La clave natural es nregistro (texto).
"""
import argparse
import json
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extras import execute_batch

load_dotenv()

ARCHIVO_DEFECTO = Path("detalle_medicamentos.jsonl")
TABLA_DEFECTO = "raw_medicamentos_detalle"
TAMANO_LOTE = 500


def conectar():
    variables = ["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"]
    faltan = [v for v in variables if not os.getenv(v)]
    if faltan:
        raise RuntimeError(f"Faltan variables en el .env: {faltan}")
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def leer_jsonl(ruta, limite=None):
    """Devuelve ({nregistro: línea_json}, líneas_corruptas).

    Se guarda la línea como texto y no el diccionario, para gastar menos
    memoria. Si un nregistro aparece dos veces, se queda la última.
    """
    medicamentos = {}
    corruptas = 0
    with open(ruta, encoding="utf-8") as f:
        for linea in f:
            linea = linea.strip()
            if not linea:
                continue
            try:
                nregistro = str(json.loads(linea)["nregistro"]).strip()
            except (json.JSONDecodeError, KeyError):
                corruptas += 1
                continue
            medicamentos[nregistro] = linea
            if limite and len(medicamentos) >= limite:
                break
    return medicamentos, corruptas


def cargar(conn, tabla, medicamentos):
    """Crea la tabla si no existe y hace upsert por nregistro, en una transacción."""
    with conn.cursor() as cur:
        crear = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {tabla} (
                id          SERIAL PRIMARY KEY,
                nregistro   TEXT NOT NULL UNIQUE,
                data        JSONB NOT NULL,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).format(tabla=sql.Identifier(tabla)).as_string(cur)
        cur.execute(crear)

        insertar = sql.SQL("""
            INSERT INTO {tabla} (nregistro, data)
            VALUES (%s, %s::jsonb)
            ON CONFLICT (nregistro) DO UPDATE
                SET data = EXCLUDED.data,
                    ingested_at = CURRENT_TIMESTAMP
        """).format(tabla=sql.Identifier(tabla)).as_string(cur)
        execute_batch(cur, insertar, list(medicamentos.items()), page_size=TAMANO_LOTE)

        # Verificación antes de confirmar: todo lo que se ha intentado cargar está
        contar = sql.SQL(
            "SELECT count(*) FROM {tabla} WHERE nregistro = ANY(%s)"
        ).format(tabla=sql.Identifier(tabla)).as_string(cur)
        cur.execute(contar, (list(medicamentos),))
        presentes = cur.fetchone()[0]
        if presentes != len(medicamentos):
            raise RuntimeError(
                f"No cuadra: se intentaron cargar {len(medicamentos)} y en la tabla "
                f"hay {presentes} de ellos. Se cancela la carga."
            )

        total = sql.SQL("SELECT count(*) FROM {tabla}").format(tabla=sql.Identifier(tabla)).as_string(cur)
        cur.execute(total)
        filas_tabla = cur.fetchone()[0]

    conn.commit()
    return filas_tabla


def main():
    parser = argparse.ArgumentParser(description="Carga el detalle de CIMA en PostgreSQL")
    parser.add_argument("--archivo", type=Path, default=ARCHIVO_DEFECTO)
    parser.add_argument("--tabla", default=TABLA_DEFECTO)
    parser.add_argument("--limite", type=int, help="cargar solo los N primeros (para pruebas)")
    args = parser.parse_args()

    try:
        medicamentos, corruptas = leer_jsonl(args.archivo, args.limite)
        if not medicamentos:
            raise RuntimeError(f"No se ha leído ningún medicamento de '{args.archivo}'.")
        if corruptas:
            print(f"AVISO: {corruptas} línea(s) incompletas descartadas. "
                  "Si la descarga ha terminado, esto debería ser 0.")
        print(f"Leídos {len(medicamentos)} medicamentos distintos de '{args.archivo}'.")

        conn = conectar()
        try:
            filas_tabla = cargar(conn, args.tabla, medicamentos)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        print(f"Carga completada. La tabla '{args.tabla}' tiene {filas_tabla} filas.")
    except (RuntimeError, psycopg2.Error, OSError) as e:
        print(f"ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
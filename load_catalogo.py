"""
load_catalogo.py - Carga el listado de medicamentos activos (fase 1) en PostgreSQL.

Esta tabla es una "foto" de qué nregistro están vigentes en CIMA ahora
mismo, a diferencia de raw_medicamentos_detalle (que es upsert-only y
nunca pierde filas). Por eso aquí se hace truncate + reload completo en
cada ejecución: un nregistro que ya no está en el catálogo tiene que
desaparecer también de esta tabla. stg_medicamentos usa esta tabla para
filtrar los medicamentos activos, y así el snapshot de dbt puede
detectar las bajas.

Uso:
    python load_catalogo.py
    python load_catalogo.py --archivo catalogo_completo_medicamentos.json --tabla raw_catalogo_activo
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

ARCHIVO_DEFECTO = Path("catalogo_completo_medicamentos.json")
TABLA_DEFECTO = "raw_catalogo_activo"


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


def leer_catalogo(ruta):
    with open(ruta, encoding="utf-8") as f:
        catalogo = json.load(f)
    nregistros = sorted({str(m["nregistro"]).strip() for m in catalogo})
    return nregistros


def cargar(conn, tabla, nregistros):
    """Crea la tabla si no existe y la sustituye entera (truncate + reload),
    todo dentro de la misma transacción: si algo falla a mitad, el rollback
    deja la tabla como estaba, nunca a medio vaciar.
    """
    with conn.cursor() as cur:
        crear = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {tabla} (
                nregistro   TEXT PRIMARY KEY,
                cargado_en  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).format(tabla=sql.Identifier(tabla)).as_string(cur)
        cur.execute(crear)

        cur.execute(sql.SQL("TRUNCATE TABLE {tabla}").format(tabla=sql.Identifier(tabla)))

        insertar = sql.SQL(
            "INSERT INTO {tabla} (nregistro) VALUES (%s)"
        ).format(tabla=sql.Identifier(tabla)).as_string(cur)
        execute_batch(cur, insertar, [(n,) for n in nregistros], page_size=500)

        cur.execute(sql.SQL("SELECT count(*) FROM {tabla}").format(tabla=sql.Identifier(tabla)))
        filas_tabla = cur.fetchone()[0]

    # Verificación ANTES de confirmar: si no cuadra, se aborta con rollback
    # en vez de dejar la tabla a medio cargar.
    if filas_tabla != len(nregistros):
        raise RuntimeError(
            f"No cuadra: se intentaron cargar {len(nregistros)} y la tabla "
            f"tiene {filas_tabla}. Se cancela la carga."
        )

    conn.commit()
    return filas_tabla


def main():
    parser = argparse.ArgumentParser(description="Carga el listado activo de CIMA en PostgreSQL")
    parser.add_argument("--archivo", type=Path, default=ARCHIVO_DEFECTO)
    parser.add_argument("--tabla", default=TABLA_DEFECTO)
    args = parser.parse_args()

    try:
        nregistros = leer_catalogo(args.archivo)
        if not nregistros:
            raise RuntimeError(f"No se ha leído ningún nregistro de '{args.archivo}'.")
        print(f"Leídos {len(nregistros)} nregistro distintos de '{args.archivo}'.")

        conn = conectar()
        try:
            filas_tabla = cargar(conn, args.tabla, nregistros)
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
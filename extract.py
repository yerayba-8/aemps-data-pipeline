"""
extract.py - Extracción del catálogo de medicamentos de CIMA (AEMPS) en dos fases.

Fase 1: listado paginado -> catalogo_completo_medicamentos.json
        (da el nº de registro de cada medicamento)
Fase 2: endpoint de detalle, uno por medicamento -> detalle_medicamentos.jsonl
        (trae el array principiosActivos, que el listado no incluye)

Uso:
    python extract.py 1
    python extract.py 2 --limite 50     # prueba con 50 medicamentos
    python extract.py 2                 # descarga completa (se puede reanudar)
"""
import argparse
import json
import math
import sys
import time
from pathlib import Path

import requests

URL_LISTADO = "https://cima.aemps.es/cima/rest/medicamentos"
URL_DETALLE = "https://cima.aemps.es/cima/rest/medicamento"

ARCHIVO_CATALOGO = Path("catalogo_completo_medicamentos.json")
ARCHIVO_DETALLE = Path("detalle_medicamentos.jsonl")
ARCHIVO_FALLIDOS = Path("fallidos.txt")

RESULTADOS_POR_PAGINA = 200
PAUSA = 0.5      # segundos entre peticiones; medir antes de bajarla
TIMEOUT = 30     # segundos máximos de espera por petición
INTENTOS = 3     # intentos por petición antes de darla por fallida


def pedir_json(sesion, url, params):
    """Hace una petición GET con timeout y reintentos.

    Devuelve el JSON como dict, o None si falla tras INTENTOS.
    Espera 2, 4, 8... segundos entre intentos.
    """
    for intento in range(1, INTENTOS + 1):
        try:
            res = sesion.get(url, params=params, timeout=TIMEOUT)
            if res.status_code == 200:
                return res.json()
            if res.status_code == 404:
                return None  # no existe: reintentar no lo arregla
            print(f"  Código {res.status_code} con {params} (intento {intento}/{INTENTOS})")
        except (requests.RequestException, ValueError) as e:
            print(f"  {type(e).__name__} con {params} (intento {intento}/{INTENTOS})")
        if intento < INTENTOS:
            time.sleep(2 ** intento)
    return None


# ----------------------------------------------------------------------
# FASE 1: listado paginado
# ----------------------------------------------------------------------
def fase_1():
    print("Fase 1: descargando el listado de medicamentos...")
    with requests.Session() as sesion:
        primera = pedir_json(
            sesion, URL_LISTADO, {"pagina": 1, "nresultados": RESULTADOS_POR_PAGINA}
        )
        if primera is None:
            raise RuntimeError("No se pudo descargar la primera página de la API.")

        total_filas = primera.get("totalFilas", 0)
        if total_filas == 0:
            raise RuntimeError("La API no devolvió ningún registro.")

        total_paginas = math.ceil(total_filas / RESULTADOS_POR_PAGINA)
        print(f"Medicamentos detectados: {total_filas} ({total_paginas} páginas)")

        resultados = list(primera.get("resultados", []))

        for pagina in range(2, total_paginas + 1):
            print(f"Descargando página {pagina}/{total_paginas}...")
            time.sleep(PAUSA)
            datos = pedir_json(
                sesion,
                URL_LISTADO,
                {"pagina": pagina, "nresultados": RESULTADOS_POR_PAGINA},
            )
            if datos is None:
                # Antes: break + mensaje de éxito. Ahora: se aborta sin guardar
                # un catálogo cortado.
                raise RuntimeError(
                    f"La página {pagina} falló tras {INTENTOS} intentos. "
                    "No se guarda un catálogo incompleto."
                )
            resultados.extend(datos.get("resultados", []))

    # Validación: deduplicar por nregistro y comparar con lo que anuncia la API
    unicos = {str(r["nregistro"]): r for r in resultados}
    if len(unicos) != total_filas:
        raise RuntimeError(
            f"No cuadra: la API anuncia {total_filas} medicamentos y se han "
            f"descargado {len(unicos)} distintos ({len(resultados)} filas en total)."
        )

    # Escritura atómica: primero a un archivo temporal y luego se renombra
    temporal = ARCHIVO_CATALOGO.with_suffix(".tmp")
    with open(temporal, "w", encoding="utf-8") as f:
        json.dump(list(unicos.values()), f, ensure_ascii=False, indent=2)
    temporal.replace(ARCHIVO_CATALOGO)

    print(f"Fase 1 completada: {len(unicos)} medicamentos en '{ARCHIVO_CATALOGO}'.")


# ----------------------------------------------------------------------
# FASE 2: detalle de cada medicamento (reanudable)
# ----------------------------------------------------------------------
def cargar_ya_descargados():
    """Devuelve el set de nregistro que ya están en el archivo JSONL.

    Si el programa se cortó a mitad de escribir, la última línea puede estar
    incompleta. Esas líneas se descartan y el archivo se reescribe limpio, para
    que ese medicamento se vuelva a pedir.
    """
    hechos = set()
    if not ARCHIVO_DETALLE.exists():
        return hechos

    lineas_validas = []
    corruptas = 0
    with open(ARCHIVO_DETALLE, encoding="utf-8") as f:
        for linea in f:
            linea = linea.strip()
            if not linea:
                continue
            try:
                medicamento = json.loads(linea)
                hechos.add(str(medicamento["nregistro"]))
                lineas_validas.append(linea)
            except (json.JSONDecodeError, KeyError):
                corruptas += 1

    if corruptas:
        print(f"Aviso: {corruptas} línea(s) incompleta(s) descartada(s) y reescritas.")
        with open(ARCHIVO_DETALLE, "w", encoding="utf-8") as f:
            for linea in lineas_validas:
                f.write(linea + "\n")

    return hechos


def fase_2(limite=None):
    if not ARCHIVO_CATALOGO.exists():
        raise RuntimeError(f"Falta '{ARCHIVO_CATALOGO}'. Ejecuta primero: python extract.py 1")

    with open(ARCHIVO_CATALOGO, encoding="utf-8") as f:
        catalogo = json.load(f)

    # str(): el nregistro es texto en toda la API. Si aquí se convirtiera a
    # número, la comparación con 'hechos' no coincidiría nunca.
    nregistros = [str(m["nregistro"]) for m in catalogo]
    if limite:
        nregistros = nregistros[:limite]

    hechos = cargar_ya_descargados()
    pendientes = [n for n in nregistros if n not in hechos]
    print(f"Fase 2: {len(hechos)} ya descargados, {len(pendientes)} pendientes.")

    fallidos = []
    # Modo "a" (append): se añade al final sin borrar lo ya descargado.
    # Con "w" cada ejecución empezaría de cero.
    with requests.Session() as sesion, open(ARCHIVO_DETALLE, "a", encoding="utf-8") as f:
        for i, nregistro in enumerate(pendientes, start=1):
            detalle = pedir_json(sesion, URL_DETALLE, {"nregistro": nregistro})

            if detalle is None or str(detalle.get("nregistro")) != nregistro:
                # No se escribe nada: así el medicamento sigue "pendiente"
                # y se reintentará en la próxima ejecución.
                fallidos.append(nregistro)
            else:
                f.write(json.dumps(detalle, ensure_ascii=False) + "\n")
                f.flush()  # a disco ya: si se corta, esta línea no se pierde

            if i % 100 == 0 or i == len(pendientes):
                print(f"  {i}/{len(pendientes)} procesados ({len(fallidos)} fallidos)")
            time.sleep(PAUSA)

    if fallidos:
        ARCHIVO_FALLIDOS.write_text("\n".join(fallidos) + "\n", encoding="utf-8")
        print(f"{len(fallidos)} fallidos, listados en '{ARCHIVO_FALLIDOS}'. "
              "Vuelve a ejecutar la fase 2 para reintentarlos.")
    elif ARCHIVO_FALLIDOS.exists():
        ARCHIVO_FALLIDOS.unlink()

    resumen()
    return fallidos


def resumen():
    """Cuenta cuántos medicamentos del JSONL traen principiosActivos."""
    total = 0
    sin_principios = 0
    with open(ARCHIVO_DETALLE, encoding="utf-8") as f:
        for linea in f:
            if not linea.strip():
                continue
            total += 1
            if not json.loads(linea).get("principiosActivos"):
                sin_principios += 1
    print(f"\nEn '{ARCHIVO_DETALLE}': {total} medicamentos, "
          f"{sin_principios} sin 'principiosActivos'.")


# ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Extracción del catálogo CIMA/AEMPS")
    parser.add_argument("fase", choices=["1", "2"], help="1 = listado, 2 = detalle")
    parser.add_argument("--limite", type=int, help="solo para la fase 2: procesar N medicamentos")
    args = parser.parse_args()

    try:
        if args.fase == "1":
            fase_1()
        else:
            fallidos = fase_2(args.limite)
            if fallidos:
                sys.exit(1)
    except RuntimeError as e:
        print(f"ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
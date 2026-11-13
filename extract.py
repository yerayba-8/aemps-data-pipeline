"""
extract.py - Extracción del catálogo de medicamentos de CIMA (AEMPS) en dos fases.

Fase 1: listado paginado -> catalogo_completo_medicamentos.json
        (da el nº de registro de cada medicamento)
        Antes de sobrescribir el catálogo compara su tamaño con el anterior:
        una caída de más del 5 % se bloquea (salvo --forzar) y un cambio de
        más del 1 % (subida o bajada) se marca como aviso.
Fase 2: endpoint de detalle, uno por medicamento -> detalle_medicamentos.jsonl
        (trae el array principiosActivos, que el listado no incluye)
        Descarga concurrente con un tope de ritmo global.

Uso:
    python extract.py 1
    python extract.py 1 --forzar          # acepta una caída del catálogo de más del 5 % (uso manual)
    python extract.py 2 --limite 50       # prueba con 50 medicamentos
    python extract.py 2                   # incremental: solo descarga los nregistro nuevos
    python extract.py 2 --completo        # redescarga TODOS, para detectar cambios (carga semanal)
    python extract.py 2 --completo --trabajadores 8 --ritmo 10
"""
import argparse
import json
import math
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests

URL_LISTADO = "https://cima.aemps.es/cima/rest/medicamentos"
URL_DETALLE = "https://cima.aemps.es/cima/rest/medicamento"

ARCHIVO_CATALOGO = Path("catalogo_completo_medicamentos.json")
ARCHIVO_DETALLE = Path("detalle_medicamentos.jsonl")
# Archivo de trabajo de una redescarga --completo en curso. Se usa un nombre
# distinto al definitivo para que ARCHIVO_DETALLE (el que lee load.py) nunca
# quede en un estado a medias: solo se sustituye cuando la redescarga termina
# sin fallidos.
ARCHIVO_DETALLE_COMPLETO_TMP = Path("detalle_medicamentos.completo.tmp.jsonl")
ARCHIVO_FALLIDOS = Path("fallidos.txt")
ARCHIVO_ESTADO = Path("estado_catalogo.json")   # lo lee alertas.py para el aviso

UMBRAL_AVISO = 0.01       # ±1 %: por encima, aviso de revisión
UMBRAL_BLOQUEO = 0.05     # caída de más del 5 %: se bloquea, no se sobrescribe nada

RESULTADOS_POR_PAGINA = 200
PAUSA = 0.5          # fase 1: segundos entre páginas del listado
TIMEOUT = 30         # segundos máximos de espera por petición
INTENTOS = 5         # intentos por petición antes de darla por fallida
TRABAJADORES = 8     # fase 2: hilos de descarga simultáneos
RITMO = 10.0         # fase 2: máximo de peticiones/segundo entre TODOS los hilos


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
# Control de cordura del catálogo (antes de sobrescribirlo)
# ----------------------------------------------------------------------
def _contar_catalogo_anterior():
    """Nº de medicamentos del catálogo que hay ahora en disco, o None si no hay uno válido."""
    if not ARCHIVO_CATALOGO.exists():
        return None
    try:
        with open(ARCHIVO_CATALOGO, encoding="utf-8") as f:
            return len(json.load(f))
    except (json.JSONDecodeError, OSError):
        return None


def _guardar_estado(estado, anterior, nuevo, mensaje):
    datos = {
        "fecha": datetime.now().isoformat(timespec="seconds"),
        "estado": estado,
        "anterior": anterior,
        "nuevo": nuevo,
        "mensaje": mensaje,
    }
    temporal = ARCHIVO_ESTADO.with_suffix(".tmp")
    temporal.write_text(json.dumps(datos, ensure_ascii=False, indent=2), encoding="utf-8")
    temporal.replace(ARCHIVO_ESTADO)


def evaluar_cambio(anterior, nuevo, forzar=False):
    """Compara el catálogo nuevo con el anterior. Lanza RuntimeError si la caída es sospechosa."""
    if not anterior:
        estado, mensaje = "primera_vez", f"Primer catálogo: {nuevo} medicamentos."
    else:
        var = (nuevo - anterior) / anterior
        base = f"{anterior} → {nuevo} medicamentos ({nuevo - anterior:+d}, {var:+.2%})"
        if var < -UMBRAL_BLOQUEO:
            if forzar:
                estado, mensaje = "forzado", f"⚠️ Caída fuerte aceptada con --forzar: {base}"
            else:
                estado, mensaje = "bloqueado", f"🛑 Caída sospechosa, catálogo NO actualizado: {base}"
        elif abs(var) > UMBRAL_AVISO:
            direccion = "Caída" if var < 0 else "Subida"
            estado, mensaje = "aviso", f"⚠️ {direccion} mayor de lo habitual, revisa: {base}"
        else:
            estado, mensaje = "ok", f"Catálogo: {base}"

    _guardar_estado(estado, anterior, nuevo, mensaje)
    print(mensaje)
    if estado == "bloqueado":
        raise RuntimeError(mensaje + ". Si es legítimo, ejecuta: python extract.py 1 --forzar")


# ----------------------------------------------------------------------
# FASE 1: listado paginado
# ----------------------------------------------------------------------
def fase_1(forzar=False):
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
                raise RuntimeError(
                    f"La página {pagina} falló tras {INTENTOS} intentos. "
                    "No se guarda un catálogo incompleto."
                )
            resultados.extend(datos.get("resultados", []))

    unicos = {str(r["nregistro"]): r for r in resultados}
    if len(unicos) != total_filas:
        raise RuntimeError(
            f"No cuadra: la API anuncia {total_filas} medicamentos y se han "
            f"descargado {len(unicos)} distintos ({len(resultados)} filas en total)."
        )

    # Control de cordura: si la caída es sospechosa lanza RuntimeError y el
    # catálogo bueno que hay en disco NO se toca.
    evaluar_cambio(_contar_catalogo_anterior(), len(unicos), forzar)

    temporal = ARCHIVO_CATALOGO.with_suffix(".tmp")
    with open(temporal, "w", encoding="utf-8") as f:
        json.dump(list(unicos.values()), f, ensure_ascii=False, indent=2)
    temporal.replace(ARCHIVO_CATALOGO)

    print(f"Fase 1 completada: {len(unicos)} medicamentos en '{ARCHIVO_CATALOGO}'.")


# ----------------------------------------------------------------------
# FASE 2: detalle de cada medicamento (reanudable y concurrente)
# ----------------------------------------------------------------------
class LimitadorRitmo:
    """Reparte 'huecos' de tiempo para que, entre todos los hilos, no se
    superen `por_segundo` peticiones por segundo."""

    def __init__(self, por_segundo):
        self.intervalo = 1.0 / por_segundo
        self.cerrojo = threading.Lock()
        self.siguiente = time.monotonic()

    def esperar(self):
        with self.cerrojo:
            ahora = time.monotonic()
            mi_turno = max(self.siguiente, ahora)
            self.siguiente = mi_turno + self.intervalo
        espera = mi_turno - ahora
        if espera > 0:
            time.sleep(espera)


_hilo = threading.local()


def _sesion_del_hilo():
    """Cada hilo usa su propia Session (las Session no están pensadas para compartirse)."""
    if not hasattr(_hilo, "sesion"):
        _hilo.sesion = requests.Session()
    return _hilo.sesion


def _descargar_detalle(nregistro, limitador):
    limitador.esperar()
    detalle = pedir_json(_sesion_del_hilo(), URL_DETALLE, {"nregistro": nregistro})
    return nregistro, detalle


def cargar_ya_descargados(ruta):
    """Devuelve el set de nregistro que ya están en el archivo JSONL indicado."""
    hechos = set()
    if not ruta.exists():
        return hechos

    lineas_validas = []
    corruptas = 0
    with open(ruta, encoding="utf-8") as f:
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
        with open(ruta, "w", encoding="utf-8") as f:
            for linea in lineas_validas:
                f.write(linea + "\n")

    return hechos


def fase_2(limite=None, completo=False, trabajadores=TRABAJADORES, ritmo=RITMO):
    if not ARCHIVO_CATALOGO.exists():
        raise RuntimeError(f"Falta '{ARCHIVO_CATALOGO}'. Ejecuta primero: python extract.py 1")

    with open(ARCHIVO_CATALOGO, encoding="utf-8") as f:
        catalogo = json.load(f)

    nregistros = [str(m["nregistro"]) for m in catalogo]
    if limite:
        nregistros = nregistros[:limite]

    if completo:
        archivo_trabajo = ARCHIVO_DETALLE_COMPLETO_TMP
        print("Fase 2 (--completo): redescargando TODOS los medicamentos del catálogo actual.")
    else:
        archivo_trabajo = ARCHIVO_DETALLE

    hechos = cargar_ya_descargados(archivo_trabajo)
    pendientes = [n for n in nregistros if n not in hechos]
    print(f"Fase 2: {len(hechos)} ya descargados, {len(pendientes)} pendientes "
          f"({trabajadores} hilos, máx. {ritmo:g} peticiones/s).")

    limitador = LimitadorRitmo(ritmo)
    fallidos = []
    procesados = 0
    inicio = time.monotonic()

    # Solo este hilo escribe en el archivo: no se mezclan líneas.
    with open(archivo_trabajo, "a", encoding="utf-8") as f, \
            ThreadPoolExecutor(max_workers=trabajadores) as pool:
        futuros = [pool.submit(_descargar_detalle, n, limitador) for n in pendientes]
        for futuro in as_completed(futuros):
            nregistro, detalle = futuro.result()
            procesados += 1

            if detalle is None or str(detalle.get("nregistro")) != nregistro:
                fallidos.append(nregistro)
            else:
                f.write(json.dumps(detalle, ensure_ascii=False) + "\n")
                f.flush()

            if procesados % 500 == 0 or procesados == len(pendientes):
                seg = time.monotonic() - inicio
                print(f"  {procesados}/{len(pendientes)} procesados "
                      f"({len(fallidos)} fallidos) - {seg:.0f} s, "
                      f"{procesados / seg:.1f} peticiones/s")

    if fallidos:
        ARCHIVO_FALLIDOS.write_text("\n".join(fallidos) + "\n", encoding="utf-8")
        print(f"{len(fallidos)} fallidos, listados en '{ARCHIVO_FALLIDOS}'. "
              "Vuelve a ejecutar la fase 2 (con el mismo modo) para reintentarlos.")
    elif ARCHIVO_FALLIDOS.exists():
        ARCHIVO_FALLIDOS.unlink()

    if completo:
        if fallidos:
            print(
                f"Redescarga completa incompleta ({len(fallidos)} fallidos). "
                f"'{ARCHIVO_DETALLE}' NO se ha modificado todavía."
            )
        else:
            archivo_trabajo.replace(ARCHIVO_DETALLE)
            print(f"Redescarga completa terminada. '{ARCHIVO_DETALLE}' actualizado con {len(nregistros)} medicamentos.")

    resumen(ARCHIVO_DETALLE if (not completo or not fallidos) else archivo_trabajo)
    return fallidos


def resumen(ruta):
    """Cuenta cuántos medicamentos del JSONL traen principiosActivos."""
    if not ruta.exists():
        return
    total = 0
    sin_principios = 0
    with open(ruta, encoding="utf-8") as f:
        for linea in f:
            if not linea.strip():
                continue
            total += 1
            if not json.loads(linea).get("principiosActivos"):
                sin_principios += 1
    print(f"\nEn '{ruta}': {total} medicamentos, "
          f"{sin_principios} sin 'principiosActivos'.")


# ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Extracción del catálogo CIMA/AEMPS")
    parser.add_argument("fase", choices=["1", "2"], help="1 = listado, 2 = detalle")
    parser.add_argument("--limite", type=int, help="solo para la fase 2: procesar N medicamentos")
    parser.add_argument(
        "--completo",
        action="store_true",
        help="fase 2: redescarga el detalle de TODOS los medicamentos del catálogo, "
             "no solo los nuevos (para detectar cambios en carga semanal)",
    )
    parser.add_argument("--trabajadores", type=int, default=TRABAJADORES,
                        help=f"fase 2: hilos simultáneos (por defecto {TRABAJADORES})")
    parser.add_argument("--ritmo", type=float, default=RITMO,
                        help=f"fase 2: máximo de peticiones por segundo (por defecto {RITMO:g})")
    parser.add_argument("--forzar", action="store_true",
                        help="fase 1: acepta una caída del catálogo de más del "
                             f"{UMBRAL_BLOQUEO * 100:.0f} %% (uso manual, tras comprobar que es legítima)")
    args = parser.parse_args()

    try:
        if args.fase == "1":
            fase_1(forzar=args.forzar)
        else:
            fallidos = fase_2(args.limite, completo=args.completo,
                              trabajadores=args.trabajadores, ritmo=args.ritmo)
            if fallidos:
                sys.exit(1)
    except RuntimeError as e:
        print(f"ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
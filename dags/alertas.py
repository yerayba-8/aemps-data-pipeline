import json
import os
from datetime import datetime, timezone

import requests

ARCHIVO_ESTADO = "/opt/airflow/data/estado_catalogo.json"


def _enviar(texto):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("Telegram no configurado: faltan TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID")
        return
    try:
        res = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": texto[:4000],
                  "disable_web_page_preview": True},
            timeout=10,
        )
        res.raise_for_status()
    except requests.RequestException as e:
        # Solo el tipo de error: el mensaje de requests incluye la URL, y la URL lleva el token.
        print(f"No se pudo enviar el aviso a Telegram: {type(e).__name__}")


def _estado_catalogo():
    """Frase de resumen que dejó extract.py (cambio de tamaño del catálogo)."""
    try:
        with open(ARCHIVO_ESTADO, encoding="utf-8") as f:
            return json.load(f).get("mensaje", "")
    except (OSError, ValueError):
        return ""


def avisar_fallo(context):
    """Se ejecuta cuando una tarea falla definitivamente (agotados los reintentos)."""
    ti = context["task_instance"]
    error = str(context.get("exception", ""))[:500]
    detalle = _estado_catalogo() if ti.task_id == "extraccion_listado" else ""
    _enviar(
        f"❌ Fallo en {ti.dag_id}\n"
        f"Tarea: {ti.task_id}\n"
        f"Run: {context['run_id']}\n"
        f"{detalle}\n"
        f"Error: {error}\n"
        f"Logs: {ti.log_url}"
    )


def avisar_exito(context):
    """Se ejecuta cuando el DAG completo termina bien."""
    dag_run = context["dag_run"]
    minutos = (datetime.now(timezone.utc) - dag_run.start_date).total_seconds() / 60
    _enviar(
        f"✅ {dag_run.dag_id} completado en {minutos:.0f} min\n"
        f"{_estado_catalogo()}\n"
        f"Run: {dag_run.run_id}"
    )
import requests
import json
import math
import time

def extraer_catalogo_completo():
    url = "https://cima.aemps.es/cima/rest/medicamentos"
    resultados_totales = []
    
    # Parámetros iniciales para la primera llamada de reconocimiento
    resultados_por_pagina = 200
    params = {
        "pagina": 1,
        "nresultados": resultados_por_pagina
    }
    
    print("Iniciando fase de reconocimiento de la API...")
    try:
        # 1. Hacemos la primera petición para calcular el volumen total
        res = requests.get(url, params=params)
        if res.status_code != 200:
            print(f"Error al conectar con la API en el inicio: {res.status_code}")
            return
            
        data_inicial = res.json()
        total_filas = data_inicial.get("totalFilas", 0)
        
        if total_filas == 0:
            print(" No se encontraron registros en la API.")
            return
            
        # 2. Calculamos el total de páginas necesarias de forma dinámica
        total_paginas = math.ceil(total_filas / resultados_por_pagina)
        print(f"Total de medicamentos detectados: {total_filas}")
        print(f"Se procesarán {total_paginas} páginas (de {resultados_por_pagina} registros cada una).\n")
        
        # Guardamos ya los primeros 200 resultados que nos han venido en esta llamada
        resultados_totales.extend(data_inicial.get("resultados", []))
        
        # 3. Arrancamos el bucle desde la página 2 hasta la última
        for num_pagina in range(2, total_paginas + 1):
            params["pagina"] = num_pagina
            print(f"Descargando página {num_pagina}/{total_paginas}...")
            
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                resultados_totales.extend(data.get("resultados", []))
            else:
                print(f"Error en página {num_pagina}: Código {response.status_code}. Deteniendo extracción.")
                break
            
            # Política de cortesía: pausamos 0.5 segundos para no saturar el servidor
            time.sleep(0.5)
            
    except Exception as e:
        print(f"Error crítico durante el pipeline de extracción: {e}")
        return

    # 4. Guardamos todo el catálogo en un archivo JSON local
    archivo_final = "catalogo_completo_medicamentos.json"
    print(f"\nGuardando {len(resultados_totales)} registros en el archivo local...")
    
    with open(archivo_final, "w", encoding="utf-8") as f:
        json.dump(resultados_totales, f, ensure_ascii=False, indent=4)
        
    print(f"¡Fase 1 completada con éxito! Archivo '{archivo_final}' generado.")

if __name__ == "__main__":
    extraer_catalogo_completo()
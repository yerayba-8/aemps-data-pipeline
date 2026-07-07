import requests
import json

def extraer_tres_paginas():
    url = "https://cima.aemps.es/cima/rest/medicamentos"
    
    # Lista vacía donde acumularemos TODOS los medicamentos de todas las páginas
    todos_los_medicamentos = []
    
    # Parámetros base
    params = {
        "pagina": 1,
        "nresultados": 200  # Aprovechamos el máximo por página que descubrimos
    }
    
    # Bucle controlado para las 3 primeras páginas
    for num_pagina in range(1, 4):
        params["pagina"] = num_pagina
        print(f"Descargando página {num_pagina}...")
        
        try:
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                _resultados_pagina = data.get("resultados", [])
                
                # Usamos EXTEND para fusionar los nuevos medicamentos de forma plana
                todos_los_medicamentos.extend(_resultados_pagina)
                print(f" Página {num_pagina} descargada. Acumulados: {len(todos_los_medicamentos)} medicamentos.")
                
            else:
                print(f"❌ Error en página {num_pagina}: Código {response.status_code}")
                break # Si una página falla, paramos por seguridad
                
        except Exception as e:
            print(f"Error crítico en la petición de la página {num_pagina}: {e}")
            break

    # Guardamos el resultado total del bloque
    archivo_final = "tres_paginas_medicamentos.json"
    with open(archivo_final, "w", encoding="utf-8") as f:
        json.dump(todos_los_medicamentos, f, ensure_ascii=False, indent=4)
        
    print(f"\n¡Proceso terminado! Archivo '{archivo_final}' guardado con un total de {len(todos_los_medicamentos)} registros.")

if __name__ == "__main__":
    extraer_tres_paginas()
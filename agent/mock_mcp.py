# backend/agent/mock_mcp.py
import re

class MockMCPAgent:
    def __init__(self):
        pass

    def process_query(self, query: str):
        """
        Simula un LLM procesando una intención.
        Entrada: "Muéstrame las sedes rurales de Antioquia en 3D"
        Salida: JSON con filtros y configuración de capa.
        """
        query = query.lower()
        
        # 1. Configuración por defecto
        action = {
            "dataset": "sedes_mock",
            "filters": {},
            "layer_type": "scatterplot", # Default
            "visualization_params": {}
        }

        reply_parts = []

        # 2. Detectar Filtros: ZONA
        if "rural" in query:
            action["filters"]["zona"] = "RURAL"
            reply_parts.append("filtrando zona Rural")
        elif "urbana" in query:
            action["filters"]["zona"] = "URBANA"
            reply_parts.append("filtrando zona Urbana")

        # 3. Detectar Filtros: AÑO
        years = re.findall(r"202[2-5]", query) # Busca 2022-2025
        if years:
            action["filters"]["year_reporte"] = years[0]
            reply_parts.append(f"del año {years[0]}")

        # 4. Detectar Filtros: DEPARTAMENTO (Lista básica para el mock)
        deptos = ["antioquia", "cundinamarca", "valle", "atlantico", "boyaca", "narino"]
        for d in deptos:
            if d in query:
                action["filters"]["DPTO_CNMBR"] = d.upper() # Asumimos que en CSV está en mayúsculas
                reply_parts.append(f"en {d.capitalize()}")
                break

        # 5. Detectar Tipo de Visualización
        if any(x in query for x in ["3d", "barras", "altura", "extruye", "volumen"]):
            action["layer_type"] = "column"
            reply_parts.append("vista en 3D")
        elif any(x in query for x in ["calor", "densidad", "heatmap", "mapa de calor"]):
            action["layer_type"] = "heatmap"
            reply_parts.append("mapa de calor")
        
        # 6. Construir respuesta verbal
        if not reply_parts:
            reply_text = "Entendido, mostrando datos generales."
        else:
            reply_text = "Claro, " + ", ".join(reply_parts) + "."

        return {
            "reply": reply_text,
            "action": action
        }

agent = MockMCPAgent()
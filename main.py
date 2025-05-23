# main.py

import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
from google import genai
from sqlalchemy import MetaData
from dotenv import load_dotenv
load_dotenv()

# ————— Configuración de entorno —————
DATABASE_URL = "mysql+pymysql://root:ZMBbrwQJBLBiPbQXhFriWPqgPmCGYzYp@yamabiko.proxy.rlwy.net:11355/railway"
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not DATABASE_URL or not GOOGLE_API_KEY:
    raise RuntimeError("Define las variables de entorno DATABASE_URL y GOOGLE_API_KEY")

# ————— Configuración de SQLAlchemy —————
engine = create_engine(DATABASE_URL)
inspector = inspect(engine)
SessionLocal = sessionmaker(bind=engine)

# ————— Configuración de Google GenAI (Gemini) —————
client = genai.Client(api_key=GOOGLE_API_KEY)
MODEL = os.getenv("LLM_MODEL")

# ————— Funciones auxiliares —————


# Creamos un MetaData y reflejamos todo el catálogo de la BD
metadata = MetaData()
metadata.reflect(bind=engine)

def fetch_schema_text_v2() -> str:
    schema_txt = ""
    # metadata.tables es un OrderedDict de Table objects
    for table in metadata.sorted_tables:
        schema_txt += f"TABLE {table.name} (\n"
        for col in table.columns:
            # col.type es el tipo de columna
            schema_txt += f"  - {col.name} {col.type}\n"
        schema_txt += ")\n\n"
    return schema_txt


def generate_sql(natural_query: str, schema: str) -> str:
    """Llama a Gemini para generar la consulta SQL correspondiente."""
    prompt = (
        "Eres un asistente que traduce peticiones en SQL. "
        "Este es el esquema de la base de datos:\n\n"
        f"{schema}\n"
        f"Solo se permiten consultas SELECT. Evita anotaciones extra como '''sql ''' o similar.\n"
        f"Por ejemplo: Pregunta: ¿Cuantos colaboradores existen? respuesta: SELECT COUNT(*) FROM colaboradores\n"
        "Genera SOLO la consulta SQL válida que responda a:\n"
        f"\"{natural_query}\""
    )
    response = client.models.generate_content(
        model=MODEL,
        contents=[prompt]
    )
    return response.text.strip()

# ————— Definición de FastAPI —————
app = FastAPI()

class QueryRequest(BaseModel):
    natural_query: str

@app.post("/sql_from_nl")
def sql_from_nl(req: QueryRequest):
    # 1. Extraer esquema
    schema = fetch_schema_text_v2()
    # 2. Generar SQL vía Gemini
    sql = generate_sql(req.natural_query, schema)
    # 3. Validación básica (solo SELECT)

    # 4. Ejecutar la consulta de forma segura
    session = SessionLocal()
    try:
        result = session.execute(text(sql))
        rows: list[dict] = result.mappings().all()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error ejecutando SQL: {e}")
    finally:
        session.close()
    # 5. Devolver SQL y resultados
    return {"sql": sql, "results": rows}

# ————— Arranque con Uvicorn —————
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=True
    )

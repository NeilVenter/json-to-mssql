from fastapi import FastAPI, UploadFile, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, List
import json
from schema_engine import analyze_json_structure, SchemaMap
from db_engine import flatten_data, sync_to_db
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allow all for local tool
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class SyncRequest(BaseModel):
    connection_string: str
    schema_map: SchemaMap
    json_data: Any

    json_data: Any

@app.get("/config")
def get_config():
    return {"connection_string": os.getenv("CONNECTION_STRING", "")}

@app.post("/process-json")
async def process_json(file: UploadFile):
    try:
        content = await file.read()
        data = json.loads(content)
        schema_map = analyze_json_structure(data)
        return {"schema_map": schema_map, "json_data": data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/sync")
async def sync(request: SyncRequest):
    try:
        # 1. Flatten
        flat_data = flatten_data(request.schema_map, request.json_data)
        
        # 2. Sync
        result = sync_to_db(request.connection_string, request.schema_map, flat_data)
        
        return result
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

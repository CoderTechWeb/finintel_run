from fastapi import FastAPI, UploadFile, File
from typing import List
from ingestion.ingest import ingest_file
import tempfile
import os

app = FastAPI()

@app.post("/analyze")
async def analyze(files: List[UploadFile] = File(...)):

    results = []

    for file in files:
        # get extension
        ext = os.path.splitext(file.filename)[1]
        if not ext:
            ext = ".csv"

        # save temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name

        # process file
        result = ingest_file(tmp_path)

        # attach filename for UI
        results.append({
            "filename": file.filename,
            "result": result
        })

    return {
        "status": "success",
        "file_count": len(results),
        "results": results
    }
from fastapi import FastAPI

app = FastAPI(title="Search Intelligence API")

@app.get("/health")
def health():
    return {"status": "ok"}

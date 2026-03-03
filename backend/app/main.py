from fastapi import FastAPI

app = FastAPI(title="QUINGAPP API")

@app.get("/")
def root():
    return {"message": "QUINGAPP backend is running"}
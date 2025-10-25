from fastapi import FastAPI
from .routers.health import router as health_router
from .routers.predict import router as predict_router

app = FastAPI(title="SportIQ API", version="0.1.0")

app.include_router(health_router, prefix="")
app.include_router(predict_router, prefix="/predict")

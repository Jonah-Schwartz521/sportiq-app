# apps/api/app/main.py
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

# Routers (each router file already sets its own prefix/tags)
from .routers.health import router as health_router
from .routers.predict import router as predict_router
from .routers.explain import router as explain_router
from .routers.insights import router as insights_router
from .routers.predictions import router as predictions_router


app = FastAPI(
    title="SportIQ API",
    version="0.1.0",
    description=(
        "Cross-sport predictive analytics API for SportIQ — "
        "powering win probabilities, explanations, and insights."
    ),
)

# CORS (open for dev; tighten allow_origins in prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # e.g., ["http://localhost:3000"] in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers WITHOUT extra prefixes (they already have them)
app.include_router(health_router)
app.include_router(predict_router)
app.include_router(explain_router)
app.include_router(insights_router)
app.include_router(predictions_router)

# Redirect root to Swagger UI
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")
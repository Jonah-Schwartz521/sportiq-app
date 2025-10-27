from fastapi import FastAPI
from fastapi.responses import RedirectResponse

# Routers
from .routers.health import router as health_router
from .routers.predict import router as predict_router
from .routers.explain import router as explain_router
from .routers.insights import router as insights_router

# Initialize FastAPI app
app = FastAPI(
    title="SportIQ API",
    version="0.1.0",
    description="Cross-sport predictive analytics API for SportIQ — powering win probabilities, explanations, and insights."
)

# Include routers
app.include_router(health_router, prefix="")
app.include_router(predict_router, prefix="/predict")
app.include_router(explain_router, prefix="")
app.include_router(insights_router, prefix="")

# Redirect root ("/") to API docs
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")
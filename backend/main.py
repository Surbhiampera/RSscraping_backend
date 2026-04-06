from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.core.config import settings
from backend.routes import upload
from backend.db.session import get_db
from backend.db.models import Base
from backend.routes import auth, upload, scrape, results, dashboard, admin, preview
from backend.routes import history, quotes, activity, usage, profile
from backend.db.session import Base, engine
from sqlalchemy import text

# ✅ Create DB tables (only for development)
Base.metadata.create_all(bind=engine)



app = FastAPI(
    title="InsureScrape API",
    description="Car Insurance Scraping Platform API",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)


# ✅ CORS Configuration (Safe & Frontend Friendly)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",   # Vite
        "http://localhost:3000",   # React / Next
        "http://127.0.0.1:5173",
        "http://127.0.0.1:3000",
        *settings.cors_origins_list,  # from env if defined
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ✅ API Routers
app.include_router(auth.router, prefix="/api/auth", tags=["Auth"])
app.include_router(upload.router, prefix="/api/upload", tags=["Upload"])
app.include_router(preview.router, prefix="/api/preview", tags=["Preview"])
app.include_router(scrape.router, prefix="/api/scrape", tags=["Scrape"])
app.include_router(results.router, prefix="/api/results", tags=["Results"])
app.include_router(dashboard.router, prefix="/api/dashboard", tags=["Dashboard"])
app.include_router(admin.router, prefix="/api/admin", tags=["Admin"])
app.include_router(history.router, prefix="/api/history", tags=["History"])
app.include_router(quotes.router, prefix="/api/quotes", tags=["Quotes"])
app.include_router(activity.router, prefix="/api/activity", tags=["Activity"])
app.include_router(usage.router, prefix="/api/usage", tags=["Usage"])
app.include_router(profile.router, prefix="/api/profile", tags=["Profile"])


# ✅ Health Check Endpoint
@app.get("/api/health", tags=["Health"])
def health_check():
    return {
        "status": "ok",
        "environment": settings.APP_ENV,
    }


# ✅ Root Endpoint (Useful for testing connection)
@app.get("/")
def root():
    return {"message": "InsureScrape API is running 🚀"}
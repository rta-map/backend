from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routes.map import router as map_router

app = FastAPI(title="RTA Map API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(map_router, prefix="/api")

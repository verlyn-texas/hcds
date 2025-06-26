from fastapi import FastAPI
from app.api import templates, datasets

app = FastAPI(
    title="Hierarchical and Customizable Datasets (HCD)",
    description="This project aims to create a service for designing hierarchical data structures that includes computational design and managing data that conforms to those designs.  It is termed, “Hierarchical and Customizable Datasets” (HCD). It is intended to be a foundation on which complex software applications can be built, either by software developers or LLM-based agents.",
    version="0.1.0"
)

# Include routers
app.include_router(templates.router, prefix="/api/v1")
app.include_router(datasets.router, prefix="/api/v1")
# app.include_router(queries.router, prefix="/api/v1")
# app.include_router(auth.router, prefix="/api/v1")

@app.get("/")
def read_root():
    return {"message": "Welcome to the HCD Service API!"} 
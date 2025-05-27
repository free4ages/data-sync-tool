"""
Sample FastAPI-based configuration management service
with PostgreSQL persistence (using SQLAlchemy and JSONB configs).
"""
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Dict, Any, List
from sqlalchemy import create_engine, Column, Integer, String, JSON, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import datetime

DATABASE_URL = "postgresql://config_user:password@localhost:5432/configdb"

# SQLAlchemy setup
engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Database models
class DataStoreModel(Base):
    __tablename__ = "datastores"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False)
    type = Column(String, nullable=False)
    config = Column(JSON, nullable=False)  # stores server/token/subject/etc

class PipelineModel(Base):
    __tablename__ = "pipelines"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False)
    config = Column(JSON, nullable=False)  # full pipeline_conf as JSON
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(
        DateTime,
        default=datetime.datetime.utcnow,
        onupdate=datetime.datetime.utcnow
    )

# Pydantic schemas
class DataStore(BaseModel):
    name: str = Field(..., example="nats_source")
    type: str = Field(..., example="nats")
    config: Dict[str, Any] = Field(
        ...,
        example={"servers":["nats://localhost:4222"], "subject":"user.events"}
    )

class Pipeline(BaseModel):
    name: str = Field(..., example="ingest_user_events")
    config: Dict[str, Any]  # mirrors YAML structure under each pipeline

# Dependency for DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# FastAPI app
app = FastAPI(
    title="Pipeline Config Service",
    description="CRUD API for managing pipeline and datastore configurations",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Create tables (run once or via Alembic migrations)
Base.metadata.create_all(bind=engine)

# DataStore endpoints
@app.post("/datastores/", response_model=DataStore)
def create_datastore(datastore: DataStore, db: Session = Depends(get_db)):
    existing = db.query(DataStoreModel).filter_by(name=datastore.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="DataStore name already exists")
    model = DataStoreModel(name=datastore.name, type=datastore.type, config=datastore.config)
    db.add(model)
    db.commit()
    db.refresh(model)
    return model

@app.get("/datastores/", response_model=List[DataStore])
def list_datastores(db: Session = Depends(get_db)):
    return db.query(DataStoreModel).all()

@app.put("/datastores/{name}", response_model=DataStore)
def update_datastore(name: str, datastore: DataStore, db: Session = Depends(get_db)):
    model = db.query(DataStoreModel).filter_by(name=name).first()
    if not model:
        raise HTTPException(status_code=404, detail="DataStore not found")
    model.type = datastore.type
    model.config = datastore.config
    db.commit()
    db.refresh(model)
    return model

@app.delete("/datastores/{name}")
def delete_datastore(name: str, db: Session = Depends(get_db)):
    model = db.query(DataStoreModel).filter_by(name=name).first()
    if not model:
        raise HTTPException(status_code=404, detail="DataStore not found")
    db.delete(model)
    db.commit()
    return {"detail": "deleted"}

# Pipeline endpoints
@app.post("/pipelines/", response_model=Pipeline)
def create_pipeline(pipeline: Pipeline, db: Session = Depends(get_db)):
    existing = db.query(PipelineModel).filter_by(name=pipeline.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Pipeline name already exists")
    model = PipelineModel(name=pipeline.name, config=pipeline.config)
    db.add(model)
    db.commit()
    db.refresh(model)
    return model

@app.get("/pipelines/", response_model=List[Pipeline])
def list_pipelines(db: Session = Depends(get_db)):
    return db.query(PipelineModel).all()

@app.get("/pipelines/{name}", response_model=Pipeline)
def get_pipeline(name: str, db: Session = Depends(get_db)):
    model = db.query(PipelineModel).filter_by(name=name).first()
    if not model:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return model

@app.put("/pipelines/{name}", response_model=Pipeline)
def update_pipeline(name: str, pipeline: Pipeline, db: Session = Depends(get_db)):
    model = db.query(PipelineModel).filter_by(name=name).first()
    if not model:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    model.config = pipeline.config
    db.commit()
    db.refresh(model)
    return model

@app.delete("/pipelines/{name}")
def delete_pipeline(name: str, db: Session = Depends(get_db)):
    model = db.query(PipelineModel).filter_by(name=name).first()
    if not model:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    db.delete(model)
    db.commit()
    return {"detail": "deleted"}

# Example: runner script fetches latest configs via API
# and executes run_pipeline for each pipeline.
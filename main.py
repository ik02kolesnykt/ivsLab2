from typing import List, Set
from sqlalchemy import Table, String, Float, DateTime, create_engine, MetaData, select, Column, Integer

from sqlalchemy.orm import sessionmaker, Session
from fastapi import  Depends, FastAPI, HTTPException
import json

from starlette.websockets import WebSocket, WebSocketDisconnect
from datetime import datetime
from pydantic import BaseModel, field_validator

from config import *


DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()

processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("user_id", Integer),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)


class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    user_id: int
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator('timestamp', mode='before')
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ).")


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData


class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    user_id: int
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime


app = FastAPI()

subscriptions: Set[WebSocket] = set()


@app.websocket("/ws/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    subscriptions.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions.remove(websocket)


async def send_data_to_subscribers(data):
    for websocket in subscriptions:
        await websocket.send_json(json.dumps(data))

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def connect_to_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# FastAPI CRUD endpoints
@app.get("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def read_processed_agent_data(record_id: int, db: Session = Depends(connect_to_db)):
    try:
        record = db.execute(select(processed_agent_data).where(processed_agent_data.c.id == record_id)).fetchone()
        if record:
            column_names = ["id", "road_state", "user_id", "x", "y", "z", "latitude", "longitude", "timestamp"]
            return ProcessedAgentDataInDB(**dict(zip(column_names, record)))
        else:
            raise HTTPException(status_code=404, detail="Record not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: ProcessedAgentData, db: Session = Depends(connect_to_db)):
    try:
        db.execute(processed_agent_data.insert().values(
            road_state=data.road_state,
            user_id=data.agent_data.user_id,
            x=data.agent_data.accelerometer.x,
            y=data.agent_data.accelerometer.y,
            z=data.agent_data.accelerometer.z,
            latitude=data.agent_data.gps.latitude,
            longitude=data.agent_data.gps.longitude,
            timestamp=data.agent_data.timestamp
        ))
        db.commit()
        return {"message": "OK"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/processed_agent_data/", response_model=list[ProcessedAgentDataInDB])
def list_processed_agent_data(db: Session = Depends(connect_to_db)):
    try:
        data = db.query(processed_agent_data).all()
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def update_processed_agent_data(record_id: int, data: ProcessedAgentData, db: Session = Depends(connect_to_db)):
    try:
        new_data = {
            "road_state": data.road_state,
            "user_id": data.agent_data.user_id,
            "x": data.agent_data.accelerometer.x,
            "y": data.agent_data.accelerometer.y,
            "z": data.agent_data.accelerometer.z,
            "latitude": data.agent_data.gps.latitude,
            "longitude": data.agent_data.gps.longitude,
            "timestamp": data.agent_data.timestamp
        }

        db.execute((
            processed_agent_data.update()
            .where(processed_agent_data.c.id == record_id)
            .values(**new_data)
        ))
        db.commit()

        return ProcessedAgentDataInDB(
            id=record_id,
            road_state=new_data["road_state"],
            user_id=new_data["user_id"],
            x=new_data["x"],
            y=new_data["y"],
            z=new_data["z"],
            latitude=new_data["latitude"],
            longitude=new_data["longitude"],
            timestamp=new_data["timestamp"]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def delete_processed_agent_data(record_id: int, db: Session = Depends(connect_to_db)):
    try:
        data_to_delete = db.execute(select(processed_agent_data).where(processed_agent_data.c.id ==
                                                                       record_id)).fetchone()

        db.execute(processed_agent_data.delete().where(processed_agent_data.c.id == record_id))
        db.commit()

        return ProcessedAgentDataInDB(
            id=data_to_delete[0],
            road_state=data_to_delete[1],
            user_id=data_to_delete[2],
            x=data_to_delete[3],
            y=data_to_delete[4],
            z=data_to_delete[5],
            latitude=data_to_delete[6],
            longitude=data_to_delete[7],
            timestamp=data_to_delete[8]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

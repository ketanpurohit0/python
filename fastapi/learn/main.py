from datetime import datetime
from typing import Optional
from fastapi import FastAPI
from pydantic import BaseModel
from enum import Enum

app = FastAPI()


class Item(BaseModel):
    name: str
    price: float
    is_offer: Optional[bool] = None


class ModelName(str, Enum):
    alexnet = "alexnet"
    resnet = "resnet"
    lenet = "lenet"


@app.get("/")
async def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
async def read_item(item_id: int, q: Optional[str] = None):
    return {"item_id": item_id, "q": q}


@app.put("/items/{item_id}")
async def update_item(item_id: int, item: Item):
    return {"item_price": item.price, "item_id": item_id, "is_offer": item.is_offer}


@app.get("/models/{model_name}")
async def get_model(model_name: ModelName):
    if model_name.value == ModelName.alexnet:
        return {"model_name": model_name, "message": "Deep Learning"}

    if model_name.value == ModelName.lenet:
        return {"model_name": model_name, "message": "LeNet"}

    return {"model_name": model_name, "message": "Residual"}


@app.get("/files/{file_path:path}")
async def get_path(file_path: str):
    return {"file_path": file_path}


@app.get("/query/")
async def query(foo: str, bar: str, dt: datetime = datetime(1963, 4, 15, 15, 43, 12)):
    #  http://127.0.0.1:8000/query/?foo={foo}&bar={bar}
    print(dt)
    return {foo: bar}


@app.get("/query2/")
async def query2(flag: bool):
    # 1, on, true, T, True -> True
    print(flag)
    return {"Flag": flag}


@app.get("/users/{user_id}/items/{item_id}")
async def users(user_id: int, item_id: str, q: str, flag: bool):
    return {"user_id": user_id, "item_id": item_id, "q": q, "flag": flag}

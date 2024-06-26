from analitics.main import count_analytics
from db_uploader.user_data import *

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

db_path = '../sovet5.db'

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/add_user")
async def add_user(seller_name: str, password: str):
    return await create_user(seller_name, password)


@app.get('/analytics')
async def get_analytics(analytics_time_type, left_side=None, right_side=None, marketplace=None):
    result = {'error': False}

    try:
        result = count_analytics(marketplace, analytics_time_type, left_side, right_side)

    except Exception as e:
        print(e)
        result['error'] = True
    finally:
        return result


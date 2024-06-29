from datetime import datetime

from analitics.main import count_dashboard, count_charts
from db_uploader.user_data import *

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

db_path = '../sovet5.db'

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:3000",
    "*"
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


@app.get('/dashboard')
async def get_dashboard(analytics_time_type, left_side=None, right_side=None, marketplace=None):
    result = {'error': False}
    analytics_time_type = analytics_time_type.lower()
    try:
        if analytics_time_type in ["день", "неделя", "месяц", "год", "день"]:
            left_side = datetime.now().date()
        else:
            left_side = left_side.split('T')[0]
            right_side = right_side.split('T')[0]

        if marketplace == 'all':
            marketplace = None

        result = count_dashboard(marketplace, analytics_time_type, left_side, right_side)

    except Exception as e:
        print(e)
        result['error'] = True
    finally:
        return result


@app.get('/charts')
async def get_analytics():
    result = {'error': False}

    try:
        result = count_charts(datetime.now().date())
    except Exception as e:
        print(e)
        result['error'] = True
    finally:
        return result


@app.get('/storage')
async def get_storage():
    result = {'error': False}

    try:
        data = await get_storage_from_db()
        result['data'] = []
        for item in data:
            dct = {'id': item[0], 'count': item[1], 'market': item[2], 'rating': item[3]}
            result['data'].append(dct)

    except Exception as e:
        print(e)
        result['error'] = True
    finally:
        return result

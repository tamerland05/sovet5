from urllib.request import Request

from db_uploader.user_data import *

from tones_bot.source.get_rate import get_rate
from db_worker import *

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import uvicorn


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


@app.get("/utils/{util_type}")
async def utils(util_type: str, token='TON', to='USDT'):
    res = {'error': False}

    try:
        if util_type == 'rate':
            rate = await get_rate(token, to)

            res['rate'] = rate
    except Exception as e:
        print(e)
        res['error'] = True
    finally:
        print(res)
        return res


@app.get("/users/wallet")
async def users_wallet(user_id: int):
    res = {'error': False}

    try:
        res = await get_user_wallet(user_id)
        res['wallet'] = res
    except Exception as e:
        print(e)
        res['error'] = True
    finally:
        print(res)
        return res


@app.get("/users/rounds")
async def users_rounds(user_id: int, rounds_type='current'):
    res = {'error': False}

    try:
        user_rounds = await get_user_rounds(user_id, rounds_type)
        res['rounds_id'] = list(int(r) for r in user_rounds.split())
    except Exception as e:
        print(e)
        res['error'] = True
    finally:
        print(res)
        return res


@app.get("/rounds")
async def main_rounds(rounds_id=None):
    res = {'error': False}

    try:
        rounds = await get_rounds(rounds_id)
        res['rounds'] = rounds[::-1]
    except Exception as e:
        print(e)
        res['error'] = True
    finally:
        return res


@app.get("/rounds/old")
async def main_rounds():
    res = {'error': False}

    try:
        round_id = await get_old_round()
        res['round_id'] = round_id
    except Exception as e:
        print(e)
        res['error'] = True
    finally:
        print(res)
        return res


def main():
    while True:
        try:
            uvicorn.run(
                "app:app",
                host='127.0.0.1',
                port=8080,
                reload=True
            )
        except Exception as e:
            print(e)

from db_selecter.main import *

import aiosqlite
import hashlib


db_name = 'sovet5.db'


async def create_user(seller_name, password):
    query = 'INSERT INTO users (seller_name, password) VALUES (?, ?)'
    password_hash = hashlib.sha256(password).hexdigest()

    async with aiosqlite.connect(db_name) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(query, (seller_name, password_hash))
        await connection.commit()


async def add_marketplace(seller_name, marketplace, api_key):
    seller_id = await get_seller_id(seller_name)

    query = 'INSERT INTO marketplaces_authorisation (seller_id, seller_key, marketplace) VALUES (?, ?, ?)'

    async with aiosqlite.connect(db_name) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(query, (seller_id, api_key, marketplace))
        await connection.commit()



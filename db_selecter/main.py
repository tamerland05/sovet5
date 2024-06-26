import aiosqlite


db_name = 'sovet5.db'


async def get_seller_id(seller_name):
    query = 'SELECT seller_id FROM sellers WHERE seller_name = ?'

    async with aiosqlite.connect(db_name) as connection:
        async with connection.cursor() as cursor:
            return await (await cursor.execute(query, seller_name)).fetchone()[0]


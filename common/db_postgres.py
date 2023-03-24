import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def checkdb(dbpath):
    engine = await connect(dbpath)
    async with engine.connect() as db:
        result = await db.execute(text("SELECT EXISTS ( SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'statuses');"))

    if result.fetchone()[0]:
        print("Database found.")
        return True
    else:
        print("Connected to database, but tables don't exist. Creating...")
        async with engine.begin() as db:
            await db.execute(text("CREATE TABLE statuses (url TEXT NOT NULL PRIMARY KEY, text TEXT, subject TEXT, created INT NOT NULL, language TEXT, bot boolean NOT NULL, reply boolean NOT NULL, attachments boolean NOT NULL)"))
            await db.execute(text("CREATE INDEX statuses_created ON statuses (created)"))
        return True


async def batchwrite(values: list, dbpath):
    engine = await connect(dbpath)
    async with engine.begin() as db:
        await db.execute(text("INSERT INTO statuses VALUES( :url, :text, :subject, :created, :language, :bot, :reply, :attachments) ON CONFLICT(url) DO NOTHING;"), values)


async def connect(dbpath):
    engine = create_async_engine("postgresql+asyncpg://" + dbpath)
    return engine

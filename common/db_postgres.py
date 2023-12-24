import asyncio
from sqlalchemy import text, bindparam, String, Boolean, Integer
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from re import compile
from datetime import datetime, date
from .ap_types import minimalStatus

validDomain = compile("^(((?!\-))(xn\-\-)?[a-z0-9\-_]{0,61}[a-z0-9]{1,1}\.)*(xn\-\-)?([a-z0-9\-]{1,61}|[a-z0-9\-]{1,30})\.[a-z]{2,}$")


async def connect(dbpath):
    engine = create_async_engine("postgresql+asyncpg://" + dbpath)
    return engine


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
            await db.execute(text("CREATE EXTENSION btree_gin;"))
            await db.execute(text("CREATE TABLE statuses (url TEXT NOT NULL PRIMARY KEY, text TEXT, subject TEXT, created BIGINT NOT NULL, language TEXT, bot boolean NOT NULL, reply boolean NOT NULL, attachments boolean NOT NULL)"))
            await db.execute(text("ALTER TABLE statuses ADD ts_text tsvector NULL GENERATED ALWAYS AS (to_tsvector('english'::regconfig, text)) STORED;"))
            await db.execute(text("CREATE INDEX statuses_created ON statuses (created);"))
            await db.execute(text("CREATE INDEX ts_created_idx ON statuses USING gin (created, ts_text);"))

        return True


async def prune(cutoff: int, dbpath):
    if type(cutoff) is not int:
        raise TypeError

    engine = await connect(dbpath)
    async with engine.begin() as db:
        await db.execute(text("DELETE FROM statuses WHERE created < :cutoff;"), ({"cutoff": cutoff}))


async def batchwrite(values: list, dbpath):
    engine = await connect(dbpath)
    async with engine.begin() as db:
        await db.execute(text("INSERT INTO statuses VALUES( :url, :text, :subject, :created, :language, :bot, :reply, :attachments) ON CONFLICT(url) DO NOTHING;"), values)


async def search(dbpath, q,
                 domain: str = None,
                 bots: bool = None,
                 replies: bool = None,
                 attachments: bool = None,
                 limit: int = 50,
                 before: datetime = None,
                 after: datetime = None):
    engine = await connect(dbpath)
    async with engine.connect() as db:
        optionmap = ((bots, 'bot'),
                     (replies, 'reply'),
                     (attachments, 'attachments'))

        staticoptions = ''
        for arg, field in optionmap:
            if arg is None:
                continue
            elif arg is False:
                staticoptions += ' %s = False AND' % field
            elif arg:
                staticoptions += ' %s = True AND' % field

        options = {}
        if before is not None:
            if type(before) is date:
                before = datetime.combine(before, datetime.min.time())

            # Extra anti-skid checks, although FastAPI does validation
            if type(before) is not datetime:
                raise "No."
            ts = int(before.timestamp())

            options['before'] = (' created < %s AND', before.timestamp())

        if after is not None:
            if type(after) is date:
                after = datetime.combine(after, datetime.max.time())

            # Extra anti-skid checks, although FastAPI does validation
            if type(after) is not datetime:
                raise "No."
            ts = int(after.timestamp())

            options['after'] = ('created > %s AND', after.timestamp())

        if domain:
            domain = domain.strip().removeprefix("https://").strip("/").lower()
            if not validDomain.match(domain):
                raise "No."

            for i in ['/', ';', "%", "&"]:
                if i in domain:
                    raise "No."

            options['domain'] = (" statuses.url LIKE %s AND", domain + '%%')

        results = []
        fetches = 0
        while fetches < 5:
            fetches += 1

            optionstext = staticoptions + ''.join([snippet[0] % f":{tag}" for tag, snippet in options.items()])
            psqltext = f"SELECT * FROM statuses WHERE {optionstext} ts_text @@ websearch_to_tsquery( :q ) ORDER BY created + 1 DESC LIMIT {int(limit)};"
            boundparams = [bindparam(tag, value[1]) for tag, value in options.items()]
            psqlquery = text(psqltext).bindparams(*boundparams, bindparam("q", value=q))

            psqlres = await db.execute(psqlquery)
            for row in psqlres:
                results.append(minimalStatus(url="https://" + row[0],
                                             text=row[1],
                                             subject=row[2],
                                             created=datetime.fromtimestamp(int(row[3])).isoformat(),
                                             language=row[4],
                                             bot=row[5],
                                             reply=row[6],
                                             attachments=row[7]).getdict())

            return results

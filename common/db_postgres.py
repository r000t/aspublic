import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
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
            await db.execute(text("CREATE TABLE statuses (url TEXT NOT NULL PRIMARY KEY, text TEXT, subject TEXT, created INT NOT NULL, language TEXT, bot boolean NOT NULL, reply boolean NOT NULL, attachments boolean NOT NULL)"))
            await db.execute(text("CREATE INDEX statuses_created ON statuses (created)"))
        return True


async def batchwrite(values: list, dbpath):
    engine = await connect(dbpath)
    async with engine.begin() as db:
        await db.execute(text("INSERT INTO statuses VALUES( :url, :text, :subject, :created, :language, :bot, :reply, :attachments) ON CONFLICT(url) DO NOTHING;"), values)


async def search(dbpath,
                 and_query, phrase_query, not_query,
                 domain: str = None,
                 bots: bool = None,
                 replies: bool = None,
                 attachments: bool = None,
                 limit: int = 50,
                 before: datetime = None,
                 after: datetime = None):
    async with connect(dbpath) as db:
        optionmap = ((bots, 'bot'),
                     (replies, 'reply'),
                     (attachments, 'attachments'))

        options = ''
        for arg, field in optionmap:
            if arg is None:
                continue
            elif arg is False:
                options += ' %s = 0 AND' % field
            elif arg:
                options += ' %s = 1 AND' % field

        if before is not None:
            if type(before) is date:
                before = datetime.combine(before, datetime.min.time())

            # Extra anti-skid checks, although FastAPI does validation
            if type(before) is not datetime:
                raise "No."
            ts = int(before.timestamp())

            options += ' created < %s AND' % before.timestamp()

        if after is not None:
            if type(after) is date:
                after = datetime.combine(after, datetime.max.time())

            # Extra anti-skid checks, although FastAPI does validation
            if type(after) is not datetime:
                raise "No."
            ts = int(after.timestamp())

            options += ' created > %s AND' % after.timestamp()

        if domain:
            domain = domain.strip().lstrip("https://").strip("/").lower()
            # Less bulletproof anti-skid checks than before, and rn FastAPI is doing *no* validation.
            if not validDomain.match(domain):
                raise "No."

            for i in ['/', ';', " ", "%", "&"]:
                if i in domain:
                    raise "No."

            options += (' url LIKE "%s%%/%%" AND' % domain)

        tsparams = ''
        if not and_query:
            if phrase_query:
                # Phrases aren't handled by postgres; Treat like a multiword AND, enforce exact order later
                for i in phrase_query:
                    tsparams += ' &'.join(phrase_query)
            else:
                return []
        else:
            for i in and_query:
                tsparams += '%s &' % i
            for i in not_query:
                tsparams += '!(%s) &'
        tsparams.rstrip('&')

        # THIS IS TESTING CODE AND IT'S ALMOST CERTAINLY VULNERABLE.
        # 0.1.6 won't release like this, but this comment is here for people viewing git history.
        # IF YOU USE THIS CODE IN PRODUCTION, YOU WILL ALMOST CERTAINLY GET OWNED.
        # THIS CODE IS VULNERABLE TO SQL INJECTION. DO NOT USE IT.
        psqlquery = "SELECT * FROM statuses WHERE %s ts_text @@ to_tsquery(%s) ORDER BY created DESC;" % (options, tsparams)
        print(psqlquery)

        results = []
        async with db.execute(psqlquery, ('"%s"' % tsparams,)) as cursor:
            async for row in cursor:
                results.append(minimalStatus(url="https://" + row[0],
                                             text=row[1],
                                             subject=row[2],
                                             created=datetime.fromtimestamp(int(row[3])).isoformat(),
                                             language=row[4],
                                             bot=row[5],
                                             reply=row[6],
                                             attachments=row[7]).getdict())

        return results

import asyncio
from sqlalchemy import text, bindparam
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
                staticoptions += ' %s = True AND' % field
            elif arg:
                staticoptions += ' %s = False AND' % field

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
            domain = domain.strip().lstrip("https://").strip("/").lower()
            # Less bulletproof anti-skid checks than before, and rn FastAPI is doing *no* validation.
            if not validDomain.match(domain):
                raise "No."

            for i in ['/', ';', " ", "%", "&"]:
                if i in domain:
                    raise "No."

            options['domain'] = (' url LIKE "%s%%/%%" AND', domain)

        # Phrases aren't handled by postgres; Treat like a multiword AND, enforce exact order later
        tsparams = []
        print("phrase %s" % phrase_query)
        if not and_query:
            if not phrase_query:
                return []
        for i in and_query:
            tsparams.append(('%s', i))
        for i in phrase_query:
            tsparams.extend([('%s', word) for word in i.split(' ')])
        for i in not_query:
            tsparams.append(('!(%s)', i))
        #tsparams = tsparams.strip('&')

        # Because of how queries are currently performed, (get *all* results, sort by date)
        # preventing another round-trip to fetch more results is greatly preferred
        if phrase_query:
            psqllimit = limit * 2
        else:
            psqllimit = limit

        results = []
        fetches = 0
        while fetches < 5:
            fetches += 1

            print(tsparams)
            optionstext = staticoptions + ''.join([snippet[0] % f":{tag}" for tag, snippet in options.items()])
            print(optionstext)
            tstext = ' &'.join([snippet[0] % f":x{tag}" for tag, (snippet) in enumerate(tsparams)])
            print(tstext)

            #psqlquery = "SELECT * FROM statuses WHERE %s ts_text @@ to_tsquery('%s') ORDER BY created + 1 DESC LIMIT %s;" % (options, tsparams, int(psqllimit))
            psqltext = f"SELECT * FROM statuses WHERE {optionstext} ts_text @@ to_tsquery('{tstext}') ORDER BY created + 1 LIMIT {psqllimit}"

            boundparams = [bindparam(tag, value[1]) for tag, value in options.items()]
            print(boundparams)
            boundparams.extend([bindparam(f"x{tag}", value[1]) for tag, value in enumerate(tsparams)])
            print(psqltext)
            print(boundparams)
            psqlquery = text(psqltext).bindparams(*boundparams)

            psqlres = await db.execute(psqlquery)
            if not psqlres:
                # No new results after refetching
                return results

            for row in psqlres:
                if any([i.lower() not in row[1].lower() for i in phrase_query]):
                    continue

                results.append(minimalStatus(url="https://" + row[0],
                                             text=row[1],
                                             subject=row[2],
                                             created=datetime.fromtimestamp(int(row[3])).isoformat(),
                                             language=row[4],
                                             bot=row[5],
                                             reply=row[6],
                                             attachments=row[7]).getdict())

                if len(results) == limit:
                    return results

            #TODO: Set up new query
            #if not phrase_query:
            return results
import sqlite3
import aiosqlite
from datetime import datetime, date
from .types import minimalStatus

default_dbpath = "statuses.sqlite3"

def checkdb(dbpath=default_dbpath):
    from os import path
    if path.isfile(dbpath):
        # TODO: Verify it's an sqlite3 database
        pass
    else:
        print("Creating new database at %s..." % dbpath)
        initdb(dbpath)


def initdb(dbpath):
    con = sqlite3.connect(dbpath)
    cur = con.cursor()
    cur.execute("PRAGMA page_size=8192;")
    cur.execute("CREATE TABLE statuses(url TEXT NOT NULL PRIMARY KEY, text TEXT, subject TEXT, created INT NOT NULL, language TEXT, bot INT NOT NULL, reply INT NOT NULL, attachments INT NOT NULL)")
    cur.execute("CREATE INDEX statuses_created ON statuses (created);")
    con.commit()
    con.close()


async def batchwrite(values: list, dbpath):
    async with aiosqlite.connect(dbpath) as db:
        await db.execute("PRAGMA cache_size=4000;")
        await db.executemany("INSERT INTO statuses VALUES(?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(url) DO NOTHING", values)
        await db.commit()


async def search(dbpath, q: str, bots: bool = None, replies: bool = None, attachments: bool = None, limit: int = 50, before: datetime = None, after: datetime = None):
    async with aiosqlite.connect(dbpath) as db:
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

            #Extra anti-skid checks, although FastAPI does validation
            if type(before) is not datetime:
                raise "No."
            ts = int(before.timestamp())

            options += ' created < %s AND' % before.timestamp()

        if after is not None:
            if type(after) is date:
                after = datetime.combine(after, datetime.max.time())

            #Extra anti-skid checks, although FastAPI does validation
            if type(after) is not datetime:
                raise "No."
            ts = int(after.timestamp())

            options += ' created > %s AND' % after.timestamp()

        sqlitequery = "SELECT * FROM statuses WHERE %s text like ? ORDER BY created DESC LIMIT %i;" % (options, int(limit))

        results = []
        async with db.execute(sqlitequery, ('%%%s%%' % q,)) as cursor:
            async for row in cursor:
                results.append(minimalStatus(url= "https://" + row[0],
                                             text=row[1],
                                             subject=row[2],
                                             created=datetime.fromtimestamp(int((row[3]))).isoformat(),
                                             language=row[4],
                                             bot=row[5],
                                             reply=row[6],
                                             attachments=row[7]).getdict())

        return results

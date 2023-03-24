import sqlite3
import aiosqlite
from re import compile
from datetime import datetime, date
from .ap_types import minimalStatus

default_dbpath = "statuses.sqlite3"
validDomain = compile("^(((?!\-))(xn\-\-)?[a-z0-9\-_]{0,61}[a-z0-9]{1,1}\.)*(xn\-\-)?([a-z0-9\-]{1,61}|[a-z0-9\-]{1,30})\.[a-z]{2,}$")


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
    cur.execute("CREATE TABLE statuses(url TEXT NOT NULL PRIMARY KEY, text TEXT, subject TEXT, created INT NOT NULL, language TEXT, bot INT NOT NULL, reply INT NOT NULL, attachments INT NOT NULL);")
    cur.execute("CREATE INDEX statuses_created ON statuses (created);")
    cur.execute("CREATE VIRTUAL TABLE fts_status USING fts5(text, subject, content = 'statuses', tokenize = \"unicode61 remove_diacritics 2\");")
    cur.execute('''
                CREATE TRIGGER IF NOT EXISTS statuses_ai AFTER INSERT ON statuses BEGIN
                  INSERT INTO fts_status(rowid, text, subject) VALUES (new.ROWID, new.text, new.subject);
                END;''')
    cur.execute('''
                CREATE TRIGGER IF NOT EXISTS statuses_ad AFTER DELETE ON statuses BEGIN
                  INSERT INTO fts_status(fts_status, rowid, text, subject) VALUES('delete', old.ROWID, old.text, old.subject);
                END;''')
    cur.execute('''
                CREATE TRIGGER IF NOT EXISTS statuses_au AFTER UPDATE ON statuses BEGIN
                  INSERT INTO fts_status(fts_status, rowid, text, subject) VALUES('delete', old.ROWID, old.text, old.subject);
                  INSERT INTO fts_status(rowid, text, subject) VALUES (new.ROWID, new.text, new.subject);
                END;
                ''')
    con.commit()
    con.close()


async def batchwrite(values: list, dbpath):
    async with aiosqlite.connect(dbpath) as db:
        await db.execute("PRAGMA cache_size=4000;")
        await db.executemany("INSERT INTO statuses VALUES(?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(url) DO NOTHING;", values)
        await db.commit()


async def search(dbpath,
                 q: str,
                 domain: str = None,
                 bots: bool = None,
                 replies: bool = None,
                 attachments: bool = None,
                 limit: int = 50,
                 before: datetime = None,
                 after: datetime = None):
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

        # As a reminder to anybody reading this code...
        # PUTTING VARIABLES INTO A SQL QUERY WITHOUT USING PARAMETERIZATION (the ?s) IS AMAZINGLY, SPECTACULARLY, UNIMAGINABLY DANGEROUS.
        # Don't use this as an example for your own code. I'm a professional, and I know what I'm doing. Now, hold my joint.
        sqlitequery = 'SELECT * FROM statuses t JOIN fts_status f ON t.ROWID = f.ROWID WHERE %s fts_status MATCH ? ORDER BY created DESC LIMIT %i;' % (options, int(limit))
        results = []
        async with db.execute(sqlitequery, ('"%s"' % q,)) as cursor:
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

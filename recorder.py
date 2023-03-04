import asyncio
import msgpack
import zstd
from fastapi import FastAPI, File, Body
from time import time
from common import db
from common.types import minimalStatus

app = FastAPI(title="as:Public Recorder", version="0.1.4")
dbpath = "recorder.sqlite3"
dedupe = {}
unflushed_statuses = {}


def import_statuses(statuses: list[list]):
    statusmap = tuple(statuses[0])
    if statusmap == minimalStatus.__slots__[:-1]:
        #Remote map matches ours, saving some cycles...
        for raw in statuses[1]:
            if raw[0] not in dedupe:
                s = minimalStatus(*raw)
                unflushed_statuses[s.url] = s
            dedupe[raw[0]] = time()

    else:
        for raw in statuses[1]:
            if raw[0] not in dedupe:
                s = minimalStatus(**dict(zip(statusmap, raw)))
                unflushed_statuses[s.url] = s
            dedupe[raw[0]] = time()


async def dbflushworker():
    async def flushtodb():
        begints = time()
        sqlitevalues = [(obj.url, obj.text, obj.subject, obj.created, obj.language, obj.bot, obj.reply, obj.attachments)
                        for url, obj in unflushed_statuses.items()]

        await db.batchwrite(sqlitevalues, dbpath)
        for i in sqlitevalues:
            unflushed_statuses.pop(i[0])

        print("Flushed %i statuses, took %i ms." % (len(sqlitevalues), int((time() - begints) * 1000)))

    lastflush = time()
    while True:
        await asyncio.sleep(10)
        if lastflush + 120 < time():
            if len(unflushed_statuses):
                await flushtodb()


@app.post("/api/recorder/checkin")
async def checkin(statuses: bytes = Body()):
    unpacked = msgpack.loads(zstd.ZSTD_uncompress(statuses))

    import_statuses(unpacked)
    print("Now has %i statuses in cache" % len(unflushed_statuses))
    return


@app.on_event("startup")
async def recorder_startup():
    db.checkdb(dbpath)
    asyncio.create_task(dbflushworker())

import asyncio
from typing import List, Optional, Union
from pydantic import BaseModel
from fastapi import FastAPI, File, Body
from fastapi.staticfiles import StaticFiles
from time import time
from datetime import datetime, date
from common import db
from common.types import minimalStatus
import msgpack
import zstd
import argparse
import sys

app = FastAPI(title="as:Public Recorder", version="0.1.4")
dbpath = "recorder.sqlite3"
dedupe = {}
unflushed_statuses = set()


def import_statuses(statuses: list[list]):
    statusmap = tuple(statuses[0])
    if statusmap == minimalStatus.__slots__[:-1]:
        #Remote map matches ours, saving some cycles...
        for raw in statuses[1]:
            if raw[0] not in dedupe:
                s = minimalStatus(*raw)
                unflushed_statuses.add(s.url)
            dedupe[raw[0]] = time()

    else:
        for raw in statuses[1]:
            if raw[0] not in dedupe:
                s = minimalStatus(**dict(zip(statusmap, raw)))
                unflushed_statuses.add(s.url)
            dedupe[raw[0]] = time()


async def dbflushworker():
    def flushtodb():
        pass

    lastflush = time()
    while True:
        await asyncio.sleep(10)
        if lastflush + 120 < time():
            if len(unflushed_statuses):
                flushtodb()


@app.post("/api/recorder/checkin")
async def checkin(statuses: bytes = Body()):
    print(len(statuses))
    unpacked = msgpack.loads(zstd.ZSTD_uncompress(statuses))
    print(len(unpacked))

    import_statuses(unpacked)
    print("Now has %i statuses in cache" % len(unflushed_statuses))
    return


@app.on_event("startup")
async def recorder_startup():
    db.checkdb(dbpath)

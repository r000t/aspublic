import asyncio
import msgpack
import toml
import zstd
import re
from fastapi import FastAPI, File, Body
from time import time
from common import db_sqlite, db_postgres
from common.ap_types import minimalStatus

app = FastAPI(title="as:Public Recorder", version="0.1.5")
dedupe = {}
unflushed_statuses = {}


def process_filters(status: minimalStatus):
    def process_policy(policy):
        for i in policy["domain"]:
            if domain.endswith(i):
                return True

        for i in policy["regex"]:
            if i.search(status.text.lower()):
                return True
            if i.search(status.subject.lower()):
                return True

        return False

    domain = status.url.split('/')[0]

    if filters["policy"] == "accept":
        if process_policy(filters["reject"]):
            return process_policy(filters["accept"])
        return True

    elif filters["policy"] == "reject":
        if process_policy(filters["accept"]):
            return not process_policy(filters["reject"])


def import_statuses(statuses: list[list]):
    statusmap = tuple(statuses[0])
    if statusmap == minimalStatus.__slots__[:-1]:
        #Remote map matches ours, saving some cycles...
        for raw in statuses[1]:
            if raw[0] not in dedupe:
                s = minimalStatus(*raw)
                if process_filters(s):
                    unflushed_statuses[s.url] = s
            dedupe[raw[0]] = time()

    else:
        for raw in statuses[1]:
            if raw[0] not in dedupe:
                s = minimalStatus(**dict(zip(statusmap, raw)))
                if process_filters(s):
                    unflushed_statuses[s.url] = s
            dedupe[raw[0]] = time()


async def dbflushworker():
    async def flushtodb():
        begints = time()

        if dbconfig["db_backend"].startswith("sqlite"):
            sqlvalues = [(obj.url, obj.text, obj.subject, obj.created, obj.language, obj.bot, obj.reply, obj.attachments)
                            for url, obj in unflushed_statuses.items()]

            await db_sqlite.batchwrite(sqlvalues, dbconfig["db_url"])
            for i in sqlvalues:
                unflushed_statuses.pop(i[0])

        elif dbconfig["db_backend"].startswith("postgres"):
            sqlvalues = [obj.getdict() for obj in unflushed_statuses.values()]
            await db_postgres.batchwrite(sqlvalues, dbconfig["db_url"])
            for i in sqlvalues:
                unflushed_statuses.pop(i["url"])

        print("Flushed %i statuses, took %i ms." % (len(sqlvalues), int((time() - begints) * 1000)))

    lastflush = time()
    while True:
        await asyncio.sleep(10)
        if lastflush + 120 < time():
            if len(unflushed_statuses):
                await flushtodb()
            lastflush = time()


@app.post("/api/recorder/checkin")
async def checkin(statuses: bytes = Body()):
    unpacked = msgpack.loads(zstd.ZSTD_uncompress(statuses))

    import_statuses(unpacked)
    print("Now has %i statuses in cache" % len(unflushed_statuses))
    return


@app.on_event("startup")
async def recorder_startup():
    global config, dbconfig, filters

    with open("recorder.toml", "r") as f:
        config = toml.load(f)

    filters = {"policy": "accept", "reject": {}, "accept": {}}
    if "filters" in config:
        for policy in ["reject", "accept"]:
            for criteria in ["regex", "domain"]:
                try:
                    pitems = set(config["filters"][policy][criteria])
                except KeyError:
                    pitems = set()

                try:
                    for file in config["filters"][policy][criteria+'_files']:
                        if criteria == 'regex':
                            pitems.update([i.strip() for i in open(file).readlines()])
                        else:
                            pitems.update([i.strip().split(',')[0] for i in open(file).readlines()])
                except KeyError:
                    pass

                pitems.discard('')
                if criteria == 'regex':
                    filters[policy][criteria] = [re.compile(i) for i in pitems]
                else:
                    filters[policy][criteria] = list(pitems)
    print(filters)

    dbconfig = config["database"]
    if dbconfig["db_backend"].startswith("sqlite"):
        db_sqlite.checkdb(dbconfig["db_url"])
    elif dbconfig["db_backend"].startswith("postgres"):
        await db_postgres.checkdb(dbconfig["db_url"])
    else:
        print("Configuration Error: db_backend must be 'sqlite' or 'postgres'")
        exit()

    asyncio.create_task(dbflushworker())


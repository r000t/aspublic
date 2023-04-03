import asyncio
import toml
from typing import List, Optional, Union
from pydantic import BaseModel, Field, HttpUrl, AnyUrl
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from attrs import define, field
from re import findall
from time import time
from datetime import datetime, date
from common import db_sqlite, db_postgres
app = FastAPI(title="as:Public Viewer", version="0.1.6")

# Path to config file
config_path = "viewer.toml"

## Set to True to serve static files from the application. You'll need to do that to run it locally.
## If you're running this even semi-publicly, see the README for a sample nginx configuration.
mountLocalDirectory = False


try:
    with open(config_path, "r") as f:
        config = toml.load(f)
except FileNotFoundError:
    print("Couldn't find configuration at %s" % config_path)
    exit()


class StatusModel(BaseModel):
    url: HttpUrl = Field("http://example.com/@user/1001", description="URL the status is located at, on its originating server")
    subject: str = Field("Example Status", description="Subject/Spoiler/Content Warning")
    text: str = Field("This is the body of an example status", description="Body of the status")
    created: datetime = Field(datetime.utcfromtimestamp(0), description="Creation timestamp of the status, in ISO 8601 format")
    language: Optional[str] = Field("en", description="Primary language of the status, in ISO 639 Part 1 two-letter format")
    bot: bool = Field(True, description="Whether the author's account was marked as a bot when the status was collected")
    reply: bool = Field(False, escription="Whether the status was a reply to another status")
    attachments: bool = Field(False, description="Whether the status had any media attachments")


class QueryModel(BaseModel):
    and_terms: List[str] = Field(description="Individual terms that must appear in at least one field being searched")
    and_phrases: List[str] = Field(description="Multiword phrases that must appear, in the given order, in at least one field being searched")
    not_terms: List[str] = Field(description="Individual terms that must not appear in any field being searched")


class DebugModel(BaseModel):
    dbtime_ms: Optional[int] = Field(description="The time taken for the database query to finish, in ms")
    query: Optional[QueryModel] = Field(description="The query that was presented to the search backend, after parsing")


class ResultModel(BaseModel):
    results: List[StatusModel] = Field(description="The actual list of search results")
    debug: Optional[DebugModel] = Field(description="Optional information helpful for debugging and performance tuning")


@define
class searchBackend:
    path: str
    parsequery: bool = field(default=False, kw_only=True)

    @staticmethod
    def translateSearchString(searchString):
        query = searchString
        and_query = searchString[:]

        not_ops = ['!', '-']
        not_ops_re = '|'.join(not_ops)
        not_query = []
        for spair in [(r'[%s]"[^"]*"', '"'), (r'[%s]\w+', '')]:
            res = findall(spair[0] % not_ops_re, query)
            for q in res:
                and_query = and_query.replace(q, '')
                not_query.append(q.strip(''.join(not_ops)).strip('"'))

        phrase_query = findall(r'"([^"]*)"', and_query)
        for q in phrase_query:
            and_query = and_query.replace('"%s"' % q, '')

        and_query = [answer.strip() for answer in and_query.split()]
        phrase_query = [answer.strip() for answer in phrase_query]
        not_query = [answer.strip(' %s\n' % ''.join(not_ops)) for answer in not_query]

        return and_query, phrase_query, not_query

    async def search(self, q, *args, **kwargs):
            and_query, phrase_query, not_query = self.translateSearchString(q)
            if self.parsequery:
                res = await self._search(and_query, phrase_query, not_query, *args, **kwargs)
            else:
                res = await self._search(q, *args, **kwargs)

            return res, (and_query, phrase_query, not_query)

    async def _search(self, and_query, phrase_query, not_query, *args, **kwargs):
        pass


@define
class sqliteSearchBackend(searchBackend):
    parsequery: bool = field(default=True, kw_only=True)

    async def _search(self, and_query, phrase_query, not_query, *args, **kwargs):
        res = await db_sqlite.search(self.path, and_query, phrase_query, not_query, **kwargs)
        return res


@define
class postgresSearchBackend(searchBackend):
    async def _search(self, q, *args, **kwargs):
        res = await db_postgres.search(self.path, q, *args, **kwargs)
        return res


@app.get("/api/unstable/search", response_model=ResultModel)
async def read_item(q: str = Query(title="Search query with standard operators"),
                    domain: str = Query(default=None, title="Return only results from this domain"),
                    bots: bool = None,
                    replies: bool = None,
                    attachments: bool = None,
                    before: Union[datetime, date] = None,
                    after: Union[datetime, date] = None,
                    limit: int = Query(default=50, ge=1, le=100, title="Number of results to return")):
    """Early search function. Case-insensitive. Standard search operators are supported.
    \n\nbots, replies, and attachments fields are optional. If they are not specified, or null, all posts are shown.
    If True, **only** that type of post will be shown. If False, that type of post will **not** be shown."""
    begints = time()
    results, parsedquery = await db.search(q=q,
                                           domain=domain,
                                           bots=bots,
                                           replies=replies,
                                           attachments=attachments,
                                           limit=limit,
                                           before=before,
                                           after=after)

    return {"results": results,
            "debug": {"dbtime_ms": int(((time() - begints) * 1000)),
                      "query": dict(zip(["and_terms", "and_phrases", "not_terms"], parsedquery))}}


@app.on_event("startup")
async def recorder_startup():
    global db

    dbconfig = config["database"]
    if dbconfig["db_backend"].startswith("sqlite"):
        db_sqlite.checkdb(dbconfig["db_url"])
        db = sqliteSearchBackend(path=dbconfig["db_url"])
    elif dbconfig["db_backend"].startswith("postgres"):
        await db_postgres.checkdb(dbconfig["db_url"])
        db = postgresSearchBackend(path=dbconfig["db_url"])
    else:
        print("Configuration Error: db_backend must be 'sqlite' or 'postgres'")
        exit()


if mountLocalDirectory:
    app.mount("/", StaticFiles(directory="viewer-static", html=True), name="frontend")
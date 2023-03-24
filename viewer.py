import asyncio
from typing import List, Optional, Union
from pydantic import BaseModel, Field, HttpUrl, AnyUrl
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from attrs import define
from re import findall
from time import time
from datetime import datetime, date
from common import db_sqlite
app = FastAPI(title="as:Public Viewer", version="0.1.5")

## Path to status database from a Collector
#dbpath = 'statuses.sqlite3'
dbpath = db_sqlite.default_dbpath

## Set to True to serve static files from the application. You'll need to do that to run it locally.
## If you're running this even semi-publicly, see the README for a sample nginx configuration.
mountLocalDirectory = True


class StatusModel(BaseModel):
    url: HttpUrl = Field("http://example.com/@user/1001", description="URL the status is located at, on its originating server")
    subject: str = Field("Example Status", description="Subject/Spoiler/Content Warning")
    text: str = Field("This is the body of an example status", description="Body of the status")
    created: datetime = Field(datetime.utcfromtimestamp(0), description="Creation timestamp of the status, in ISO 8601 format")
    language: Optional[str] = Field("en", description="Primary language of the status, in ISO 639 Part 1 two-letter format")
    bot: bool = Field(True, description="Whether the author's account was marked as a bot when the status was collected")
    reply: bool = Field(False, escription="Whether the status was a reply to another status")
    attachments: bool = Field(False, description="Whether the status had any media attachments")


class DebugModel(BaseModel):
    dbtime_ms: Optional[int] = Field(description="The time taken for the database query to finish, in ms")


class ResultModel(BaseModel):
    results: List[StatusModel] = Field(description="The actual list of search results")
    debug: Optional[DebugModel] = Field(description="Optional information helpful for debugging and performance tuning")


@define
class searchBackend():
    def translateSearchString(self, searchString):
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

    async def search(self, searchString, *args, **kwargs):
        and_query, phrase_query, not_query = self.translateSearchString(searchString)
        return self._search(and_query, phrase_query, not_query, *args, **kwargs)

    async def _search(self, and_query, phrase_query, not_query, *args, **kwargs):
        pass




@app.get("/api/unstable/search", response_model=ResultModel)
async def read_item(q: str = Query(title="Single search term to look for in status text/body"),
                    domain: str = Query(default=None, title="Return only results from this domain"),
                    bots: bool = None,
                    replies: bool = None,
                    attachments: bool = None,
                    before: Union[datetime, date] = None,
                    after: Union[datetime, date] = None,
                    limit: int = Query(default=50, ge=1, le=100, title="Number of results to return")):
    """Early search function. Case-insensitive. Does not support operators. Entire query is treated as a single term.
    \n\nbots, replies, and attachments fields are optional. If they are not specified, or null, all posts are shown.
    If True, **only** that type of post will be shown. If False, that type of post will **not** be shown."""
    begints = time()
    results = await db.search(db.default_dbpath,
                              q=q,
                              domain=domain,
                              bots=bots,
                              replies=replies,
                              attachments=attachments,
                              limit=limit,
                              before=before,
                              after=after)

    return {"results": results, "debug": {"dbtime_ms": int(((time() - begints) * 1000))}}

if mountLocalDirectory:
    app.mount("/", StaticFiles(directory="viewer-static", html=True), name="frontend")
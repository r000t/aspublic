import asyncio
from typing import List, Optional, Union
from pydantic import BaseModel, Field, HttpUrl
from fastapi import FastAPI, Query
from time import time
from datetime import datetime, date
from common import db

app = FastAPI(title="as:Public")


class StatusModel(BaseModel):
    url: HttpUrl = Field(description="URL the status is located at, on its originating server")
    text: str = Field(description="Body of the status")
    subject: str = Field(description="Subject/Spoiler/Content Warning")
    created: datetime = Field(description="Creation timestamp of the status, in ISO 8601 format")
    language: str = Field("en", description="Primary language of the status, in ISO 639 Part 1 two-letter format")
    bot: bool = Field(description="Whether the author's account was marked as a bot when the status was collected")
    reply: bool = Field(description="Whether the status was a reply to another status")
    attachments: bool = Field(description="Whether the status had any media attachments")


class DebugModel(BaseModel):
    dbtime_ms: Optional[int] = Field(description="The time taken for the database query to finish, in ms")


class ResultModel(BaseModel):
    results: List[StatusModel] = Field(description="The actual list of search results")
    debug: Optional[DebugModel] = Field(description="Optional information helpful for debugging and performance tuning")


@app.get("/api/unstable/search", response_model=ResultModel)
async def read_item(q: str = Query(title="Single search term to look for in status text/body"),
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
                              bots=bots,
                              replies=replies,
                              attachments=attachments,
                              limit=limit,
                              before=before,
                              after=after)

    return {"results": results, "debug": {"dbtime_ms": int(((time() - begints) * 1000))}}
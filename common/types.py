import attrs
from attrs import define, asdict
from time import time


@define(slots=True)
class minimalStatus:
    url: str
    text: str
    subject: str
    created: int
    language: str
    bot: bool
    reply: bool
    attachments: bool

    def getdict(self):
        return asdict(self)


@define(slots=True)
class listenerStats:
    domain: str
    method: str = "..."
    status: int = 0
    lastStatusTimestamp: int = 0
    lastHeartbeatTimestamp: int = 0
    receivedStatusCount: int = 0
    uniqueStatusCount: int = 0
    startedTimestamp: int = time()


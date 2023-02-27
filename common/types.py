import attr.setters
import attrs
from attrs import define, asdict, field
from time import time

defaultZeroIntField = field(default=0, converter=int, on_setattr=attr.setters.convert)

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
    lastStatusTimestamp: int = defaultZeroIntField
    lastHeartbeatTimestamp: int = defaultZeroIntField
    lastRetryTimestamp: int = defaultZeroIntField
    receivedStatusCount: int = 0
    uniqueStatusCount: int = 0
    rejectedStatusCount: int = 0



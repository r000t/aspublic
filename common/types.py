import attrs
from attrs import define, asdict


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
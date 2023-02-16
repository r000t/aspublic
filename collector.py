#!/usr/bin/python3
import asyncio
from time import time
from datetime import datetime, timezone
from html2text import HTML2Text
from mastodon import Mastodon, streaming
from common import db
from common.types import minimalStatus

domains = ["mastodon.social",
           "mastodon.online",
           "mastodon.world",
           "mastodon.xyz",
           "mastodon.art",
           "mastodon.cloud",
           "hachyderm.io",
           "fosstodon.org",
           "witches.live",
           "masto.ai",
           "mstdn.social",
           "mas.to",
           "infosec.exchange"]


# TODO: Put back in collectorLoop once stream listener no longer needs threads
unsent_statuses = {}
dedupe = {}


class mpyStreamListener(streaming.StreamListener):
    def on_update(self, s):

        # This is the opt-out mechanism.
        if s['visibility'] != 'public':
            return

        if s['url'] not in dedupe:
            dt = datetime.fromisoformat(s['created_at'])
            if dt.tzinfo is None:
                print("Fixing naive datetime...")
                dt.replace(tzinfo=timezone.utc)

            extract = minimalStatus(url=s['url'].split('://')[1],
                                    text=htmlparser.handle(s['content']).strip(),
                                    subject=s['spoiler_text'],
                                    created=int(dt.timestamp()),
                                    language=s['language'],
                                    bot=s['account']['bot'],
                                    reply=s['in_reply_to_id'] is not None,
                                    attachments=len(s['media_attachments']) is not 0)
            #[print("%s: %s" % (i, s[i])) for i in s.keys()]

            unsent_statuses[extract.url] = extract
            dedupe[extract.url] = time()

        else:
            # Skip status, update last seen time
            #print("Duplicate status %s" % s['url'])
            dedupe[s['url']] = time()


async def flushtodb():
    begints = time()

    # A list comprehension is beautiful at ANY size.
    sqlitevalues = [(obj.url, obj.text, obj.subject, obj.created, obj.language, obj.bot, obj.reply, obj.attachments)
                    for url, obj in unsent_statuses.items()]

    await db.batchwrite(sqlitevalues, db.default_dbpath)
    for i in sqlitevalues:
        unsent_statuses.pop(i[0])
    print("Flushed %i statuses, took %s seconds." % (len(sqlitevalues),
                                                     str(time() - begints)))


async def mpyWebsocketWorker(domain):
    mpyClient = Mastodon(api_base_url=domain)
    mpyClient.stream_public(mpyStreamListener(), run_async=True, reconnect_async=True)


async def collectorLoop():
    workers = {}
    for domain in domains:
        workers[domain] = asyncio.create_task(mpyWebsocketWorker(domain))

    while True:
        await asyncio.sleep(60)
        if len(unsent_statuses):
            print("Flushing statuses to disk...")
            await flushtodb()
            print("Pruning dedupe cache...")
            ts = int(time())
            delqueue = []
            for k, v in dedupe.items():
                if v + 60 > ts:
                    #print("Pruning %s" % k)
                    #I'd delete them here, but you can't do that while iterating over the dict.
                    delqueue.append(k)

            for i in delqueue:
                del dedupe[i]



if __name__ == '__main__':
    db.checkdb(db.default_dbpath)

    htmlparser = HTML2Text()
    htmlparser.ignore_links = True
    htmlparser.body_width = 0
    asyncio.run(collectorLoop())
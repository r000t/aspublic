#!/usr/bin/python
import asyncio
import argparse
from time import time
from datetime import datetime, timezone
from rich.console import Console
from rich.table import Table
from html2text import HTML2Text
from mastodon import Mastodon, streaming
from common import db
from common.types import minimalStatus

parser = argparse.ArgumentParser(prog="as:Public Standalone Collector")
parser.add_argument('mode', choices=["run", "test"])
parser.add_argument('--db', default=db.default_dbpath, help="Use/create sqlite3 database at this location")
parser.add_argument('-s', '--server', action='append', default=[],
                    help="Connect to this server. You can call this multiple times.")
parser.add_argument('-l', '--list', action='append', default=[],
                    help="Read servers from this list. It can be a CSV if the first field is the domain.")
parser.add_argument('--useragent', type=str, help="HTTP User-Agent to present when opening connections")
parser.add_argument('--debug', action="store_true", help="Enable verbose output useful for debugging")


class mpyStreamListener(streaming.StreamListener):
    def __init__(self, state):
        self.state = state

        self.htmlparser = HTML2Text()
        self.htmlparser.ignore_links = True
        self.htmlparser.body_width = 0

    def on_update(self, s):

        # This is the opt-out mechanism.
        if s['visibility'] != 'public':
            return

        self.state[2] = time()
        self.state[4] += 1
        if s['url'] not in dedupe:
            self.state[5] += 1

            # This literally seemed to change on a dime after a week of working fine. Adding check.
            if type(s['created_at']) is datetime:
                dt = s['created_at']
            elif type(s['created_at']) is str:
                dt = datetime.fromisoformat(s['created_at'])

            if dt.tzinfo is None:
                print("Fixing naive datetime...")
                dt.replace(tzinfo=timezone.utc)

            extract = minimalStatus(url=s['url'].split('://')[1],
                                    text=self.htmlparser.handle(s['content']).strip(),
                                    subject=s['spoiler_text'],
                                    created=int(dt.timestamp()),
                                    language=s['language'],
                                    bot=s['account']['bot'],
                                    reply=s['in_reply_to_id'] is not None,
                                    attachments=len(s['media_attachments']) is not 0)

            unsent_statuses[extract.url] = extract
            dedupe[s['url']] = time()

        else:
            # Skip status, update last seen time
            dedupe[s['url']] = time()

    def handle_heartbeat(self):
        self.state[3] = time()


async def flushtodb():
    begints = time()

    # A list comprehension is beautiful at ANY size.
    sqlitevalues = [(obj.url, obj.text, obj.subject, obj.created, obj.language, obj.bot, obj.reply, obj.attachments)
                    for url, obj in unsent_statuses.items()]

    await db.batchwrite(sqlitevalues, db.default_dbpath)
    for i in sqlitevalues:
        unsent_statuses.pop(i[0])
    print("Flushed %i statuses, took %s seconds." % (len(sqlitevalues), str(time() - begints)))


async def mpyWebsocketWorker(id, domain, testing=False):
    mpyClient = Mastodon(api_base_url=domain)
    healthy = await asyncio.to_thread(mpyClient.stream_healthy)
    if not healthy:
        print("[!] [%s] Streaming API is down! Exiting." % domain)
        return False
    if testing or args.debug:
        print("[+] [%s] Streaming API is healthy." % domain)
        if testing:
            return True

    mpyClient.stream_public(mpyStreamListener(workers[id]), run_async=True, reconnect_async=True)


async def collectorLoop(domains):
    global workers

    print("Starting workers for %i domains..." % len(domains))
    id = 0
    for domain in domains:
        workers[id] = [asyncio.create_task(mpyWebsocketWorker(id, domain)), domain, 0, 0, 0, 0]
        id += 1

    c = Console()
    lastflush = time()
    while True:
        await asyncio.sleep(5)

        c.clear()
        statusScreen(c)
        if lastflush + 120 < time():
            if len(unsent_statuses):
                if args.debug:
                    print("Flushing statuses to disk...")
                await flushtodb()
                if args.debug:
                    print("Pruning dedupe cache...")
                ts = int(time())
                delqueue = []
                for k, v in dedupe.items():
                    if v + 600 < ts:
                        # I'd delete them here, but you can't do that while iterating over the dict.
                        delqueue.append(k)

                for i in delqueue:
                    del dedupe[i]

            lastflush = time()


def buildDomainList(args):
    domains = set([i for i in args.server])
    for domainfile in args.list:
        with open(domainfile) as f:
            domains.update([i.strip().split(',')[0] for i in f.readlines()])
    return list(domains)


async def testDomains(domains):
    workers = [(i, asyncio.create_task(mpyWebsocketWorker(i, testing=True))) for i in domains]
    await asyncio.gather(*[i[1] for i in workers])

    table = Table()
    table.add_column("Domain")
    table.add_column("Streaming Ready")
    for i in workers:
        table.add_row(i[0], str(i[1].result()))
    c = Console()
    c.print(table)


def statusScreen(c):
    t = Table()
    t.add_column("Worker")
    t.add_column("Domain")
    t.add_column("Last Status")
    t.add_column("Last Heartbeat")
    t.add_column("Statuses")
    t.add_column("Unique")

    for k,v in workers.items():
        t.add_row(str(k),
                  v[1],
                  datetime.fromtimestamp(v[2]).strftime("%H:%M:%S"),
                  datetime.fromtimestamp(v[3]).strftime("%H:%M:%S"),
                  str(v[4]), str(v[5]))

    c.print("as:Public Standalone Collector")
    c.print(t)
    c.print("Unwritten statuses: %i" % len(unsent_statuses))
    c.print("Dedupe cache size: %i" % len(dedupe))


if __name__ == '__main__':
    args = parser.parse_args()

    domains = buildDomainList(args)
    if args.mode == "run":
        db.checkdb(args.db)

        # Setup global variables before starting async loop
        workers = {}
        unsent_statuses = {}
        dedupe = {}

        asyncio.run(collectorLoop(domains))

    elif args.mode == "test":
        asyncio.run(testDomains(domains))
#!/usr/bin/python
import asyncio
import argparse
from time import time
from datetime import datetime, timezone
from html2text import HTML2Text
from mastodon import Mastodon, streaming
from mastodon.errors import MastodonError
from common import db
from common.types import minimalStatus

parser = argparse.ArgumentParser(prog="as:Public Standalone Collector")
parser.add_argument('mode', choices=["run", "test"])
parser.add_argument('--db', default=db.default_dbpath, help="Use/create sqlite3 database at this location")
parser.add_argument('-s', '--server', action='append', default=[],
                    help="Connect to this server. You can call this multiple times.")
parser.add_argument('-l', '--list', action='append', default=[],
                    help="Read servers from this list. It can be a CSV if the first field is the domain.")
parser.add_argument('--useragent', type=str, help="HTTP User-Agent to present when opening connections",
                    default="Collector/0.1.2")
parser.add_argument('--discover', action="store_true", help="Automatically try to listen to new instances")
parser.add_argument('--debug', action="store_true", help="Enable verbose output useful for debugging")
parser.add_argument('--nostatus', action="store_true", help="Don't show status screen. May save RAM.")


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

            if args.discover:
                domain = extract.url.split('/')[0]
                if domain not in discoveredDomains:
                    discoveredDomains[domain] = 0

        else:
            # Skip status, update last seen time
            dedupe[s['url']] = time()

    def handle_heartbeat(self):
        self.state[3] = time()


class mpyStreamTester(streaming.StreamListener):
    def __init__(self):
        self.gotStatus = False
        self.gotHeartbeat = False

    def on_update(self, status):
        self.gotStatus = True

    def on_heartbeat(self):
        self.gotHeartbeat = True


async def flushtodb():
    begints = time()

    # A list comprehension is beautiful at ANY size.
    sqlitevalues = [(obj.url, obj.text, obj.subject, obj.created, obj.language, obj.bot, obj.reply, obj.attachments)
                    for url, obj in unsent_statuses.items()]

    await db.batchwrite(sqlitevalues, db.default_dbpath)
    for i in sqlitevalues:
        unsent_statuses.pop(i[0])
    print("Flushed %i statuses, took %s seconds." % (len(sqlitevalues), str(time() - begints)))


def mpyStreamingWorker(wid, domain):
    mpyClient = Mastodon(api_base_url=domain, user_agent=args.useragent)
    mpyClient.stream_public(mpyStreamListener(workers[wid]), run_async=True, reconnect_async=True)


async def spawnCollectorWorker(domain):
    try:
        wid = max(workers.keys()) + 1
    except ValueError:
        wid = 0
    workers[wid] = [asyncio.to_thread(mpyStreamingWorker, wid, domain), domain, 0, 0, 0, 0]
    await workers[wid][0]


async def mpyTestDomain(domain, timeout=15):
    mpyClient = Mastodon(api_base_url=domain, user_agent=args.useragent, request_timeout=5)
    try:
        healthy = await asyncio.to_thread(mpyClient.stream_healthy)
    except MastodonError:
        return False

    if not healthy:
        if args.debug:
            print("[!] [%s] Streaming API is down! Giving up." % domain)
        return False

    if args.debug:
        print("[+] [%s] Streaming API is healthy." % domain)

    try:
        testListener = mpyStreamTester()
        testWorker = mpyClient.stream_public(testListener, run_async=True)
    except MastodonError:
        return False

    waited = 0
    while waited < timeout:
        if testListener.gotHeartbeat or testListener.gotStatus:
            testWorker.close()
            return True
        await asyncio.sleep(.5)
        waited += .5

    testWorker.close()
    return False


async def discoverDomain(domain):
    res = await mpyTestDomain(domain)
    if res:
        if args.debug:
            print("[+] [%s] Passed testing, now listening." % domain)
        discoveredDomains[domain] = 2
        await spawnCollectorWorker(domain)
    else:
        discoveredDomains[domain] = -2


async def collectorLoop(domains):
    global workers

    print("Starting workers for %i domains..." % len(domains))
    for domain in domains:
        await spawnCollectorWorker(domain)

    if not args.nostatus:
        c = Console()
    lastflush = time()
    while True:
        if not args.nostatus:
            c.clear()
            statusScreen(c)

        if lastflush + 120 < time():
            if len(unsent_statuses):
                if args.debug:
                    print("Flushing statuses to disk...")
                await flushtodb()
            lastflush = time()

            if args.debug:
                print("Pruning dedupe cache...")
            ts = int(time())
            for i in [k for k, v in dedupe.items() if v + 600 < ts]:
                del dedupe[i]

        if args.discover:
            maxkickoffs = 5
            kickoffs = 0
            undiscovered = [k for k, v in discoveredDomains.items() if v == 0]
            for domain in undiscovered:
                discoveredDomains[domain] = -1
                asyncio.create_task(discoverDomain(domain))
                kickoffs += 1
                if kickoffs >= maxkickoffs:
                    break

        await asyncio.sleep(5)


def buildDomainList(args):
    domains = set([i for i in args.server])
    for domainfile in args.list:
        with open(domainfile) as f:
            domains.update([i.strip().split(',')[0] for i in f.readlines()])
    return list(domains)


async def testDomains(domains):
    workers = [(i, asyncio.create_task(mpyTestDomain(i))) for i in domains]
    await asyncio.gather(*[i[1] for i in workers])

    if args.nostatus:
        for i in workers:
            print("%s: %s" % (i[0], {True: "PASS", False: "FAIL"}[i[1]]))
    else:
        c = Console()
        table = Table()
        table.add_column("Domain")
        table.add_column("Streaming Ready")
        for i in workers:
            table.add_row(i[0], str(i[1].result()))
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

    if not args.nostatus:
        from rich.console import Console
        from rich.table import Table

    domains = buildDomainList(args)
    if args.mode == "run":
        db.checkdb(args.db)

        # Setup global variables before starting async loop
        workers = {}
        unsent_statuses = {}
        dedupe = {}
        discoveredDomains = {i:1 for i in domains}

        import cProfile
        asyncio.run(collectorLoop(domains))

    elif args.mode == "test":
        asyncio.run(testDomains(domains))
#!/usr/bin/python
import argparse
import asyncio
import aiohttp
import websockets
import json
import dateutil
from time import time
from datetime import datetime, timezone
from html2text import HTML2Text
from mastodon import Mastodon, streaming
from mastodon.errors import MastodonError
from common import db
from common.types import minimalStatus, listenerStats

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


def importStatus(s, stats: listenerStats, htmlparser: HTML2Text):
    stats.lastStatusTimestamp = time()
    stats.receivedStatusCount += 1
    if s['url'] not in dedupe:
        stats.uniqueStatusCount += 1

        # This literally seemed to change on a dime after a week of working fine. Adding check.
        if type(s['created_at']) is datetime:
            dt = s['created_at']
        elif type(s['created_at']) is str:
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

        unsent_statuses[extract.url] = extract
        dedupe[s['url']] = time()

        if args.discover:
            domain = extract.url.split('/')[0]
            if domain not in discoveredDomains:
                discoveredDomains[domain] = 0

    else:
        # Skip status, update last seen time
        dedupe[s['url']] = time()


class AttribAccessDict(dict):
    def __getattr__(self, attr):
        if attr in self:
            return self[attr]
        else:
            raise AttributeError(f"Attribute not found: {attr}")

    def __setattr__(self, attr, val):
        if attr in self:
            raise AttributeError("Attribute-style access is read only")
        super(AttribAccessDict, self).__setattr__(attr, val)


class nativeWebsocketsListener():
    @staticmethod
    def __json_allow_dict_attrs(json_object):
        """
        Makes it possible to use attribute notation to access a dicts
        elements, while still allowing the dict to act as a dict.
        """
        if isinstance(json_object, dict):
            return AttribAccessDict(json_object)
        return json_object

    @staticmethod
    def __json_date_parse(json_object):
        """
        Parse dates in certain known json fields, if possible.
        """
        known_date_fields = ["created_at", "week", "day", "expires_at", "scheduled_at",
                                "updated_at", "last_status_at", "starts_at", "ends_at", "published_at", "edited_at", "date", "period"]
        mark_delete = []
        for k, v in json_object.items():
            if k in known_date_fields:
                if v is not None:
                    try:
                        if isinstance(v, int):
                            json_object[k] = datetime.datetime.fromtimestamp(v, datetime.timezone.utc)
                        else:
                            json_object[k] = dateutil.parser.parse(v)
                    except:
                        # When we can't parse a date, we just leave the field out
                        mark_delete.append(k)
        # Two step process because otherwise python gets very upset
        for k in mark_delete:
            del json_object[k]
        return json_object

    @staticmethod
    def __json_truefalse_parse(json_object):
        """
        Parse 'True' / 'False' strings in certain known fields
        """
        for key in ('follow', 'favourite', 'reblog', 'mention', 'confirmed', 'suspended', 'silenced', 'disabled', 'approved', 'all_day'):
            if (key in json_object and isinstance(json_object[key], str)):
                if json_object[key].lower() == 'true':
                    json_object[key] = True
                if json_object[key].lower() == 'false':
                    json_object[key] = False
        return json_object

    @staticmethod
    def __json_strnum_to_bignum(json_object):
        """
        Converts json string numerals to native python bignums.
        """
        for key in ('id', 'week', 'in_reply_to_id', 'in_reply_to_account_id', 'logins', 'registrations', 'statuses',
                    'day', 'last_read_id', 'value', 'frequency', 'rate', 'invited_by_account_id', 'count'):
            if (key in json_object and isinstance(json_object[key], str)):
                try:
                    json_object[key] = int(json_object[key])
                except ValueError:
                    pass

        return json_object

    @staticmethod
    def __json_hooks(json_object):
        """
        All the json hooks. Used in request parsing.
        """
        json_object = nativeWebsocketsListener.__json_strnum_to_bignum(json_object)
        json_object = nativeWebsocketsListener.__json_date_parse(json_object)
        json_object = nativeWebsocketsListener.__json_truefalse_parse(json_object)
        json_object = nativeWebsocketsListener.__json_allow_dict_attrs(json_object)
        return json_object

    def __init__(self, endpoint, stats: listenerStats):
        self.endpoint = endpoint
        self.stats = stats
        self.stats.method = "Websockets"
        self.stats.status = 1
        self.htmlparser = HTML2Text()
        self.htmlparser.ignore_links = True
        self.htmlparser.body_width = 0

    async def listen(self):
        print(1)
        while True:
            # Websockets documentation says that .connect(), when used like "async for ws in websockets.connect()"
            # will raise exceptions, allowing you to decide which get retried and which raise further. __aiter__ in
            # Connect() in websockets/client.py says that it does not; It only raises on asyncio.CancelledError. The
            # catchall except block never has any opportunity to raise.
            # This means that every exception other than CancelledError will retry forever.
            try:
                async with websockets.connect(self.endpoint) as ws:
                    await ws.send('{ "type": "subscribe", "stream": "public"}')
                    self.stats.status = 2
                    async for message in ws:
                        envelope = json.loads(message)
                        if envelope["event"] != "update":
                            continue

                        importStatus(json.loads(envelope["payload"], object_hook=nativeWebsocketsListener.__json_hooks),
                                     self.stats,
                                     self.htmlparser)

            except KeyboardInterrupt:
                raise

            except Exception as e:
                print("fuckballs %s" % e)
                raise

            finally:
                self.stats.status = -2



class mpyStreamListener(streaming.StreamListener):
    def __init__(self, stats: listenerStats):
        self.stats = stats
        self.stats.method = "mastodon.py"
        self.stats.status = 1
        self.htmlparser = HTML2Text()
        self.htmlparser.ignore_links = True
        self.htmlparser.body_width = 0


    def on_update(self, s):

        # This is the opt-out mechanism.
        if s['visibility'] != 'public':
            return

        importStatus(s, self.stats, self.htmlparser)

    def handle_heartbeat(self):
        self.stats.status = 2
        self.stats.lastHeartbeatTimestamp = time()

    def on_abort(self, err):
        print("mpy caught error")
        print(err)
        self.stats.status = -2


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

    await db.batchwrite(sqlitevalues, args.db)
    for i in sqlitevalues:
        unsent_statuses.pop(i[0])
    print("Flushed %i statuses, took %s seconds." % (len(sqlitevalues), str(time() - begints)))


async def domainWorker(domain, stats):
    while True:
        try:
            async with httpsession.get('https://%s/api/v1/streaming/health' % domain) as resp:
                # Check if Streaming API is up, and also get redirected if needed.
                streamingBase = ''.join((resp.url.host, resp.url.path)).rstrip('/health')
                break

        except aiohttp.ClientConnectorError:
            # Assume temporary DNS problem, retry
            print("Retrying initial health check...")
            await asyncio.sleep(0.1)
            continue

    listener = nativeWebsocketsListener("wss://%s" % streamingBase, stats)
    try:
        await listener.listen()
    except websockets.InvalidStatusCode:
        print("[!] [%s] Refused websockets connection." % domain)
    finally:
        stats.status = -2

    # Fallback to mastodon.py
    mpyClient = Mastodon(api_base_url=domain, user_agent=args.useragent)
    streamingHandler = mpyClient.stream_public(mpyStreamListener(stats), run_async=True, reconnect_async=True)

    try:
        await shutdownEvent.wait()
        stats.status = -2
    except asyncio.CancelledError:
        streamingHandler.close()
        raise


async def spawnCollectorWorker(domain):
    try:
        wid = max(workers.keys()) + 1
    except ValueError:
        wid = 0
    stats = listenerStats(domain=domain)
    workers[wid] = (asyncio.create_task(domainWorker(domain, stats)), stats)


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
    global httpsession
    global shutdownEvent
    httpsession = aiohttp.ClientSession()
    shutdownEvent = asyncio.Event()

    print("Starting workers for %i domains..." % len(domains))
    for domain in domains:
        await spawnCollectorWorker(domain)

    if not args.nostatus:
        c = Console()
    lastflush = time()
    while True:
        try:
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

        except asyncio.CancelledError:
            print("Shutting down collector loop")
            shutdownEvent.set()
            await httpsession.close()



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
    t.add_column("ID")
    t.add_column("Domain")
    t.add_column("Status")
    t.add_column("Method")
    t.add_column("Last Status")
    t.add_column("Statuses")
    t.add_column("Unique")

    statusMap = {0: "INIT",
                 1: "SETUP",
                 2: "ACTIVE",
                 -1: "RETRY",
                 -2: "FAILED"}

    for k,v in workers.items():
        stats: listenerStats = v[1]
        t.add_row(str(k),
                  stats.domain,
                  statusMap[stats.status],
                  stats.method,
                  datetime.fromtimestamp(stats.lastStatusTimestamp).strftime("%H:%M:%S"),
                  str(stats.receivedStatusCount), str(stats.uniqueStatusCount))

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

        asyncio.run(collectorLoop(domains))

    elif args.mode == "test":
        asyncio.run(testDomains(domains))
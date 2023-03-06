#!/usr/bin/python
import sys
import argparse
import asyncio
import aiohttp
import websockets
import json
import re
import dateutil
from os import makedirs, path
from attrs import define, field
from time import time
from datetime import datetime, timezone
from urllib.parse import urlparse
from html2text import HTML2Text

from mastodon import Mastodon, streaming
from common import db
from common.types import minimalStatus, listenerStats

parser = argparse.ArgumentParser(prog="as:Public Standalone Collector")
parser.add_argument('mode', choices=["run", "test"])
parser.add_argument('--db', default=db.default_dbpath, help="Use/create sqlite3 database at this location")
parser.add_argument('-r', '--recorder', action='append', default=[],
                    help="Push statuses to the recorder at this hostname.")
parser.add_argument('-s', '--server', action='append', default=[],
                    help="Connect to this server. You can call this multiple times.")
parser.add_argument('-l', '--list', action='append', default=[],
                    help="Read servers from this list. It can be a CSV if the first field is the domain.")
parser.add_argument("--exclude", action='append', default=[],
                    help="Do not connect to or process statuses from this server. You can call this multiple times.")
parser.add_argument("--exclude-list", action='append', default=[],
                    help="Read servers to ignore from this list. It can be a CSV if the first field is the domain")
parser.add_argument("--exclude-regex", action='append', default=[],
                    help="Drop statuses mathing this regular expression. You can call this multiple times.")
parser.add_argument("--exclude-regex-list", action='append', default=[],
                    help="Read regular expressions to drop from this list.")
parser.add_argument('--useragent', type=str, help="HTTP User-Agent to present when opening connections",
                    default="Collector/0.1.4")
parser.add_argument('--discover', action="store_true", help="Automatically try to listen to new instances")
parser.add_argument('--debug', action="store_true", help="Enable verbose output useful for debugging")
parser.add_argument('--logdir', default="logs", help="Output debugging logs to this directory")
parser.add_argument('--nolog', action="store_true", help="Do not log to disk")
parser.add_argument('--nostatus', action="store_true", help="Don't show status screen. Saves CPU and some RAM")
parser.add_argument('--nompy', action="store_true", help="Do not use mastodon.py, and only use native Websockets")
proxyOptions = parser.add_mutually_exclusive_group()
proxyOptions.add_argument('--proxy', type=str, help="SOCKS4/5 or HTTP proxy, in uri://user:pass@host:port format")
proxyOptions.add_argument('--tor', action='store', dest='proxy', const="socks5://127.0.0.1:9050", nargs="?",
                          help="Shortcut for --proxy socks5://127.0.0.1:9050")


def importStatus(s, stats: listenerStats, htmlparser: HTML2Text):
    def logwrap(message):
        return "[%s] [%s] %s" % (stats.domain, stats.method, message)

    # This is the opt-out mechanism.
    if s['visibility'] != 'public':
        return False

    if s['reblog']:
        # Mastodo (sic) software appears to send boosts to the public stream.
        return importStatus(s['reblog'], stats, htmlparser)

    stats.lastStatusTimestamp = time()
    stats.receivedStatusCount += 1

    parsedurl = urlparse(s['url'])
    domain = parsedurl.netloc
    url = domain + parsedurl.path

    # This is here and not past the dedupe check because (I'm guessing) it's less CPU heavy to do some string
    # checks than checking for dict membership. This also prevents excluded domains from doing certain things,
    # such as filling up the dedupe cache.
    for i in excluded_domains:
        if domain.endswith(i):
            if args.debug:
                log.debug(logwrap("Rejected status %s (Matched excluded domain %s)" % (s['url'], i)))
            stats.rejectedStatusCount += 1
            return False

    if s['url'] not in dedupe:
        stats.uniqueStatusCount += 1

        parsedText = htmlparser.handle(s['content']).strip()
        if excluded_regex_compiled:
            if excluded_regex_compiled.search(parsedText):
                if args.debug:
                    log.debug(logwrap("Rejected status %s (Matched excluded regex)" % s['url']))
                stats.rejectedStatusCount += 1
                return False

        # This literally seemed to change on a dime after a week of working fine. Adding check.
        if type(s['created_at']) is datetime:
            dt = s['created_at']
        elif type(s['created_at']) is str:
            dt = datetime.fromisoformat(s['created_at'])

        if dt.tzinfo is None:
            log.warning(logwrap("Fixing naive datetime. This should never happen."))
            dt.replace(tzinfo=timezone.utc)

        extract = minimalStatus(url=url,
                                text=parsedText,
                                subject=s['spoiler_text'],
                                created=int(dt.timestamp()),
                                language=s['language'],
                                bot=s['account']['bot'],
                                reply=s['in_reply_to_id'] is not None,
                                attachments=len(s['media_attachments']) != 0)

        unsent_statuses[extract.url] = extract
        dedupe[s['url']] = time()

        if args.discover:
            domain = extract.url.split('/')[0]
            if domain not in discoveredDomains:
                if domain not in excluded_domains:
                    discoveredDomains[domain] = 0

    else:
        # Skip status, update last seen time
        dedupe[s['url']] = time()
        return False


@define
class sink:
    mapstatuses: bool = field(default=False, kw_only=True)
    minFlushFreq: int = field(default=30, kw_only=True)
    maxFlushFreq: int = field(default=300, kw_only=True)
    lastflushed: int = field(default=0, init=False)
    missed: set = field(default=set(), init=False)
    busy: set = field(default=False, init=False)

    async def flush(self, statuses: dict):
        pass

    def flushable(self):
        if self.busy:
            return False
        if self.lastflushed + self.minFlushFreq > time():
            return False
        return True

    def needs_flushed(self):
        return self.lastflushed + self.maxFlushFreq < time()


@define
class sqlitesink(sink):
    dbpath: str
    mapstatuses: bool = field(default=True, kw_only=True)
    minFlushFreq: int = field(default=60, kw_only=True)

    async def flush(self, statuses: tuple):
        begints = time()
        await db.batchwrite(statuses[1], self.dbpath)
        log.info("Flushed %i statuses, took %i ms." % (len(statuses[1]), int((time() - begints) * 1000)))


@define
class recordersink(sink):
    recorderuri: str
    mapstatuses: bool = field(default=True, kw_only=True)

    async def flush(self, statuses: tuple):
        payload = zstd.ZSTD_compress(msgpack.dumps(statuses))
        r = await httpsession.post('%s/api/recorder/checkin' % self.recorderuri, data=payload)


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

    async def listen(self, retries=6, backoff=1, useragent=None, proxy=None):
        retriesLeft = retries
        lastRetry = time()
        lastBackoff = backoff

        wsargs = {"user_agent_header": useragent, "open_timeout": 10, "max_queue": 16}

        if proxy:
            _proxy = Proxy.from_url(proxy)
            host = urlparse(self.endpoint).netloc
            sock = await _proxy.connect(dest_host=host, dest_port=443)

            wsargs.update({"sock": sock, "server_hostname": host})

        while True:
            # Websockets documentation says that .connect(), when used like "async for ws in websockets.connect()"
            # will raise exceptions, allowing you to decide which get retried and which raise further. __aiter__ in
            # Connect() in websockets/client.py says that it does not; It only raises on asyncio.CancelledError. The
            # catchall except block never has any opportunity to raise.
            # This means that every exception other than CancelledError will retry forever.
            try:
                async with websockets.connect(self.endpoint, **wsargs) as ws:
                    await ws.send('{ "type": "subscribe", "stream": "public"}')
                    self.stats.status = 2
                    async for message in ws:
                        envelope = json.loads(message)
                        if envelope["event"] != "update":
                            continue

                        importStatus(json.loads(envelope["payload"], object_hook=nativeWebsocketsListener.__json_hooks),
                                     self.stats,
                                     self.htmlparser)

            except (websockets.ConnectionClosedError, TimeoutError) as e:
                if retriesLeft:
                    # Reset retry counter if the last retry was more than 5 minutes ago.
                    if (lastRetry + 300) < time():
                        retriesLeft = retries
                        lastBackoff = backoff

                    self.stats.status = -1
                    log.debug("[%s] [websockets] Websockets closed: %s; Retrying in %is" % (self.stats.domain, e, lastBackoff))
                    await asyncio.sleep(lastBackoff)

                    retriesLeft -= 1
                    lastRetry = time()
                    lastBackoff = backoff * 2
                    continue

                else:
                    log.info("[%s] [websockets] Websockets closed: %s; Exhausted retries." % (self.stats.domain, e))


class mpyStreamListener(streaming.StreamListener):
    def __init__(self, stats: listenerStats):
        self.stats = stats
        self.stats.method = "mastodon.py"
        self.stats.status = 1
        self.htmlparser = HTML2Text()
        self.htmlparser.ignore_links = True
        self.htmlparser.body_width = 0

    def on_update(self, s):
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


async def flushtosinks():
    async def flush(sinktoflush: sink):
        log.debug("[flushtosinks] Flushing to %s" % type(sinktoflush))
        try:
            if sinktoflush.mapstatuses:
                await sinktoflush.flush(mappedstatuses)
            else:
                await sinktoflush.flush(flushable_statuses.copy())
            sinktoflush.lastflushed = int(time())
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("[flushtosinks] Error %s in sink %s" % (repr(e), repr(sinktoflush)))

    flushable_sinks = [i for i in sinks if i.flushable()]
    # Only push if any sinks have passed their max time between flushes
    if any([i.needs_flushed() for i in flushable_sinks]):
        log.debug("[flushtosinks] Flushing to eligible sinks...")
        flushable_statuses = unsent_statuses.copy()

        # Only generate mapped statuses if any sinks use them
        if any([i.mapstatuses for i in flushable_sinks]):
            # I thought there was a way to just get the values out as a tuple, like with the slots
            # This probably is hilariously wasteful and likely ain't worth the bandwidth gain
            statusmap = minimalStatus.__slots__[:-1]
            mappedstatuses = (statusmap, tuple([tuple([getattr(i, value) for value in statusmap]) for i in flushable_statuses.values()]))

        for i in flushable_sinks:
            asyncio.create_task(flush(i))

        for i in flushable_statuses:
            del(unsent_statuses[i])


def mkmpy(domain):
    if args.proxy:
        customSession = session()
        customSession.proxies.update({'http': args.proxy, 'https': args.proxy})
    else:
        customSession = None
    mpyClient = Mastodon(api_base_url=domain, user_agent=args.useragent, session=customSession)
    return mpyClient


async def domainWorker(domain, stats):
    def logwrap(message):
        return "[%s] [domainWorker] %s" % (domain, message)

    result, streamingBase = await nativeTestDomain(domain, retries=5)
    if not result:
        log.info(logwrap("Failed self-testing. Not connecting."))
        stats.status = -2
        return False

    listener = nativeWebsocketsListener("wss://%s" % streamingBase, stats)
    try:
        await listener.listen(useragent=args.useragent, proxy=args.proxy)
    except asyncio.CancelledError:
        raise
    except websockets.InvalidStatusCode:
        logmessage = "Refused websockets connection."
    except websockets.InvalidURI:
        logmessage = "Redirected to %s but we didn't capture it properly." % streamingBase
    except Exception as e:
        logmessage = "Unhandled exception %s in websockets." % repr(e)

    stats.status = -2
    if args.nompy:
        log.error(logwrap(logmessage + " Exiting."))
        return False
    else:
        log.debug(logwrap(logmessage + " Falling back to mastodon.py"))

    # Fallback to mastodon.py
    try:
        setupThread = asyncio.to_thread(mkmpy, domain)
        mpyClient = await setupThread
        streamingHandler = mpyClient.stream_public(mpyStreamListener(stats), run_async=True, reconnect_async=True)
    except:
        stats.status = -2
        return False

    try:
        await shutdownEvent.wait()
    except asyncio.CancelledError:
        streamingHandler.close()
        raise
    finally:
        stats.status = -2


async def spawnCollectorWorker(domain):
    try:
        wid = max(workers.keys()) + 1
    except ValueError:
        wid = 0
    stats = listenerStats(domain=domain)
    workers[wid] = (asyncio.create_task(domainWorker(domain, stats)), stats)


async def nativeTestDomain(domain, retries: int = 0, backoff: int = 2):
    def logwrap(message):
        return "[%s] [nativeTester] %s" % (domain, message)

    while True:
        try:
            for endpoint in ["/api/v1/streaming/public", "/api/v1/streaming"]:
                log.debug(logwrap("trying %s" % endpoint))
                async with httpsession.get('https://%s%s' % (domain, endpoint), timeout=5) as resp:
                    if resp.url.host != domain:
                        # Retry with new domain
                        log.debug(logwrap("Redirected to %s, restarting test." % resp.url.host))
                        return await nativeTestDomain(resp.url.host)
                    if resp.status >= 500:
                        reason = resp.status
                        continue
                    if resp.status >= 400:
                        reason = resp.status
                        continue

                    # Check if Streaming API is up, and also get redirected if needed.
                    streamingBase = ''.join((resp.url.host, resp.url.path)).rstrip('/public')
                    return True, streamingBase

            else:
                log.debug(logwrap("%s Error. Exhausted endpoints to try. Giving up." % reason))
                return False, None

        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            logmessage = "Timed out during health check."
        except aiohttp.ClientConnectorError as e:
            logmessage = "Generic aiohttp error %s." % repr(e)
        except ProxyError as e:
            log.error(logwrap("Proxy error %s during health check. Exiting." % repr(e)))
            return False, None
        except Exception as e:
            log.warn(logwrap("Unhandled exception %s during health check. Exiting." % repr(e)))
            return False, None

        if retries:
            # Assume temporary problem, retry
            log.debug(logwrap(logmessage + " Retrying in %is") % backoff)
            await asyncio.sleep(backoff)
            backoff = backoff * 2
            retries -= 1
            continue
        log.debug(logwrap(logmessage + " Not retrying."))
        return False, None


async def discoverDomain(domain):
    res = await nativeTestDomain(domain)
    if res[0]:
        log.debug("[%s] [discovery] Passed testing, now listening." % domain)
        discoveredDomains[domain] = 2
        await spawnCollectorWorker(domain)
    else:
        discoveredDomains[domain] = -2


async def collectorLoop(domains):
    global httpsession
    global shutdownEvent
    if args.debug:
        hostdetails = ' '.join([platform.system(), platform.version(), platform.processor()])
    else:
        hostdetails = sys.platform
    await log.debug("%s started on Python %s, %s" % (parser.prog, sys.version, hostdetails))
    await log.debug("Started with config %s" % args)
    await log.debug("%i sinks registered." % len(sinks))

    if args.proxy:
        connector = ProxyConnector.from_url(args.proxy)
    else:
        connector = None
    httpsession = aiohttp.ClientSession(headers={'User-Agent': args.useragent}, connector=connector)
    shutdownEvent = asyncio.Event()

    log.debug("Starting workers for %i domains" % len(domains))
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

            if lastflush + 30 < time():
                if len(unsent_statuses):
                    await flushtosinks()
                lastflush = time()

                log.debug("Pruning dedupe cache")
                ts = int(time())
                for i in [k for k, v in dedupe.items() if v + 600 < ts]:
                    del dedupe[i]

            if args.discover:
                maxkickoffs = 10
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
            log.info("Shutting down collector loop...")
            shutdownEvent.set()
            await httpsession.close()
            await flushtosinks()
            raise


def buildListFromArgs(direct, lists, nocsv=False):
    items = set([i for i in direct])
    for file in lists:
        if nocsv:
            items.update([i.strip() for i in open(file).readlines()])
        else:
            items.update([i.strip().split(',')[0] for i in open(file).readlines()])
    return list(items)


async def testDomains(domains):
    global httpsession
    async with aiohttp.ClientSession() as httpsession:
        workers = [(i, asyncio.create_task(nativeTestDomain(i))) for i in domains]
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
    t.add_column("Dropped")

    statusMap = {0: "INIT",
                 1: "SETUP",
                 2: "ACTIVE",
                 -1: "RETRY",
                 -2: "FAILED"}

    for k,v in workers.items():
        stats: listenerStats = v[1]

        if stats.lastStatusTimestamp:
            lastStatusText = richText(datetime.fromtimestamp(stats.lastStatusTimestamp).strftime("%H:%M:%S"))
            if (stats.lastStatusTimestamp + 300) < time():
                lastStatusText.stylize("red")
            elif (stats.lastStatusTimestamp + 30) < time():
                lastStatusText.stylize("orange")

        else:
            lastStatusText = "-"

        t.add_row(str(k),
                  stats.domain,
                  statusMap[stats.status],
                  stats.method,
                  lastStatusText,
                  str(stats.receivedStatusCount), str(stats.uniqueStatusCount), str(stats.rejectedStatusCount))

    c.print("as:Public Standalone Collector")
    c.print(t)
    c.print("Unwritten statuses: %i" % len(unsent_statuses))
    c.print("Dedupe cache size: %i" % len(dedupe))


def logSetup():
    from aiologger import Logger
    from aiologger.levels import LogLevel
    from aiologger.filters import StdoutFilter
    from aiologger.handlers.files import AsyncFileHandler
    from aiologger.handlers.streams import AsyncStreamHandler
    from aiologger.formatters.base import Formatter
    from sys import stdout, stderr

    logLevel = {True: LogLevel.DEBUG, False: LogLevel.INFO}[args.debug]
    formatter = Formatter("%(asctime)s|%(name)s|%(levelname)s|%(message)s")
    log = Logger(name="collector")
    log.add_handler(AsyncStreamHandler(stream=stdout, level=logLevel, formatter=formatter, filter=StdoutFilter()))
    log.add_handler(AsyncStreamHandler(stream=stderr, level=LogLevel.WARNING, formatter=formatter))
    if not args.nolog:
        makedirs(args.logdir, exist_ok=True)
        fileLoggingHandler = AsyncFileHandler(filename=path.join(args.logdir, 'collector.log'), formatter=formatter)
        fileLoggingHandler.level = LogLevel.DEBUG
        log.add_handler(fileLoggingHandler)

    return log


if __name__ == '__main__':
    args = parser.parse_args()
    if args.debug:
        import platform

    if args.recorder:
        import msgpack
        import zstd

    if args.proxy:
        from python_socks.async_.asyncio import Proxy
        from python_socks._errors import ProxyError
        from aiohttp_socks import ProxyConnector
        from requests import session
    else:
        class ProxyError(Exception):
            pass

    if not args.nostatus:
        from rich.console import Console
        from rich.table import Table
        from rich.text import Text as richText

    domains = buildListFromArgs(args.server, args.list)
    excluded_domains = buildListFromArgs(args.exclude, args.exclude_list)
    excluded_regex = buildListFromArgs(args.exclude_regex, args.exclude_regex_list, nocsv=True)
    if len(excluded_regex):
        excluded_regex_compiled = re.compile('|'.join(["(%s)" % i for i in excluded_regex]))
    else:
        excluded_regex_compiled = False

    if args.mode == "run":
        log = logSetup()
        db.checkdb(args.db)

        sinks = []
        sinks.append(sqlitesink(args.db))
        sinks.extend([recordersink(i) for i in args.recorder])

        # Setup global variables before starting async loop
        workers = {}
        unsent_statuses = {}
        dedupe = {}
        discoveredDomains = {i:1 for i in domains}

        try:
            asyncio.run(collectorLoop(domains))
        except KeyboardInterrupt:
            # Exit a bit more cleanly.
            exit()

    elif args.mode == "test":
        asyncio.run(testDomains(domains), debug=True)
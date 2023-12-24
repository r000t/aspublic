#!/usr/bin/python3
import sys
import argparse
import asyncio
import aiohttp
import websockets
import json
import re
import dateutil
import traceback
from os import makedirs, path
from attrs import define, field
from time import time
from datetime import datetime, timezone
from urllib.parse import urlparse
from html2text import HTML2Text

from mastodon import Mastodon, streaming
from common import db_sqlite
from common.ap_types import minimalStatus, listenerStats


class dbDefaultAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if values is None:
            setattr(namespace, self.dest, db_sqlite.default_dbpath)
        else:
            setattr(namespace, self.dest, values)


parser = argparse.ArgumentParser(prog="as:Public Standalone Collector")
parser.add_argument('mode', choices=["run", "test"])
parser.add_argument('--db', nargs='?', action=dbDefaultAction, help="Use/create sqlite3 database at this location")
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
                    default="Collector/0.1.7")
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


def importStatus(s: dict, stats: listenerStats, htmlparser: HTML2Text):
    '''
    Processes a status from the Mastodon API. Deduplication, content filtering, and opt-out happen here.

    Returns True if the status was unique and accepted for collection. Returns False if the status was
    rejected for any reason, or if it has been seen before. May return the created status instead of True
    in the future. The big motivation behind the return value for this is allowing the caller to take
    specific actions (like forwarding the status to a mirror) only if the status is new and not filtered.
    '''
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
    '''
    A Sink periodically receives batches of statuses. More than one Sink can be registered to a running Collector.

    How often Sinks are processed (flushed) is controlled by `minFlushFreq` and `maxFlushFreq`. The main loop will
    periodically check if *any* sink is due for a flush based on its `maxFlushFreq`, and push all unsent statuses
    to *every* sink.

    The Sink will not be flushed less than `minFlushFreq`. In this case, the statuses that would have been flushed
    are set aside. The same thing happens if the Sink is busy at the time that statuses are pushed to it.
    '''
    mapstatuses: bool = field(default=False, kw_only=True)
    minFlushFreq: int = field(default=30, kw_only=True)
    maxFlushFreq: int = field(default=60, kw_only=True)
    lastflushed: int = field(default=0, init=False)
    missed: set = field(default=set(), init=False)
    busy: bool = field(default=False, init=False)

    async def flush(self, statuses: tuple, *args, **kwargs):
        '''
        Common code for handling a batch of statuses from the main loop. Queues statuses for later if the Sink
        has been flushed too recently, or if it is still processing the last batch.

        This is an internal function. Subclasses should override `_flush()`
        '''
        if self.busy:
            if self.mapstatuses:
                if "statusmap" not in kwargs:
                    raise AttributeError("statusmap is required for a sink with mapstatuses=True")
            self.missed.update(statuses)
            return

        try:
            self.busy = True

            if self.missed:
                push_statuses = tuple(set(statuses) | self.missed)
            else:
                push_statuses = statuses

            statuses_flushed = await self._flush(push_statuses, *args, **kwargs)

            if statuses_flushed:
                if self.missed:
                    self.missed -= set(statuses_flushed)
            if statuses_flushed != push_statuses:
                self.missed.update(set(statuses) - set(statuses_flushed))
            log.debug("Sink %s flush finished. Sink backlog size is %i" % (type(self), len(self.missed)))

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.debug("%s flush failed due to %s." % (type(self), repr(e)))
        finally:
            # Assume push failed, add statuses to backlog
            self.missed.update(statuses)
            self.busy = False

    async def _flush(self, statuses: tuple, *args, **kwargs):
        '''
        An empty function designed to be overriden by subclasses. This is where your code for actually doing
        somthing with the batch of statuses (processing, storing) would go.

        This function will be run in the main event loop. CPU-heavy processing should be sent to a separate thread.

        You must return a set of statuses that have been processed/stored/etc and can be removed from this Sink's
        queue. Alternately, you can pass the existing `statuses` list to just mark them all to be cleared.
        '''
        return ()

    def flushable(self):
        '''
        Returns True if this Sink is eligible to be flushed to. Both of the following must be true:

        - `minFlushFreq` seconds have passed since the last time this Sink was flushed
        - This Sink is not busy (it has finished processing the previous batch of statuses
        '''
        if self.busy:
            return False
        if self.lastflushed + self.minFlushFreq > time():
            return False
        return True

    def needs_flushed(self):
        '''
        Returns True if the Sink needs to be flushed to, meaning more than `maxFlushFreq` seconds have passed
        since the last time.
        '''
        return self.lastflushed + self.maxFlushFreq < time()


@define
class sqlitesink(sink):
    '''
    A Sink for saving statuses to an sqlite3 database on disk.

    - `dbpath` (str): The path to an existing sqlite3 database
    '''
    dbpath: str
    mapstatuses: bool = field(default=True, kw_only=True)
    minFlushFreq: int = field(default=60, kw_only=True)

    async def _flush(self, statuses: tuple, *args, **kwargs):
        begints = time()
        await db_sqlite.batchwrite(statuses[1], self.dbpath)
        log.info("Flushed %i statuses, took %i ms." % (len(statuses[1]), int((time() - begints) * 1000)))
        return statuses


@define
class recordersink(sink):
    '''
    A Sink for sending collected statuses to a Recorder.

    - `recorderuri` (str): The complete URI to a running Recorder (Example: http://10.0.0.10/ or http://hostname.local/)
    '''
    recorderuri: str
    mapstatuses: bool = field(default=True, kw_only=True)

    async def _flush(self, statuses: tuple, *args, **kwargs):
        begints = time()
        payload = zstd.ZSTD_compress(msgpack.dumps((kwargs["statusmap"], statuses)))
        compressts = time()
        r = await httpsession.post('%s/api/recorder/checkin' % self.recorderuri, data=payload)
        networkts = time()
        log.info("Flushed %i statuses to %s. Compression took %i ms, push took %i ms" % (len(statuses),
                                                                                         self.recorderuri,
                                                                                         begints-compressts,
                                                                                         begints-networkts))
        return statuses


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

                    # Subscribe to public (federated) firehose
                    await ws.send('{ "type": "subscribe", "stream": "public"}')

                    # Mark worker as active for status display
                    self.stats.status = 2

                    # Endlessly listen for and import statuses received from server, ignoring other messages
                    async for message in ws:
                        envelope = json.loads(message)
                        if envelope["event"] != "update":
                            continue

                        importStatus(json.loads(envelope["payload"], object_hook=nativeWebsocketsListener.__json_hooks),
                                     self.stats,
                                     self.htmlparser)

            except (websockets.ConnectionClosedError, TimeoutError) as e:
                # Exponential backoff, up to a maximum number of retries
                if retriesLeft:
                    # Reset retry counter if the last retry was more than 5 minutes ago.
                    if (lastRetry + 300) < time():
                        retriesLeft = retries
                        lastBackoff = backoff

                    # Mark worker as "retrying" for status display, then sleep
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
    '''
    Mastodon.py stream listener that imports statuses it receives, and updates its worker status/stats
    when the server sends a keepalive, or upon failure.
    '''
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
    '''
    Flushes all statuses collected so far to all registered Sinks, if any are due to be pushed to.

    The status cache is not cleared. Instead, a copy is made and pushed out to Sinks. Then, each status in the copy
    is individually deleted from the status cache.
    '''
    async def flush(sinktoflush: sink):
        log.debug("[flushtosinks] Flushing to %s" % type(sinktoflush))
        try:
            if sinktoflush.mapstatuses:
                await sinktoflush.flush(statuses=mappedstatuses, statusmap=statusmap)
            else:
                await sinktoflush.flush(statuses=flushable_statuses)
            sinktoflush.lastflushed = int(time())
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("[flushtosinks] Error %s in sink %s" % (repr(e), repr(sinktoflush)))

    # Sinks have a "minimum time" between flushes, and won't be pushed to if this time hasn't elapsed
    # TODO: Flush to them anyway and handle skipping too-frequent pushes at the Sink
    # Current logic causes these sinks to miss statuses entirely because statuses are deleted afterwards
    flushable_sinks = [i for i in sinks if i.flushable()]

    # Only push if any sinks have passed their max time between flushes
    if any([i.needs_flushed() for i in flushable_sinks]):
        log.debug("[flushtosinks] Flushing to eligible sinks...")
        flushable_statuses = tuple(unsent_statuses.values())

        # Only generate mapped statuses if any sinks use them
        if any([i.mapstatuses for i in flushable_sinks]):
            # I thought there was a way to just get the values out as a tuple, like with the slots
            # This probably is hilariously wasteful and likely ain't worth the bandwidth gain
            statusmap = minimalStatus.__slots__[:-1]
            mappedstatuses = tuple([tuple([getattr(i, value) for value in statusmap]) for i in flushable_statuses])

        for i in flushable_sinks:
            asyncio.create_task(flush(i))

        for i in flushable_statuses:
            del(unsent_statuses[i.url])


def mkmpy(domain, **kwargs):
    '''
    Creates a Mastodon.py client object, with proxies and custom user-agent, if configured. This is a separate
    function so that it can be passed into asyncio.to_thread(), as Mastodon.py will make (blocking) web requests
    during instantiation.
    '''
    if args.proxy:
        customSession = session()
        customSession.proxies.update({'http': args.proxy, 'https': args.proxy})
    else:
        customSession = None
    mpyClient = Mastodon(api_base_url=domain, user_agent=args.useragent, session=customSession, **kwargs)
    return mpyClient


async def domainWorker(domain: str, stats: listenerStats, bearer_token: str = None):
    '''
    The actual domain worker loop. First uses nativeTestDomain() to try and discover the streaming API endpoint.
    If this initial test fails, the worker exits. If discovery succeeds, it will first try connecting through
    Websockets, falling back to normal http streaming via Mastodon.py if this fails.
    '''
    def logwrap(message):
        '''Decorates log messages with this worker's domain and type'''
        return "[%s] [domainWorker] %s" % (domain, message)

    # Try to discover streaming API endpoint
    result, streamingBase = await nativeTestDomain(domain, bearer_token=bearer_token, retries=5)
    if not result:
        log.info(logwrap("Failed self-testing. Not connecting."))
        stats.status = -2
        return False

    # First, try websockets
    if bearer_token:
        websocketuri = f"{streamingBase.rstrip('/')}?access_token={bearer_token}"
    else:
        websocketuri = streamingBase

    listener = nativeWebsocketsListener("wss://%s" % websocketuri, stats)

    try:
        # Listens forever, if successful.
        await listener.listen(useragent=args.useragent, proxy=args.proxy)
    except asyncio.CancelledError:
        # Properly handle application exit
        raise
    except websockets.InvalidStatusCode as e:
        if e.status_code == 401:
            if bearer_token:
                logmessage = "Refused access token."
            else:
                logmessage = "An access token is required."
        else:
            logmessage = "Refused websockets connection."
    except websockets.InvalidURI:
        logmessage = "Redirected to %s but we didn't capture it properly." % streamingBase
    except Exception as e:
        logmessage = "Unhandled exception %s in websockets." % repr(e)

    # Mark worker as failed for status display. Exit if not configured to fallback to mastodon.py
    stats.status = -2
    if args.nompy:
        log.error(logwrap(logmessage + " Exiting."))
        return False
    else:
        log.debug(logwrap(logmessage + " Falling back to mastodon.py"))

    # Fallback to mastodon.py, which isn't asynchronous and must run in a separate thread
    try:
        setupThread = asyncio.to_thread(mkmpy, domain, access_token=bearer_token)
        mpyClient = await setupThread
        streamingHandler = mpyClient.stream_public(mpyStreamListener(stats), run_async=True, reconnect_async=True)
    except:
        # Mark worker as failed for status display, and exit.
        stats.status = -2
        return False

    # Properly handle application shutdown
    try:
        await shutdownEvent.wait()
    except asyncio.CancelledError:
        streamingHandler.close()
        raise
    finally:
        stats.status = -2


async def spawnCollectorWorker(domain: str, bearer_token: str = None):
    '''
    Spawns a domain worker. Assigns it an ID and adds it to the dict of workers for management and status display.
    '''
    try:
        wid = max(workers.keys()) + 1
    except ValueError:
        wid = 0

    stats = listenerStats(domain=domain)
    workers[wid] = (asyncio.create_task(domainWorker(domain, stats, bearer_token=bearer_token)), stats)


async def nativeTestDomain(domain, bearer_token: str = None, retries: int = 0, backoff: int = 2):
    '''
    Tries to discover the Streaming API endpoint, and whether authentication is needed. Redirections to other
    domains (streaming.example.com) and endpoints are ideally found here. This is also a separate function so
    that it is not necessary to spawn an entire worker just to see if a domain supports public streaming.

    Returns (True, streaming_endpoint_url) if it finds a valid streaming endpoint, (False, None) if it cannot.
    '''

    def logwrap(message):
        # Decorates the log entry with the current domain and the name of this function
        return "[%s] [nativeTester] %s" % (domain, message)

    while True:
        try:
            for endpoint in ["/api/v1/streaming/public", "/api/v1/streaming"]:
                log.debug(logwrap("trying %s" % endpoint))

                if bearer_token:
                    log.debug(logwrap(f"Using bearer token {bearer_token}"))
                    headers = {"Authorization": f"Bearer {bearer_token}"}
                else:
                    headers = {}

                async with httpsession.get('https://%s%s' % (domain, endpoint), timeout=5, headers=headers) as resp:
                    if resp.url.host != domain:
                        # Redirected to a new domain. This typically means the Streaming API is hosted
                        # on a different subdomain (streaming.example.com). The test will be rerun with
                        # the new domain, and its result will be passed upwards.
                        log.debug(logwrap("Redirected to %s, restarting test with new domain." % resp.url.host))
                        return await nativeTestDomain(resp.url.host, bearer_token=bearer_token, retries=retries, backoff=backoff)



                    # For an error status, move on to the next candidate endpoint
                    if resp.status >= 500:
                        reason = resp.status
                        text = await resp.text()
                        log.debug(logwrap("%s Error (%s). Moving on." % (reason, text)))
                        continue
                    if resp.status >= 400:
                        reason = resp.status
                        text = await resp.text()
                        if "missing access token" in text.lower():
                            log.debug(logwrap("Streaming API requires authorization. Giving up."))
                            return False, None
                        else:
                            log.debug(logwrap("%s Error (%s). Moving on." % (reason, text)))
                        continue

                    # Rewrites the endpoint path with the actual host and path that were returned by the httpd, after
                    # any redirects. Anything using this endpoint should not be redirected any further.
                    streamingBase = ''.join((resp.url.host, resp.url.path)).rstrip('/public')
                    return True, streamingBase

            else:
                log.debug(logwrap("%s Error. Exhausted endpoints to try. Giving up." % reason))
                return False, None


        # Properly handle application exit, separate types of failures for better logging
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


        # If configured to retry, apply an exponential backoff. Otherwise, give up.
        if retries:
            log.debug(logwrap(logmessage + " Retrying in %is") % backoff)
            await asyncio.sleep(backoff)
            backoff = backoff * 2
            retries -= 1
            continue
        else:
            log.debug(logwrap(logmessage + " Not retrying."))
            return False, None


async def discoverDomain(domain):
    '''
    Task that the main loop can run to see if a domain provides public streaming. If it does, a new worker
    is created to connect to it.
    '''
    res = await nativeTestDomain(domain)
    if res[0]:
        log.debug("[%s] [discovery] Passed testing, now listening." % domain)
        discoveredDomains[domain] = 2
        await spawnCollectorWorker(domain)
    else:
        discoveredDomains[domain] = -2


async def collectorLoop(domains: dict):
    '''
    The as:Public Collector. Spawns workers, then periodically performs housekeeping functions, such as flushing
    collected statuses to registered Sinks, testing newly-discovered domains (if enabled), and updating the on-screen
    list of workers, their domains, and their statistics.
    '''
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
    for domain, bearer_token in domains.items():
        await spawnCollectorWorker(domain, bearer_token=bearer_token)

    if not args.nostatus:
        c = Console()
    lastflush = time()
    while True:
        try:
            if not args.nostatus:
                c.clear()
                drawStatusScreen(c)

            if lastflush + 15 < time():
                if len(unsent_statuses):
                    await flushtosinks()
                lastflush = time()

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
            # Properly handle application shutdown. Performs a final flush to registered Sinks and raises.
            log.info("Shutting down collector loop...")
            shutdownEvent.set()
            await httpsession.close()
            await flushtosinks()
            raise

        except Exception as e:
            # Unhandled exception in main loop. Write out relevant information to a file and try to exit cleanly.
            print("(Crash.) In a wrinkle of steel we are gone.")
            tbpath = "error-collector-%s.log" % datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
            traceback.print_tb(e.__traceback__, file=open(tbpath, "w"))
            if args.debug:
                traceback.print_tb(e.__traceback__)
            else:
                print("A log of this error (Type %s) is at %s" % (repr(e), tbpath))
            exit()


def buildListFromArgs(direct, lists, nocsv=False):
    # Combines and deduplicates lists of domains from command-line arguments and files
    items = set(direct)
    for file in lists:
        if nocsv:
            items.update([i.strip() for i in open(file).readlines()])
        else:
            items.update([i.strip().split(',')[0] for i in open(file).readlines()])
    items.discard('')
    return list(items)


def buildListFromArgsWithAccessTokens(direct, lists, nocsv=False):
    # Combines and deduplicates lists of domains from command-line arguments and files
    items = {d: None for d in direct}
    separator = '|'
    for file in lists:
        with open(file, 'r') as f:
            for line in f.readlines():
                parts = line.strip().split(separator)
                domain = parts[0]
                token = parts[1] if len(parts) > 1 else None  # Get token if available
                items[domain] = token  # Add or update the domain-token pair

    return items



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


def drawStatusScreen(c):
    statusMap = {0: "INIT",
                 1: "SETUP",
                 2: "ACTIVE",
                 -1: "RETRY",
                 -2: "FAILED"}

    # Create worker status table
    t = Table()
    t.add_column("ID")
    t.add_column("Domain")
    t.add_column("Status")
    t.add_column("Method")
    t.add_column("Last Status")
    t.add_column("Statuses")
    t.add_column("Unique")
    t.add_column("Dropped")

    # Add row for each worker
    for k,v in workers.items():
        stats: listenerStats = v[1]

        # Update "Last Status" timestamp, and highlight servers we haven't gotten statuses from in a while.
        if stats.lastStatusTimestamp:
            lastStatusText = richText(datetime.fromtimestamp(stats.lastStatusTimestamp).strftime("%H:%M:%S"))

            if (stats.lastStatusTimestamp + 300) < time():
                lastStatusText.stylize("red")
            elif (stats.lastStatusTimestamp + 30) < time():
                lastStatusText.stylize("orange")
        else:
            # We have yet to receive any statuses from this server
            lastStatusText = "-"

        # Actually assemble and add row to table
        t.add_row(str(k),
                  stats.domain,
                  statusMap[stats.status],
                  stats.method,
                  lastStatusText,
                  str(stats.receivedStatusCount), str(stats.uniqueStatusCount), str(stats.rejectedStatusCount))

    # Draw a title, the worker status table, and some global stats
    c.print("as:Public Standalone Collector")
    c.print(t)
    c.print("Unwritten statuses: %i" % len(unsent_statuses))
    c.print("Dedupe cache size: %i" % len(dedupe))


def logSetup():
    '''
    Internal logger setup. Logging to file is also set up here, if enabled. Imports are done here to avoid
    cluttering the main namespace. This runs before the actual async loop is started, so that logging is
    available throughout its entire lifecycle.

    Returns the created Logger object.
    '''
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

    # Set up logging to file, if enabled.
    if not args.nolog:
        makedirs(args.logdir, exist_ok=True)
        fileLoggingHandler = AsyncFileHandler(filename=path.join(args.logdir, 'collector.log'), formatter=formatter)
        fileLoggingHandler.level = LogLevel.DEBUG
        log.add_handler(fileLoggingHandler)

    return log


if __name__ == '__main__':
    '''
    Parses arguments, imports optional packages if they will be needed, sets up global variables, and then
    starts the actual async main loop. 
    '''
    args = parser.parse_args()

    # Imports only needed for debugging
    if args.debug:
        import platform

    # Imports only needed for flushing to a Recorder
    if args.recorder:
        import msgpack
        import zstd

    # Imports only needed if using an http/socks proxy
    if args.proxy:
        from python_socks.async_.asyncio import Proxy
        from python_socks._errors import ProxyError
        from aiohttp_socks import ProxyConnector
        from requests import session
    else:
        # If not using a proxy, create a dummy ProxyError so Try/Except blocks don't break
        class ProxyError(Exception):
            pass

    # Imports only needed for the status screen (which is on by default)
    if not args.nostatus:
        from rich.console import Console
        from rich.table import Table
        from rich.text import Text as richText

    # buildListFromArgs() combines and deduplicates entries from command-line arguments and on-disk lists.
    domains: dict = buildListFromArgsWithAccessTokens(args.server, args.list)
    excluded_domains: list = buildListFromArgs(args.exclude, args.exclude_list)
    excluded_regex: list = buildListFromArgs(args.exclude_regex, args.exclude_regex_list, nocsv=True)

    # Pre-compile all regular expressions
    if len(excluded_regex):
        excluded_regex_compiled = re.compile('|'.join(["(%s)" % i for i in excluded_regex]))
    else:
        excluded_regex_compiled = False

    if args.mode == "run":
        # Create logging facility that will be passed into app
        log = logSetup()

        sinks = []

        # Register sqlite3 Sink
        if args.db is not None:
            db_sqlite.checkdb(args.db)
            sinks.append(sqlitesink(args.db))

        # Instantiate and register Recorder Sinks.
        sinks.extend([recordersink(i) for i in args.recorder])

        # Setup global variables before starting async loop
        workers = {}
        unsent_statuses = {}
        dedupe = {}
        discoveredDomains = {i:1 for i in domains}

        # Actually start the Collector.
        try:
            asyncio.run(collectorLoop(domains))
        except KeyboardInterrupt:
            # Exit a bit more cleanly.
            exit()

    elif args.mode == "test":
        asyncio.run(testDomains(domains), debug=True)
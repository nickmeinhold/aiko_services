# To Do
# ~~~~~
# - Support request headers / bearer-token auth via PipelineElement parameters
# - create_targets(): HTTP upload (POST/PUT), once a matching write
#   PipelineElement exists (currently source-only, fails closed)

import ipaddress
import socket
import time
from urllib.parse import urlsplit, urlunsplit

import requests
from requests.adapters import HTTPAdapter

import aiko_services as aiko

__all__ = ["DataSchemeHTTP"]

_LOGGER = aiko.process.logger(__name__)

_TIMEOUT_SECONDS = 30                    # total wall-clock deadline per fetch
_MAX_REDIRECTS = 5
_MAX_CONTENT_BYTES = 64 * 1024 * 1024    # 64 MiB cap on DECODED response bytes
_CHUNK_BYTES = 65536

# --------------------------------------------------------------------------- #
# parameter: "data_sources" provides one or more HTTP(S) URLs.  Each URL is
#   fetched with an HTTP GET and its response body becomes a single frame
#   "record" (raw bytes), mirroring the "zmq" DataScheme's network byte records.
# - "(http://hostname/path)"
# - "(https://hostname/path_0 https://hostname/path_1 ...)"
#
# parameter: "data_targets" -- HTTP upload (POST) is not yet implemented; the
#   "http" DataScheme is source-only and fails closed when used as a target.
#
# SSRF: by default the host of the initial URL AND of every redirect hop is
#   resolved and rejected unless EVERY resolved address is globally routable
#   (blocks loopback, RFC1918, link-local incl. 169.254.169.254 metadata,
#   CGNAT, IPv4-mapped, etc via `not ip.is_global`).  The connection is then
#   PINNED to a validated IP so the name cannot re-resolve to an internal
#   address between check and connect (DNS-rebinding TOCTOU); the original
#   hostname is preserved for the Host header and, for HTTPS, TLS SNI + cert
#   verification.  Redirects are followed manually (allow_redirects=False) so
#   each hop is validated before it is contacted, an https origin cannot be
#   downgraded to http, and env proxies are ignored (trust_env=False).  Set the
#   PipelineElement parameter "allow_private_addresses" true to bypass all of
#   this for a trusted local endpoint (e.g. a dev server) -- default false.


def _to_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value, default):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _resolve_public_ip(host):
    # Resolve `host` and return (ip, None) only if EVERY resolved address is
    # globally routable; otherwise (None, diagnostic).  Fail closed on a
    # resolution error.  Returning a single vetted IP lets the caller pin the
    # connection to it so a second resolution can't rebind to an internal host.
    if not host:
        return None, "missing host"
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror as gai_error:
        return None, f'cannot resolve "{host}": {gai_error}'
    ip = None
    for info in infos:
        try:
            candidate = ipaddress.ip_address(info[4][0])
        except ValueError:
            return None, f'unparseable address for "{host}"'
        if not candidate.is_global:
            return None, f'non-public address {candidate} for "{host}"'
        if ip is None:
            ip = str(candidate)
    if ip is None:
        return None, f'no address for "{host}"'
    return ip, None


class _PinnedSNIAdapter(HTTPAdapter):
    # HTTPS to a pinned IP: keep TLS SNI + certificate verification bound to the
    # ORIGINAL hostname, not the IP we actually dial.
    def __init__(self, server_hostname, **kwargs):
        self._server_hostname = server_hostname
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **kwargs):
        kwargs["assert_hostname"] = self._server_hostname
        kwargs["server_hostname"] = self._server_hostname
        super().init_poolmanager(connections, maxsize, block=block, **kwargs)


def _host_port_header(parts):
    host = parts.hostname or ""
    if ":" in host:                       # IPv6 literal
        host = f"[{host}]"
    return f"{host}:{parts.port}" if parts.port else host


def _pin_url_to_ip(parts, ip):
    netloc = f"[{ip}]" if ":" in ip else ip
    if parts.port:
        netloc = f"{netloc}:{parts.port}"
    return urlunsplit(
        (parts.scheme, netloc, parts.path or "/", parts.query, ""))


class DataSchemeHTTP(aiko.DataScheme):
    def create_sources(self,
        stream, data_sources, frame_generator=None, use_create_frame=True):

        if not frame_generator:
            frame_generator = self.frame_generator

        urls = []
        for data_source in data_sources:
            scheme = aiko.DataScheme.parse_url_scheme(data_source)
            if scheme not in ("http", "https"):
                diagnostic =  \
                    f'HTTP DataScheme requires an http(s) URL, not "{scheme}"'
                return aiko.StreamEvent.ERROR, {"diagnostic": diagnostic}
            urls.append(data_source)

        stream.variables["source_urls_generator"] = iter(urls)
        rate = _to_float(
            self.pipeline_element.get_parameter("rate", default=None)[0], None)
        self.pipeline_element.create_frames(stream, frame_generator, rate=rate)
        return aiko.StreamEvent.OKAY, {}

    def create_targets(self, stream, data_targets):
        diagnostic = "HTTP DataScheme is source-only; " \
            "HTTP upload (POST) target is not yet implemented"
        return aiko.StreamEvent.ERROR, {"diagnostic": diagnostic}

    def frame_generator(self, stream, frame_id):
        data_batch_size = _to_int(
            self.pipeline_element.get_parameter("data_batch_size", default=1)[0],
            1)
        allow_private = self.pipeline_element.get_parameter(
            "allow_private_addresses", default=False)[0]

        records = []
        try:
            while (data_batch_size > 0):
                data_batch_size -= 1
                url = next(stream.variables["source_urls_generator"])
                stream_event, result = self._fetch(url, allow_private)
                if stream_event != aiko.StreamEvent.OKAY:
                    return stream_event, result
                records.append(result["record"])
        except StopIteration:
            pass

        if records:
            return aiko.StreamEvent.OKAY, {"records": records}
        else:
            return aiko.StreamEvent.STOP,  \
                {"diagnostic": "All frames generated"}

    def _fetch(self, url, allow_private=False):
        # One total deadline spans connect + all redirect hops + body streaming,
        # so a slow-drip server can't hold the frame thread past _TIMEOUT_SECONDS.
        deadline = time.monotonic() + _TIMEOUT_SECONDS
        origin_scheme = aiko.DataScheme.parse_url_scheme(url)
        current = url
        session = requests.Session()
        session.trust_env = False         # ignore HTTP(S)_PROXY / NO_PROXY env
        try:
            for _hop in range(_MAX_REDIRECTS + 1):
                parts = urlsplit(current)
                scheme = (parts.scheme or "").lower()
                if scheme not in ("http", "https"):
                    return aiko.StreamEvent.ERROR, {"diagnostic":
                        f'HTTP redirect to non-http(s) URL "{current}"'}
                if origin_scheme == "https" and scheme == "http":
                    return aiko.StreamEvent.ERROR, {"diagnostic":
                        f'HTTP refused https->http downgrade to "{current}"'}
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return aiko.StreamEvent.ERROR,  \
                        {"diagnostic": "HTTP request exceeded deadline"}

                request_url = current
                headers = {"Accept-Encoding": "identity"}
                if not allow_private:
                    ip, diagnostic = _resolve_public_ip(parts.hostname)
                    if ip is None:
                        return aiko.StreamEvent.ERROR,  \
                            {"diagnostic": f"HTTP blocked {diagnostic}"}
                    # Pin the connection to the validated IP; keep the hostname
                    # for routing (Host) and TLS (SNI + cert) so a re-resolve
                    # can't rebind to an internal address.
                    request_url = _pin_url_to_ip(parts, ip)
                    headers["Host"] = _host_port_header(parts)
                    if scheme == "https":
                        session.mount("https://", _PinnedSNIAdapter(
                            parts.hostname))

                # allow_redirects=False: follow manually so each hop is
                # validated BEFORE it is contacted.
                response = session.get(request_url, timeout=remaining,
                    stream=True, allow_redirects=False, headers=headers)
                try:
                    if response.is_redirect or response.is_permanent_redirect:
                        location = response.headers.get("Location")
                        if not location:
                            return aiko.StreamEvent.ERROR,  \
                                {"diagnostic": "HTTP redirect without Location"}
                        # Resolve relative Location against the logical URL
                        # (current), not the IP-pinned one.
                        current = requests.compat.urljoin(current, location)
                        continue

                    response.raise_for_status()
                    # NOTE: iter_content decodes Content-Encoding, so the cap is
                    # on DECODED bytes -- memory is bounded at _MAX_CONTENT_BYTES
                    # even for a compressed "bomb"; wire bytes may be smaller.
                    body = bytearray()
                    for chunk in response.iter_content(chunk_size=_CHUNK_BYTES):
                        if time.monotonic() > deadline:
                            return aiko.StreamEvent.ERROR,  \
                                {"diagnostic": "HTTP response exceeded deadline"}
                        body.extend(chunk)
                        if len(body) > _MAX_CONTENT_BYTES:
                            return aiko.StreamEvent.ERROR, {"diagnostic":
                                f"HTTP response exceeds "
                                f"{_MAX_CONTENT_BYTES} byte cap"}
                    _LOGGER.debug(f"_fetch(): {url} -> {len(body)} bytes")
                    return aiko.StreamEvent.OKAY, {"record": bytes(body)}
                finally:
                    response.close()

            return aiko.StreamEvent.ERROR,  \
                {"diagnostic": f"HTTP exceeded {_MAX_REDIRECTS} redirects"}
        except requests.RequestException as request_error:
            return aiko.StreamEvent.ERROR,  \
                {"diagnostic": f'HTTP GET "{url}" failed: {request_error}'}
        finally:
            session.close()


aiko.DataScheme.add_data_scheme("http", DataSchemeHTTP)
aiko.DataScheme.add_data_scheme("https", DataSchemeHTTP)

# --------------------------------------------------------------------------- #

# To Do
# ~~~~~
# - Support request headers / bearer-token auth via PipelineElement parameters
# - create_targets(): HTTP upload (POST/PUT), once a matching write
#   PipelineElement exists (currently source-only, fails closed)

import ipaddress
import socket
import time
from urllib.parse import urljoin, urlsplit

import requests

import aiko_services as aiko

__all__ = ["DataSchemeHTTP"]

_LOGGER = aiko.process.logger(__name__)

_TIMEOUT_SECONDS = 30                    # total wall-clock deadline per fetch
_MAX_REDIRECTS = 5
_MAX_CONTENT_BYTES = 64 * 1024 * 1024    # 64 MiB response cap (avoid OOM)
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
# SSRF: by default the initial URL AND every redirect hop are resolved and
#   rejected if they land on a loopback / link-local / private / reserved /
#   multicast address (blocks 127.0.0.1, ::1, RFC1918, and the 169.254.169.254
#   cloud-metadata endpoint).  Redirects are followed manually (not by requests)
#   so each hop is validated BEFORE it is contacted.  Set the PipelineElement
#   parameter "allow_private_addresses" true to permit them (e.g. a local dev
#   server) -- default false.


def _host_is_blocked(host):
    # Resolve host and block if ANY resolved address is non-public. Fail closed
    # on a resolution error (an unresolvable host is not fetchable anyway).
    if not host:
        return True
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror:
        return True
    for info in infos:
        try:
            ip = ipaddress.ip_address(info[4][0])
        except ValueError:
            return True
        if (ip.is_loopback or ip.is_link_local or ip.is_private
                or ip.is_reserved or ip.is_multicast or ip.is_unspecified):
            return True
    return False


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
        rate, _ = self.pipeline_element.get_parameter("rate", default=None)
        rate = float(rate) if rate else None
        self.pipeline_element.create_frames(stream, frame_generator, rate=rate)
        return aiko.StreamEvent.OKAY, {}

    def create_targets(self, stream, data_targets):
        diagnostic = "HTTP DataScheme is source-only; " \
            "HTTP upload (POST) target is not yet implemented"
        return aiko.StreamEvent.ERROR, {"diagnostic": diagnostic}

    def frame_generator(self, stream, frame_id):
        data_batch_size, _ = self.pipeline_element.get_parameter(
            "data_batch_size", default=1)
        data_batch_size = int(data_batch_size)
        allow_private, _ = self.pipeline_element.get_parameter(
            "allow_private_addresses", default=False)

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
        current = url
        try:
            for _hop in range(_MAX_REDIRECTS + 1):
                scheme = aiko.DataScheme.parse_url_scheme(current)
                if scheme not in ("http", "https"):
                    return aiko.StreamEvent.ERROR, {"diagnostic":
                        f'HTTP redirect to non-http(s) URL "{current}"'}
                if not allow_private:
                    host = urlsplit(current).hostname
                    if _host_is_blocked(host):
                        return aiko.StreamEvent.ERROR, {"diagnostic":
                            f'HTTP blocked non-public host "{host}"'}
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return aiko.StreamEvent.ERROR,  \
                        {"diagnostic": "HTTP request exceeded deadline"}

                # allow_redirects=False: follow manually so each hop is validated
                # BEFORE it is contacted. Accept-Encoding: identity so the size
                # cap measures wire bytes (no decompression-bomb inflation).
                response = requests.get(current, timeout=remaining, stream=True,
                    allow_redirects=False,
                    headers={"Accept-Encoding": "identity"})
                if response.is_redirect or response.is_permanent_redirect:
                    location = response.headers.get("Location")
                    response.close()
                    if not location:
                        return aiko.StreamEvent.ERROR,  \
                            {"diagnostic": "HTTP redirect without Location"}
                    current = urljoin(current, location)
                    continue

                response.raise_for_status()
                body = bytearray()
                for chunk in response.iter_content(chunk_size=_CHUNK_BYTES):
                    if time.monotonic() > deadline:
                        response.close()
                        return aiko.StreamEvent.ERROR,  \
                            {"diagnostic": "HTTP response exceeded deadline"}
                    body.extend(chunk)
                    if len(body) > _MAX_CONTENT_BYTES:
                        response.close()
                        return aiko.StreamEvent.ERROR, {"diagnostic":
                            f"HTTP response exceeds {_MAX_CONTENT_BYTES} byte cap"}
                response.close()
                _LOGGER.debug(f"_fetch(): {url} -> {len(body)} bytes")
                return aiko.StreamEvent.OKAY, {"record": bytes(body)}

            return aiko.StreamEvent.ERROR,  \
                {"diagnostic": f"HTTP exceeded {_MAX_REDIRECTS} redirects"}
        except requests.RequestException as request_error:
            return aiko.StreamEvent.ERROR,  \
                {"diagnostic": f'HTTP GET "{url}" failed: {request_error}'}


aiko.DataScheme.add_data_scheme("http", DataSchemeHTTP)
aiko.DataScheme.add_data_scheme("https", DataSchemeHTTP)

# --------------------------------------------------------------------------- #

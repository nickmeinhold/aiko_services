# To Do
# ~~~~~
# - SSRF hardening beyond "http(s)-only + redirect re-check": optional host
#   allow-list and blocking link-local / cloud-metadata addresses
#   (e.g. 169.254.0.0/16) when exposed to untrusted PipelineDefinitions
# - Support request headers / bearer-token auth via PipelineElement parameters
# - create_targets(): HTTP upload (POST/PUT), once a matching write
#   PipelineElement exists (currently source-only, fails closed)

import requests

import aiko_services as aiko

__all__ = ["DataSchemeHTTP"]

_LOGGER = aiko.process.logger(__name__)

_TIMEOUT_SECONDS = 30                    # per-request connect + read timeout
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

        records = []
        try:
            while (data_batch_size > 0):
                data_batch_size -= 1
                url = next(stream.variables["source_urls_generator"])
                stream_event, result = self._fetch(url)
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

    def _fetch(self, url):
        session = requests.Session()
        session.max_redirects = _MAX_REDIRECTS
        try:
            response = session.get(url,
                timeout=_TIMEOUT_SECONDS, stream=True)
            response.raise_for_status()

            # A redirect must not land on a non-http(s) target (e.g. file://)
            final_scheme = aiko.DataScheme.parse_url_scheme(response.url)
            if final_scheme not in ("http", "https"):
                return aiko.StreamEvent.ERROR, {"diagnostic":
                    f'HTTP redirect to non-http(s) URL "{response.url}"'}

            # Enforce a response-size cap.  Content-Length may be absent or
            # dishonest, so also cap while streaming the body.
            # Content-Length is attacker/server-controlled and may be absent or
            # non-numeric -- guard the int parse (a bare int() would raise
            # ValueError, escaping the requests.RequestException handler and
            # crashing the pipeline thread).  The streaming cap below is the
            # real enforcement; this is just an early-out on an honest header.
            content_length = response.headers.get("Content-Length")
            if content_length and content_length.isdigit()  \
                    and int(content_length) > _MAX_CONTENT_BYTES:
                return aiko.StreamEvent.ERROR, {"diagnostic":
                    f"HTTP response exceeds {_MAX_CONTENT_BYTES} byte cap"}

            body = bytearray()
            for chunk in response.iter_content(chunk_size=_CHUNK_BYTES):
                body.extend(chunk)
                if len(body) > _MAX_CONTENT_BYTES:
                    return aiko.StreamEvent.ERROR, {"diagnostic":
                        f"HTTP response exceeds {_MAX_CONTENT_BYTES} byte cap"}

            _LOGGER.debug(f"_fetch(): {url} -> {len(body)} bytes")
            return aiko.StreamEvent.OKAY, {"record": bytes(body)}
        except requests.RequestException as request_error:
            return aiko.StreamEvent.ERROR,  \
                {"diagnostic": f'HTTP GET "{url}" failed: {request_error}'}
        finally:
            session.close()

aiko.DataScheme.add_data_scheme("http", DataSchemeHTTP)
aiko.DataScheme.add_data_scheme("https", DataSchemeHTTP)

# --------------------------------------------------------------------------- #

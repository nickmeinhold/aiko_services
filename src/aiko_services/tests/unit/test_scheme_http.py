# Usage
# ~~~~~
# pytest [-s] unit/test_scheme_http.py
#
# Covers the "http"/"https" DataScheme:
# - registration into DataScheme.LOOKUP via the media package import
#   (guards the class of bug where a scheme module exists but is never
#    imported, so add_data_scheme() never runs)
# - a real HTTP GET (local server) surfaced as a frame "record"
# - SSRF: non-public hosts blocked by default; manual per-hop redirect follow
# - fail-closed behaviour: non-http(s) source, target, size cap, deadline

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import aiko_services as aiko
import aiko_services.elements.media as media
from aiko_services.elements.media import scheme_http

_BODY = b"\xff\xd8\xff\xe0 not-utf8 image-ish bytes \x00\x01\x02"


class _StubElement:
    def __init__(self, params=None):
        self.share = {}
        # Local test servers bind 127.0.0.1 (loopback), which the SSRF guard
        # blocks by default -- opt in unless a test overrides it.
        self._params = {"allow_private_addresses": True}
        if params:
            self._params.update(params)
        self.created_frames = []

    def get_parameter(self, name, default=None):
        return self._params.get(name, default), None

    def create_frames(self, stream, frame_generator, rate=None):
        self.created_frames.append((frame_generator, rate))


class _StubStream:
    def __init__(self):
        self.variables = {}


def _make_scheme(params=None):
    element = _StubElement(params)
    return scheme_http.DataSchemeHTTP(element), element


def _serve(handler):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    return server, f"http://{host}:{port}"


@pytest.fixture
def http_server():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(_BODY)))
            self.end_headers()
            self.wfile.write(_BODY)

        def log_message(self, *args):
            pass

    server, base = _serve(Handler)
    yield f"{base}/image.jpeg"
    server.shutdown()


# --- registration (the lesson) ------------------------------------------- #

def test_http_and_https_registered():
    assert "http" in aiko.DataScheme.LOOKUP
    assert "https" in aiko.DataScheme.LOOKUP
    assert aiko.DataScheme.LOOKUP["http"] is scheme_http.DataSchemeHTTP
    assert aiko.DataScheme.LOOKUP["https"] is scheme_http.DataSchemeHTTP


def test_media_package_exports_scheme():
    assert media.DataSchemeHTTP is scheme_http.DataSchemeHTTP


# --- functional GET ------------------------------------------------------- #

def test_fetch_returns_body_as_record(http_server):
    scheme, _ = _make_scheme()
    stream = _StubStream()
    stream.variables["source_urls_generator"] = iter([http_server])

    stream_event, result = scheme.frame_generator(stream, 0)
    assert stream_event == aiko.StreamEvent.OKAY
    assert result["records"] == [_BODY]


def test_generator_stops_when_exhausted(http_server):
    scheme, _ = _make_scheme()
    stream = _StubStream()
    stream.variables["source_urls_generator"] = iter([])

    stream_event, _ = scheme.frame_generator(stream, 0)
    assert stream_event == aiko.StreamEvent.STOP


def test_redirect_is_followed():
    # Origin 302s to a second path; the manual redirect loop must follow it and
    # return the final body (allow_private=True so both loopback hops resolve).
    class RedirectHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/first":
                self.send_response(302)
                self.send_header("Location", "/second")
                self.end_headers()
            else:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(_BODY)

        def log_message(self, *args):
            pass

    server, base = _serve(RedirectHandler)
    try:
        scheme, _ = _make_scheme()
        stream = _StubStream()
        stream.variables["source_urls_generator"] = iter([f"{base}/first"])
        stream_event, result = scheme.frame_generator(stream, 0)
        assert stream_event == aiko.StreamEvent.OKAY
        assert result["records"] == [_BODY]
    finally:
        server.shutdown()


# --- SSRF guard ----------------------------------------------------------- #

@pytest.mark.parametrize("host,ok", [
    ("127.0.0.1", False),       # loopback
    ("169.254.169.254", False),  # cloud metadata (link-local)
    ("10.0.0.1", False),        # RFC1918
    ("192.168.1.1", False),     # RFC1918
    ("100.64.0.1", False),      # CGNAT (missed by a hand-rolled blocklist)
    ("::1", False),             # IPv6 loopback
    ("8.8.8.8", True),          # public
])
def test_resolve_public_ip(host, ok):
    ip, diagnostic = scheme_http._resolve_public_ip(host)
    if ok:
        assert ip is not None and diagnostic is None
    else:
        assert ip is None and diagnostic


def test_private_host_rejected_by_default(http_server):
    # Same loopback URL, but allow_private_addresses=False -> blocked pre-fetch.
    scheme, _ = _make_scheme({"allow_private_addresses": False})
    stream = _StubStream()
    stream.variables["source_urls_generator"] = iter([http_server])
    stream_event, result = scheme.frame_generator(stream, 0)
    assert stream_event == aiko.StreamEvent.ERROR
    assert "blocked" in result["diagnostic"]


def test_pinned_fetch_reaches_validated_ip(http_server, monkeypatch):
    # Exercise the REAL pinned path (allow_private=False): pretend the host
    # resolved to a validated public IP, but hand back the loopback IP so the
    # pinned connection lands on the local test server. Proves the resolve ->
    # pin -> connect-to-IP + Host-header machinery actually works.
    monkeypatch.setattr(scheme_http, "_resolve_public_ip",
                        lambda host: ("127.0.0.1", None))
    scheme, _ = _make_scheme({"allow_private_addresses": False})
    stream = _StubStream()
    stream.variables["source_urls_generator"] = iter([http_server])
    stream_event, result = scheme.frame_generator(stream, 0)
    assert stream_event == aiko.StreamEvent.OKAY
    assert result["records"] == [_BODY]


def test_redirect_to_blocked_host_is_rejected(monkeypatch):
    # hop-1 validates, hop-2 (the Location target) does not -> the per-hop guard
    # must reject BEFORE contacting hop-2. Pin hop-1 to loopback; block others.
    class RedirectHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(302)
            self.send_header("Location", "http://blocked.invalid/meta")
            self.end_headers()

        def log_message(self, *args):
            pass

    server, base = _serve(RedirectHandler)

    def fake_resolve(host):
        return ("127.0.0.1", None) if host == "127.0.0.1" \
            else (None, f"non-public {host}")
    monkeypatch.setattr(scheme_http, "_resolve_public_ip", fake_resolve)
    try:
        scheme, _ = _make_scheme({"allow_private_addresses": False})
        stream = _StubStream()
        stream.variables["source_urls_generator"] = iter([f"{base}/first"])
        stream_event, result = scheme.frame_generator(stream, 0)
        assert stream_event == aiko.StreamEvent.ERROR
        assert "blocked" in result["diagnostic"]
    finally:
        server.shutdown()


# --- fail-closed guards --------------------------------------------------- #

def test_non_http_source_errors():
    scheme, element = _make_scheme()
    stream = _StubStream()
    stream_event, result = scheme.create_sources(stream, ["file:/etc/passwd"])
    assert stream_event == aiko.StreamEvent.ERROR
    assert element.created_frames == []   # never scheduled a fetch


def test_target_is_source_only():
    scheme, _ = _make_scheme()
    stream = _StubStream()
    stream_event, result = scheme.create_targets(stream, ["http://host/up"])
    assert stream_event == aiko.StreamEvent.ERROR
    assert "source-only" in result["diagnostic"]


def test_malformed_content_length_does_not_crash():
    # A non-numeric Content-Length must not crash the pipeline thread (bare
    # int() would raise ValueError, escaping the requests handler).
    class BadLenHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", "not-a-number")
            self.end_headers()
            self.wfile.write(_BODY)

        def log_message(self, *args):
            pass

    server, base = _serve(BadLenHandler)
    try:
        scheme, _ = _make_scheme()
        stream = _StubStream()
        stream.variables["source_urls_generator"] = iter([f"{base}/x.jpeg"])
        stream_event, result = scheme.frame_generator(stream, 0)
        assert stream_event == aiko.StreamEvent.OKAY
        assert result["records"] == [_BODY]
    finally:
        server.shutdown()


def test_response_size_cap(http_server, monkeypatch):
    monkeypatch.setattr(scheme_http, "_MAX_CONTENT_BYTES", 4)
    scheme, _ = _make_scheme()
    stream = _StubStream()
    stream.variables["source_urls_generator"] = iter([http_server])

    stream_event, result = scheme.frame_generator(stream, 0)
    assert stream_event == aiko.StreamEvent.ERROR
    assert "cap" in result["diagnostic"]


def test_deadline_exceeded(http_server, monkeypatch):
    # A non-positive total deadline must fail closed before any body is read.
    monkeypatch.setattr(scheme_http, "_TIMEOUT_SECONDS", -1)
    scheme, _ = _make_scheme()
    stream = _StubStream()
    stream.variables["source_urls_generator"] = iter([http_server])
    stream_event, result = scheme.frame_generator(stream, 0)
    assert stream_event == aiko.StreamEvent.ERROR
    assert "deadline" in result["diagnostic"]

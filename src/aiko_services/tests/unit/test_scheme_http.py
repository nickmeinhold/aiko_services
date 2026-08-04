# Usage
# ~~~~~
# pytest [-s] unit/test_scheme_http.py
#
# Covers the "http"/"https" DataScheme:
# - registration into DataScheme.LOOKUP via the media package import
#   (guards the class of bug where a scheme module exists but is never
#    imported, so add_data_scheme() never runs)
# - a real HTTP GET (local server) surfaced as a frame "record"
# - fail-closed behaviour: non-http(s) source, target (source-only), size cap

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
        self._params = params or {}
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

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    yield f"http://{host}:{port}/image.jpeg"
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


def test_response_size_cap(http_server, monkeypatch):
    monkeypatch.setattr(scheme_http, "_MAX_CONTENT_BYTES", 4)
    scheme, _ = _make_scheme()
    stream = _StubStream()
    stream.variables["source_urls_generator"] = iter([http_server])

    stream_event, result = scheme.frame_generator(stream, 0)
    assert stream_event == aiko.StreamEvent.ERROR
    assert "cap" in result["diagnostic"]

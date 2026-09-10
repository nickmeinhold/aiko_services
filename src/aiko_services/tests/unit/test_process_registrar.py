# Usage
# ~~~~~
# pytest [-s] unit/test_process_registrar.py
# pytest [-s] unit/test_process_registrar.py::test_add_service_publishes_add
#
# Regression tests for the Registrar side of the Service lifecycle in
# "process.py".
#
# add_service() and remove_service() tell the Registrar about the change only
# when the Process is connected to a Registrar:
#
#     if self.connection.is_connected(ConnectionState.REGISTRAR):
#         self._add_service_to_registrar(service)
#
# The allocator tests give their Process a Connection that starts at
# ConnectionState.NONE, so they never enter that branch. That keeps the
# allocator away from the environment, and it leaves the publish itself
# covered by no test at all. These tests enter the branch on purpose.
#
# What each test must assert, and why the payload and not the call:
# the identity of a Service on the bus is its "topic_path", which the payload
# carries. A test that asserts only that a publish occurred passes when the
# payload names the wrong Service, because the wrong path publishes too.
#
# To Do
# ~~~~~
# - None, yet !

import pytest

import aiko_services as aiko_package
from aiko_services.main.connection import Connection, ConnectionState
from aiko_services.main.process import ProcessData, ProcessImplementation

REGISTRAR_TOPIC_PATH = "namespace/hostname/1000/1"
REGISTRAR_TOPIC_IN = f"{REGISTRAR_TOPIC_PATH}/in"

class ServiceStub:
    # A payload, not a Service: "process.py" reads these fields only
    def __init__(self, name, protocol="test:0", tags="a=1"):
        self.name = name
        self.protocol = protocol
        self.transport = "mqtt"
        self.service_id = None
        self.topic_path = None
        self._tags = tags
        self.registrar_calls = []

    def get_tags_string(self):
        return self._tags

    def registrar_handler_call(self, action, registrar):
        # "on_registrar()" calls this on every Service it holds. A stub
        # without it raises inside the broad "except" there, which logs a
        # warning, which the MQTT logging handler then publishes: the capture
        # below would read a log message as Registrar traffic
        self.registrar_calls.append(action)

class MessageStub:
    # Captures what reaches the transport, which is the only place the
    # Registrar hears about a Service
    def __init__(self):
        self.published = []

    def publish(self, topic, payload):
        self.published.append((topic, payload))

    def registrar_payloads(self):
        # Only what reached the Registrar's "topic_in". "aiko.message" also
        # carries the log topic, so an unfiltered capture makes a test read
        # log traffic and depend on which tests ran before it
        return [payload for topic, payload in self.published
            if topic == REGISTRAR_TOPIC_IN]

@pytest.fixture
def message(monkeypatch):
    # "aiko.message" and "aiko.registrar" are ProcessData CLASS attributes,
    # so the singleton shares them. monkeypatch puts both back afterwards
    message = MessageStub()
    monkeypatch.setattr(ProcessData, "message", message)
    monkeypatch.setattr(
        ProcessData, "registrar", {"topic_path": REGISTRAR_TOPIC_PATH})
    return message

@pytest.fixture
def process():
    # A private ProcessImplementation with a Connection of its own, so that
    # the state these tests force is not the state of the shared singleton
    process = ProcessImplementation()
    process.connection = Connection("test_process_registrar")
    return process

def _connect(process):
    process.connection.update_state(ConnectionState.REGISTRAR)

def test_the_fixture_is_isolated_from_the_singleton(process, message):
    # Control: without this, every test below may be reading shared state
    assert process is not aiko_package.process
    assert process._services is not aiko_package.process._services
    assert message.registrar_payloads() == []

def test_nothing_is_published_while_the_registrar_is_absent(process, message):
    # Null arm: the capture must read exactly zero when the branch is not
    # entered, or a later non-empty reading proves nothing
    assert not process.connection.is_connected(ConnectionState.REGISTRAR)

    service = ServiceStub("a")
    process.add_service(service)
    process.remove_service(service.service_id)

    assert message.registrar_payloads() == []

def test_add_service_publishes_add(process, message):
    _connect(process)
    service = ServiceStub("a")

    process.add_service(service)

    payloads = message.registrar_payloads()
    assert len(payloads) == 1
    payload = payloads[0]
    assert payload.startswith(
        f"(add {service.topic_path} {service.name} "
        f"{service.protocol} {service.transport} ")
    assert payload.endswith("(a=1))")

def test_remove_service_publishes_remove_for_the_service_removed(
    process, message):

    # The half of the Service lifecycle that reached the wire: an evicted
    # Service that stays registered is a Registrar entry for something gone.
    # Two Services, so a payload naming the wrong one cannot pass
    _connect(process)
    a, b = ServiceStub("a"), ServiceStub("b")
    process.add_service(a)
    process.add_service(b)
    message.published.clear()

    process.remove_service(a.service_id)

    payloads = message.registrar_payloads()
    assert len(payloads) == 1
    payload = payloads[0]
    assert payload == f"(remove {a.topic_path})"
    assert b.topic_path not in payload

def test_removing_an_unknown_service_id_publishes_nothing(process, message):
    _connect(process)
    process.add_service(ServiceStub("a"))
    message.published.clear()

    process.remove_service(99)

    assert message.registrar_payloads() == []

def test_a_service_without_a_protocol_is_not_published(process, message):
    # Both Registrar calls guard on "service.protocol", so a Service that
    # has none is added and removed without the Registrar hearing of it
    _connect(process)
    service = ServiceStub("a", protocol=None)

    process.add_service(service)
    process.remove_service(service.service_id)

    assert message.registrar_payloads() == []

def test_on_registrar_found_republishes_every_service(process, message):
    # A Process that finds a Registrar must announce the Services it already
    # holds, with the ids they already have. New ids here would rename live
    # Services on the bus
    process.connection.update_state(ConnectionState.TRANSPORT)
    a, b = ServiceStub("a"), ServiceStub("b")
    process.add_service(a)
    process.add_service(b)
    assert message.registrar_payloads() == []  # no Registrar yet
    topic_paths_before = (a.topic_path, b.topic_path)

    process.on_registrar(
        None, "topic",
        f"(primary found {REGISTRAR_TOPIC_PATH} 0 1757000000)")

    assert (a.topic_path, b.topic_path) == topic_paths_before
    published = message.registrar_payloads()
    assert len(published) == 2
    assert any(payload.startswith(f"(add {a.topic_path} ")
        for payload in published)
    assert any(payload.startswith(f"(add {b.topic_path} ")
        for payload in published)

def test_on_registrar_found_enters_the_registrar_state(process, message):
    # Control for the test above: the republish is only meaningful if the
    # Process is left in a state where later changes are published too
    process.connection.update_state(ConnectionState.TRANSPORT)

    process.on_registrar(
        None, "topic",
        f"(primary found {REGISTRAR_TOPIC_PATH} 0 1757000000)")

    assert process.connection.is_connected(ConnectionState.REGISTRAR)

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
# Two Registrar topic paths, because one is not enough to tell the tests
# apart. REGISTRAR_TOPIC_PATH is the Registrar a connected Process already
# has. DISCOVERED_TOPIC_PATH is set by nothing but "on_registrar()" reading
# it out of a "(primary found ...)" payload, so a test that finds a publish
# there has shown that the payload was read. With one path, an
# "on_registrar()" that ignores the message entirely still publishes to the
# right place, and the test cannot tell the two apart
#
# To Do
# ~~~~~
# - None, yet !

import pytest

import aiko_services as aiko_package
from aiko_services.main.connection import Connection, ConnectionState
from aiko_services.main.process import ProcessData, ProcessImplementation
from aiko_services.main.utilities.parser import parse

REGISTRAR_TOPIC_PATH = "namespace/hostname/1000/1"
REGISTRAR_TOPIC_IN = f"{REGISTRAR_TOPIC_PATH}/in"
DISCOVERED_TOPIC_PATH = "namespace/hostname/2000/1"
DISCOVERED_TOPIC_IN = f"{DISCOVERED_TOPIC_PATH}/in"

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
        # below would read a log message as Registrar traffic.
        # Both arguments are kept, because a Service that hears the action
        # without the Registrar has been told half of what happened
        self.registrar_calls.append((action, registrar))

class MessageStub:
    # Captures what reaches the transport, which is the only place the
    # Registrar hears about a Service
    def __init__(self):
        self.published = []

    def publish(self, topic, payload):
        self.published.append((topic, payload))

    def payloads(self, topic_in):
        # Only what reached ONE Registrar "topic_in". "aiko.message" also
        # carries the log topic, so a capture of everything makes a test read
        # log traffic and depend on which tests ran before it. Naming the
        # topic also keeps the two Registrar paths apart
        return [payload for topic, payload in self.published
            if topic == topic_in]

@pytest.fixture
def message(monkeypatch):
    # "aiko.message" and "aiko.registrar" are ProcessData CLASS attributes,
    # so the singleton shares them. monkeypatch puts both back afterwards.
    # "registrar" starts as None: a test that wants one says so, and the
    # "on_registrar()" tests get theirs only from the payload they send
    message = MessageStub()
    monkeypatch.setattr(ProcessData, "message", message)
    monkeypatch.setattr(ProcessData, "registrar", None)
    return message

@pytest.fixture
def process():
    # A private ProcessImplementation with a Connection of its own, so that
    # the state these tests force is not the state of the shared singleton
    process = ProcessImplementation()
    process.connection = Connection("test_process_registrar")
    return process

def _connect(process):
    # The Process already knows a Registrar and is connected to it
    ProcessData.registrar = {"topic_path": REGISTRAR_TOPIC_PATH}
    process.connection.update_state(ConnectionState.REGISTRAR)

def _found(topic_path):
    return f"(primary found {topic_path} 0 1757000000)"

def _add_fields(payload):
    # Read the payload with the parser the Process itself uses, so a test
    # asserts the fields of a message and not a slice of a string. A prefix
    # and a suffix leave the middle of the payload unasserted, and the owner
    # lives in the middle
    command, parameters = parse(payload)
    assert command == "add"
    assert len(parameters) == 6, parameters
    topic_path, name, protocol, transport, owner, tags = parameters
    return {"topic_path": topic_path, "name": name, "protocol": protocol,
        "transport": transport, "owner": owner, "tags": tags}

def _assert_describes(payload, service):
    # Every field of the Service, not only the topic path. A republish that
    # keeps the identity and loses the protocol is still a wrong message
    fields = _add_fields(payload)
    assert fields["topic_path"] == service.topic_path
    assert fields["name"] == service.name
    assert fields["protocol"] == service.protocol
    assert fields["transport"] == service.transport
    assert fields["tags"] == [service.get_tags_string()]
    # The owner comes from the operating system, so a test cannot know it.
    # It can still hold the field to one token in its own position, which is
    # what a reader of the payload depends on
    assert fields["owner"]
    assert len(fields["owner"].split()) == 1

def test_the_fixture_is_isolated_from_the_singleton(process, message):
    # Control: without this, every test below may be reading shared state
    assert process is not aiko_package.process
    assert process._services is not aiko_package.process._services
    assert message.published == []
    assert ProcessData.registrar is None

def test_nothing_is_published_while_the_registrar_is_absent(process, message):
    # Null arm: the capture must read exactly zero when the branch is not
    # entered, or a later non-empty reading proves nothing
    assert not process.connection.is_connected(ConnectionState.REGISTRAR)

    service = ServiceStub("a")
    process.add_service(service)
    process.remove_service(service.service_id)

    assert message.published == []

def test_add_service_publishes_add(process, message):
    _connect(process)
    service = ServiceStub("a")

    process.add_service(service)

    payloads = message.payloads(REGISTRAR_TOPIC_IN)
    assert len(payloads) == 1
    _assert_describes(payloads[0], service)

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

    payloads = message.payloads(REGISTRAR_TOPIC_IN)
    assert len(payloads) == 1
    assert parse(payloads[0]) == ("remove", [a.topic_path])
    assert b.topic_path not in payloads[0]

def test_removing_an_unknown_service_id_publishes_nothing(process, message):
    _connect(process)
    process.add_service(ServiceStub("a"))
    message.published.clear()

    process.remove_service(99)

    assert message.payloads(REGISTRAR_TOPIC_IN) == []

def test_a_service_without_a_protocol_is_not_published(process, message):
    # Both Registrar calls guard on "service.protocol", so a Service that
    # has none is added and removed without the Registrar hearing of it
    _connect(process)
    service = ServiceStub("a", protocol=None)

    process.add_service(service)
    process.remove_service(service.service_id)

    assert message.payloads(REGISTRAR_TOPIC_IN) == []

def test_on_registrar_found_republishes_every_service(process, message):
    # A Process that finds a Registrar must announce the Services it already
    # holds, with the ids they already have. New ids here would rename live
    # Services on the bus.
    # The Process has no Registrar until the payload gives it one, and the
    # payload names a path no fixture sets, so a publish that arrives there
    # is a publish that read the message
    process.connection.update_state(ConnectionState.TRANSPORT)
    a, b = ServiceStub("a"), ServiceStub("b")
    process.add_service(a)
    process.add_service(b)
    assert message.published == []  # no Registrar yet
    topic_paths_before = (a.topic_path, b.topic_path)

    process.on_registrar(None, "topic", _found(DISCOVERED_TOPIC_PATH))

    assert (a.topic_path, b.topic_path) == topic_paths_before
    assert ProcessData.registrar["topic_path"] == DISCOVERED_TOPIC_PATH
    payloads = message.payloads(DISCOVERED_TOPIC_IN)
    assert len(payloads) == 2
    by_topic_path = {_add_fields(payload)["topic_path"]: payload
        for payload in payloads}
    _assert_describes(by_topic_path[a.topic_path], a)
    _assert_describes(by_topic_path[b.topic_path], b)

def test_on_registrar_found_tells_every_service_which_registrar(
    process, message):

    # A Service hears the action and the Registrar together. An action
    # without the Registrar is half of what happened
    process.connection.update_state(ConnectionState.TRANSPORT)
    service = ServiceStub("a")
    process.add_service(service)

    process.on_registrar(None, "topic", _found(DISCOVERED_TOPIC_PATH))

    assert service.registrar_calls == [
        ("found", {"topic_path": DISCOVERED_TOPIC_PATH,
            "version": "0", "timestamp": "1757000000"})]

def test_a_service_added_after_the_registrar_is_found_goes_to_it(
    process, message):

    # State without observable work proves nothing: the Process must not
    # only enter ConnectionState.REGISTRAR, it must publish to the Registrar
    # that the payload named
    process.connection.update_state(ConnectionState.TRANSPORT)
    process.on_registrar(None, "topic", _found(DISCOVERED_TOPIC_PATH))
    assert process.connection.is_connected(ConnectionState.REGISTRAR)

    service = ServiceStub("late")
    process.add_service(service)

    assert message.payloads(REGISTRAR_TOPIC_IN) == []
    payloads = message.payloads(DISCOVERED_TOPIC_IN)
    assert len(payloads) == 1
    _assert_describes(payloads[0], service)

def test_on_registrar_absent_leaves_the_registrar_state(process, message):
    # The other half of the lifecycle. A Process that loses its Registrar
    # must forget it and leave the state that says it has one, or the next
    # add_service() enters a branch that dereferences a Registrar that is
    # no longer there
    process.connection.update_state(ConnectionState.TRANSPORT)
    process.on_registrar(None, "topic", _found(DISCOVERED_TOPIC_PATH))
    service = ServiceStub("a")
    process.add_service(service)
    message.published.clear()

    process.on_registrar(None, "topic", "(primary absent)")

    assert ProcessData.registrar is None
    assert not process.connection.is_connected(ConnectionState.REGISTRAR)
    assert process.connection.get_state() == ConnectionState.TRANSPORT
    assert service.registrar_calls[-1] == ("absent", None)

def test_nothing_is_published_after_the_registrar_goes_absent(
    process, message):

    # The state above is only worth having if it stops the publish
    process.connection.update_state(ConnectionState.TRANSPORT)
    process.on_registrar(None, "topic", _found(DISCOVERED_TOPIC_PATH))
    process.on_registrar(None, "topic", "(primary absent)")
    message.published.clear()

    process.add_service(ServiceStub("late"))

    assert message.payloads(DISCOVERED_TOPIC_IN) == []
    assert message.payloads(REGISTRAR_TOPIC_IN) == []

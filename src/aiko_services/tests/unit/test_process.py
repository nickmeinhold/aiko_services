# Usage
# ~~~~~
# pytest [-s] unit/test_process.py
# pytest [-s] unit/test_process.py::test_service_id_not_reused_after_remove
#
# Regression tests for Service identity allocation in "process.py".
#
# add_service() assigns "service_id", from which "topic_path" is built, and
# "topic_path" is the Service's identity on the bus. So the allocator must
# never issue an id that a live Service already holds.
#
# The bug these cover: "service_count" was both the live count (decremented by
# remove_service()) and the id allocator. Remove a non-last Service, add
# another, and the new Service takes the removed id, which is still held by a
# live Service: the new one overwrites the previous holder in "_services", and
# both carry the same topic_path.
#
# Reachable through hyperspace.py:296, which calls remove_service() when an
# empty Category is destroyed.
#
# To Do
# ~~~~~
# - None, yet !

import pytest

import aiko_services as aiko
from aiko_services.main.connection import Connection, ConnectionState
from aiko_services.main.process import ProcessImplementation


class ServiceStub:
    # A payload, not a Service: these tests exercise the allocator in
    # add_service() / remove_service(), which never inspects the object beyond
    # "protocol"
    def __init__(self, label):
        self.label = label
        self.service_id = None
        self.topic_path = None
        self.protocol = None


@pytest.fixture
def process():
    # A private ProcessImplementation, never the "aiko.process" singleton that
    # the rest of the suite shares. ProcessImplementation.__init__() only
    # assigns attributes, so an instance that initialize() never touched has its
    # own "_services", counters and lock, and nothing here has to be put back.
    #
    # "connection" is a ProcessData CLASS attribute, so a new instance still
    # shares the singleton's Connection. Give this one its own, which starts at
    # ConnectionState.NONE: add_service() and remove_service() then always skip
    # the Registrar publish, whatever the environment, rather than depending on
    # every stub remembering to leave "protocol" as None
    process = ProcessImplementation()
    process.connection = Connection("test_process")
    return process


def test_the_fixture_is_isolated_from_the_singleton(process):
    # Control: if the fixture is not isolated, the tests below are mutating
    # shared state and their results say nothing about the allocator
    assert process is not aiko.process
    assert process._services is not aiko.process._services
    assert not process.connection.is_connected(ConnectionState.REGISTRAR)


def test_service_ids_are_distinct_without_removals(process):
    # Control: if this ever fails, the tests below prove nothing
    a, b = ServiceStub("a"), ServiceStub("b")
    process.add_service(a)
    process.add_service(b)

    assert a.service_id != b.service_id
    assert a.topic_path != b.topic_path


def test_service_id_not_reused_after_remove(process):
    a, b = ServiceStub("a"), ServiceStub("b")
    process.add_service(a)
    process.add_service(b)

    process.remove_service(a.service_id)  # remove the NON-last Service

    c = ServiceStub("c")
    process.add_service(c)

    assert c.service_id != b.service_id, \
        "new Service reused the id of a live Service"
    assert c.topic_path != b.topic_path, \
        "two live Services share a topic_path"


def test_live_service_survives_a_later_add(process):
    a, b = ServiceStub("a"), ServiceStub("b")
    process.add_service(a)
    process.add_service(b)
    process.remove_service(a.service_id)
    process.add_service(ServiceStub("c"))

    assert b in process._services.values(), \
        "a live Service was evicted from _services by a later add_service()"


def test_ids_are_not_reused_after_every_service_is_removed(process):
    # The gap is deliberate. Remove every Service and the next id is still the
    # next one up, never 1 again, so a Service that outlives its Process's
    # registry entry can never be confused with a later arrival. Pinned here so
    # a later reader does not "repair" the gap as an accident
    a = ServiceStub("a")
    process.add_service(a)
    process.remove_service(a.service_id)

    assert process.service_count == 0, "no live Services remain"

    b = ServiceStub("b")
    process.add_service(b)

    assert b.service_id != a.service_id, \
        "the allocator restarted after the registry emptied"
    assert b.service_id == a.service_id + 1


def test_service_count_still_tracks_live_services(process):
    # The allocator changed; the public return value must not
    a = ServiceStub("a")

    assert process.add_service(a) == 1
    assert process.add_service(ServiceStub("b")) == 2
    assert process.remove_service(a.service_id) == 1
